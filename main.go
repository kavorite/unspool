package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/compress"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/google/gopacket"
	"github.com/google/gopacket/pcapgo"
	"github.com/klauspost/pgzip"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

func fck(err error) {
	if err != nil {
		panic(err)
	}
}

type Record struct {
	Time  time.Time
	Event string
	Asset string
	Price float32
	Size  float32
}

func makeRecord(m Message, o Order) Record {
	return Record{
		Time:  m.Time(),
		Event: string(m.Typecode),
		Asset: m.Symbol.String(),
		Price: o.Price.Float(),
		Size:  float32(o.Size),
	}
}

func processPayload(allowTypeCodes map[byte]struct{}, payload []byte) (records []Record, err error) {
	cursor := bytes.NewReader(payload)
	header := TransportHeader{}
	err = binary.Read(cursor, binary.LittleEndian, &header)
	if err != nil {
		return
	}
	records = make([]Record, 0, header.MessageCount*2)
	for range header.MessageCount {
		var mLength Short
		err = binary.Read(cursor, binary.LittleEndian, &mLength)
		if err != nil {
			return
		}
		var typecode byte
		typecode, err = cursor.ReadByte()
		if err != nil {
			return
		}
		err = cursor.UnreadByte()
		if err != nil {
			return
		}

		if _, allow := allowTypeCodes[typecode]; allow {
			switch typecode {
			case 'T':
				trade := TradeReport{}
				err = binary.Read(cursor, binary.LittleEndian, &trade)
				if err != nil {
					return
				}
				records = append(records, makeRecord(trade.Message, trade.Order))
			case '8', '5':
				level := PriceLevelUpdate{}
				err = binary.Read(cursor, binary.LittleEndian, &level)
				if err != nil {
					return
				}
				records = append(records, makeRecord(level.Message, level.Order))
			case 'Q':
				quote := QuoteUpdate{}
				err = binary.Read(cursor, binary.LittleEndian, &quote)
				if err != nil {
					return
				}
				msg := quote.Message
				msg.Typecode = 'B'
				records = append(records, makeRecord(msg, Order{quote.BidSize, quote.BidPrice}))
				msg.Typecode = 'S'
				records = append(records, makeRecord(msg, Order{quote.AskSize, quote.AskPrice}))
			default:
				_, err = cursor.Seek(int64(mLength), io.SeekCurrent)
				if err != nil {
					return
				}
			}
		} else {
			_, err = cursor.Seek(int64(mLength), io.SeekCurrent)
			if err != nil {
				return
			}
		}
	}
	return
}

// HistEntry represents a single historical data entry from the IEX API
type HistEntry struct {
	Date string `json:"date"`
	Feed string `json:"feed"`
	Link string `json:"link"`
    Size int64  `json:"size"`
}

// processFile downloads and processes a single file, writing results to a parquet file
func processFile(dbName string, entry HistEntry, msgTypes map[byte]struct{}, bar *mpb.Bar) error {
	outputFile := filepath.Join(dbName, entry.Date+".parquet")
	// Create file for output
	outFile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("error creating file %s: %v", outputFile, err)
	}
	defer outFile.Close()

	// Download the file
	resp, err := http.Get(entry.Link)
	if err != nil {
		return fmt.Errorf("error downloading %s: %v", entry.Link, err)
	}
	defer resp.Body.Close()
	
	// Update the bar's total if Content-Length is available
	if contentLength := resp.ContentLength; contentLength > 0 {
		bar.SetTotal(contentLength, false)
	}
	
	// Create reader with progress bar
	reader := bar.ProxyReader(resp.Body)

	// Setup readers
	g, err := pgzip.NewReader(reader)
	if err != nil {
		return fmt.Errorf("error creating gzip reader: %v", err)
	}
	defer g.Close()
	
	pcapReader, err := pcapgo.NewNgReader(g, pcapgo.DefaultNgReaderOptions)
	if err != nil {
		return fmt.Errorf("error creating pcap reader: %v", err)
	}
	
	src := gopacket.NewPacketSource(pcapReader, pcapReader.LinkType())
	src.DecodeOptions.Lazy = true
	src.DecodeOptions.NoCopy = true
	
	// Create a schema for the Parquet file
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_ns},
			{Name: "event", Type: arrow.BinaryTypes.String},
			{Name: "asset", Type: arrow.BinaryTypes.String},
			{Name: "price", Type: arrow.PrimitiveTypes.Float32},
			{Name: "size", Type: arrow.PrimitiveTypes.Float32},
		},
		nil,
	)

	// Create parquet.WriterProperties
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))

	// Create pqarrow.ArrowWriterProperties
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	// Create a parquet.Writer
	w, err := pqarrow.NewFileWriter(schema, outFile, writerProps, arrowProps)
	if err != nil {
		return fmt.Errorf("error creating parquet writer: %v", err)
	}
	defer w.Close()

	// Create a record builder
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	// Process packets
	for packet := range src.Packets() {
		if app := packet.ApplicationLayer(); app != nil {
			records, err := processPayload(msgTypes, app.LayerContents())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error processing payload: %v\n", err)
				continue
			}

			for _, record := range records {
				b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(record.Time.UnixNano()))
				b.Field(1).(*array.StringBuilder).Append(record.Event)
				b.Field(2).(*array.StringBuilder).Append(record.Asset)
				b.Field(3).(*array.Float32Builder).Append(record.Price)
				b.Field(4).(*array.Float32Builder).Append(record.Size)

				if b.Field(0).Len() >= 1000 {
					// Write the record batch
					rec := b.NewRecord()
					err = w.Write(rec)
					if err != nil {
						rec.Release()
						return fmt.Errorf("error writing record batch: %v", err)
					}
					rec.Release()
					b = array.NewRecordBuilder(memory.DefaultAllocator, schema)
				}
			}
		}
	}

	// Write any remaining records
	if b.Field(0).Len() > 0 {
		rec := b.NewRecord()
		err = w.Write(rec)
		if err != nil {
			rec.Release()
			return fmt.Errorf("error writing final record batch: %v", err)
		}
		rec.Release()
	}
	
	return nil
}

func main() {
	var (
		dbName, allow string
		fromDate, toDate string
		threads int
	)
	flag.StringVar(&dbName, "db", "hist", "path to destination parquet database")
	flag.StringVar(&allow, "allow", "T85Q", "allowed event typecodes")
	flag.StringVar(&fromDate, "f", "", "from date (YYYYMMDD format, default: yesterday)")
	flag.StringVar(&toDate, "t", "", "to date (YYYYMMDD format, default: today)")
	flag.IntVar(&threads, "threads", runtime.NumCPU()-1, "number of threads for parallel processing")
	flag.Parse()
	
	if dbName == "" {
		fmt.Fprintf(os.Stderr, "missing -db\n")
		os.Exit(-1)
	}
	
	// Set default dates if not provided
	if fromDate == "" {
		yesterday := time.Now().AddDate(0, 0, -1)
		fromDate = yesterday.Format("20060102")
	}
	
	if toDate == "" {
		toDate = time.Now().Format("20060102")
	}
	
	// Ensure thread count is at least 1
	if threads < 1 {
		threads = 1
	}
	
	// Create database directory if it doesn't exist
	err := os.MkdirAll(dbName, 0755)
	fck(err)
	
	// Set up message types
	codes := map[byte]struct{}{}
	for _, t := range []byte(allow) {
		codes[t] = struct{}{}
	}
	
	// Fetch historical data entries from IEX API
	fmt.Println("Fetching historical data entries from IEX API...")
	histResp, err := http.Get("https://iextrading.com/api/1.0/hist")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error fetching historical data: %v\n", err)
		os.Exit(1)
	}
	defer histResp.Body.Close()
	
	var histData map[string][]map[string]any
	if err := json.NewDecoder(histResp.Body).Decode(&histData); err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding historical data: %v\n", err)
		os.Exit(1)
	}
	
	// Extract and filter entries
	var entries []HistEntry
	for _, yearData := range histData {
		for _, entry := range yearData {
			date, ok1 := entry["date"].(string)
			feed, ok2 := entry["feed"].(string)
			link, ok3 := entry["link"].(string)
			sizeFloat, _ := entry["size"].(float64)
			
			if !ok1 || !ok2 || !ok3 {
				continue
			}

            size := int64(sizeFloat)

			// Filter by date range and feed type (TOPS only)
			if feed == "TOPS" && date >= fromDate && date <= toDate {
				entries = append(entries, HistEntry{
					Date: date,
					Feed: feed,
					Link: link,
					Size: size,
				})
			}
		}
	}
	
	fmt.Printf("Found %d TOPS feed entries in date range %s to %s\n", len(entries), fromDate, toDate)
	
	if len(entries) == 0 {
		fmt.Println("No entries to process.")
		return
	}
	
	// Create a progress container with shared progress bars
	progress := mpb.New(
		mpb.WithWidth(80),
		mpb.WithRefreshRate(10*time.Millisecond),
	)
	
	// Create a worker pool with fixed number of workers
    if threads < 0 {
        threads = runtime.NumCPU() - 1
    }
	numWorkers := max(threads, 1)
	
	fmt.Printf("Processing with %d worker threads\n", numWorkers)
	
	// Create a channel for distributing entries to workers
	entryChan := make(chan HistEntry, len(entries))
		
	// Track number of workers that have completed
	var wgDone sync.WaitGroup
	wgDone.Add(numWorkers)
	
	// Start the workers
	for range numWorkers {
		go func() {
			defer wgDone.Done()
			
			// Process entries until channel is closed
			for entry := range entryChan {
				bar := progress.New(entry.Size,
                    mpb.BarStyle().Lbound("[").Filler("#").Tip("~").Padding(" ").Rbound("]"),
					mpb.BarWidth(40),
					mpb.BarRemoveOnComplete(),
					mpb.PrependDecorators(
						decor.Name(entry.Date, decor.WC{W: 10, C: decor.DindentRight}),
						decor.CountersKibiByte("% 6.2f / % 6.2f"),
					),
				)
                processFile(dbName, entry, codes, bar)
            }
		}()
	}
	// Add all entries to the channel for workers to process
	for _, entry := range entries {
		entryChan <- entry
	}
	close(entryChan)
    wgDone.Wait()
}
