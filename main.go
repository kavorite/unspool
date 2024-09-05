package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
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

	"github.com/schollz/progressbar/v3"
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
	for i := 0; i < int(header.MessageCount)-1; i++ {
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

func main() {
	var (
		dbName, allow string
	)
	flag.StringVar(&dbName, "db", "hist", "path to destination parquet database")
	flag.StringVar(&allow, "allow", "T85Q", "allowed event typecodes")
	flag.Parse()
	if dbName == "" {
		fmt.Fprintf(os.Stderr, "missing -db\n")
		os.Exit(-1)
	}
	err := os.MkdirAll(dbName, 0755)
	fck(err)
	msgTypes := map[byte]struct{}{}
	for _, t := range []byte(allow) {
		msgTypes[t] = struct{}{}
	}
	paths := flag.Args()
	payloads := make(chan []byte, 1<<12)
	go func() {
		defer close(payloads)
		for _, path := range paths {
			var (
				f   *os.File
				bar *progressbar.ProgressBar
				err error
			)
			stdin := path == "-"

			if stdin {
				bar = progressbar.DefaultBytes(-1, "ingest stdin...")
				f = os.Stdin
			} else {
				stat, err := os.Stat(path)
				fck(err)
				bar = progressbar.DefaultBytes(stat.Size(), fmt.Sprintf("ingest %s...", path))
				f, err = os.Open(path)
				fck(err)
			}
			t := io.TeeReader(f, bar)
			g, err := pgzip.NewReader(t)
			fck(err)
			p, err := pcapgo.NewNgReader(g, pcapgo.DefaultNgReaderOptions)
			fck(err)
			if !stdin {
				defer g.Close()
			}
			fck(err)
			src := gopacket.NewPacketSource(p, p.LinkType())
			src.DecodeOptions.Lazy = true
			src.DecodeOptions.NoCopy = true
			for packet := range src.Packets() {
				payloads <- packet.ApplicationLayer().LayerContents()
			}
		}
	}()
	fck(err)

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

	// Create a parquet.Writer that writes to stdout
	w, err := pqarrow.NewFileWriter(schema, os.Stdout, writerProps, arrowProps)
	fck(err)
	defer w.Close()

	// Create a record builder
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	for payload := range payloads {
		records, err := processPayload(msgTypes, payload)
		fck(err)

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
				fck(err)
				rec.Release()
				b = array.NewRecordBuilder(memory.DefaultAllocator, schema)
			}
		}
	}

	// Write any remaining records
	if b.Field(0).Len() > 0 {
		rec := b.NewRecord()
		err = w.Write(rec)
		fck(err)
		rec.Release()
	}
}
