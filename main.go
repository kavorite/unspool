package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/pcapgo"
	"github.com/klauspost/pgzip"
	"github.com/segmentio/parquet-go"

	"github.com/schollz/progressbar/v3"
)

func fck(err error) {
	if err != nil {
		panic(err)
	}
}

type Record struct {
	time  time.Time
	event string
	asset string
	price float32
	size  float32
}

func makeRecord(m Message, o Order) Record {
	return Record{
		time:  m.Time(),
		event: string(m.Typecode),
		asset: m.Symbol.String(),
		price: o.Price.Float(),
		size:  float32(o.Size),
	}
}

func processPayload(allowTypeCodes map[byte]struct{}, payload []byte) (records []Record, err error) {
	cursor := bytes.NewReader(payload)
	header := TransportHeader{}
	err = binary.Read(cursor, binary.LittleEndian, &header)
	if err != nil {
		return
	}
	records = make([]Record, 0, header.MessageCount)
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

func datePartName(root string, date time.Time) string {
	return root + string(os.PathSeparator) + date.UTC().Format("2006-01-02") + ".parquet"
}

func main() {
	var (
		dbName, allow string
	)
	flag.StringVar(&dbName, "db", "hist", "path to destination parquet database")
	flag.StringVar(&allow, "allow", "T85", "allowed event typecodes")
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
	payloads := make(chan []byte, 1024)
	wg := sync.WaitGroup{}
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		var (
			ostrm *os.File
			steno *parquet.GenericWriter[Record]
		)
		defer func() {
			if steno != nil {
				steno.Flush()
				steno.Close()
			}
		}()
		for payload := range payloads {
			records, err := processPayload(msgTypes, payload)
			fck(err)
			if len(records) > 0 {
				opath := datePartName(dbName, records[0].time.UTC().Truncate(24*time.Hour))
				if ostrm == nil || ostrm.Name() != opath {
					if steno != nil {
						err = steno.Close()
						fck(err)
					}
					ostrm, err = os.OpenFile(opath, os.O_CREATE|os.O_WRONLY, 0644)
					fck(err)
					steno = parquet.NewGenericWriter[Record](ostrm)
				}
				_, err = steno.Write(records)
				fck(err)
			}
		}
	}()
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
	close(payloads)
}
