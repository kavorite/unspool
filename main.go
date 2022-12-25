package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/pcapgo"
	"github.com/klauspost/pgzip"
	"github.com/nakabonne/tstorage"

	"github.com/schollz/progressbar/v3"
)

func fck(err error) {
	if err != nil {
		panic(err)
	}
}

func processPayload(allowTypeCodes map[byte]struct{}, payload []byte) (batch []tstorage.Row, err error) {
	cursor := bytes.NewReader(payload)
	header := TransportHeader{}
	err = binary.Read(cursor, binary.LittleEndian, &header)
	if err != nil {
		return
	}
	for i := 0; i < int(header.MessageCount); i++ {
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

		record := func(msg Message, price float64, side string) tstorage.Row {
			return tstorage.Row{
				Metric: "price",
				Labels: []tstorage.Label{
					{Name: "symbol", Value: msg.Symbol.ToString()},
					{Name: "side", Value: side},
					{Name: "type", Value: string([]byte{byte(msg.Typecode)})},
				},
				DataPoint: tstorage.DataPoint{
					Value:     price,
					Timestamp: int64(msg.Timestamp),
				},
			}
		}

		if _, allow := allowTypeCodes[typecode]; allow {
			switch typecode {
			case 'T':
				trade := TradeReport{}
				err = binary.Read(cursor, binary.LittleEndian, &trade)
				if err != nil {
					return
				}
				batch = append(batch, record(trade.Message, trade.Price.Value(), "trade"))
			case 'Q':
				quote := QuoteUpdate{}
				err = binary.Read(cursor, binary.LittleEndian, &quote)
				if err != nil {
					return
				}
				batch = append(batch, record(quote.Message, quote.AskPrice.Value(), "ask"))
				batch = append(batch, record(quote.Message, quote.BidPrice.Value(), "bid"))
			case '8', '5':
				level := PriceLevelUpdate{}
				err = binary.Read(cursor, binary.LittleEndian, &level)
				var side string
				if typecode == '8' {
					side = "bid"
				} else {
					side = "ask"
				}
				batch = append(batch, record(level.Message, level.Price.Value(), side))
				if err != nil {
					return
				}
			default:
				continue
			}
		}
	}
	return
}

func main() {
	dbName := ""
	allow := ""
	flag.StringVar(&dbName, "db", "", "path to destination tstorage")
	flag.StringVar(&allow, "allow", "TQ85", "allowed event typecodes")
	flag.Parse()
	if dbName == "" {
		fmt.Fprintf(os.Stderr, "missing -db\n")
		os.Exit(-1)
	}
	db, err := tstorage.NewStorage(
		tstorage.WithDataPath(dbName),
		tstorage.WithPartitionDuration(30*time.Minute),
		tstorage.WithTimestampPrecision(tstorage.Nanoseconds),
	)
	fck(err)
	defer db.Close()
	batches := make(chan []tstorage.Row, 128)
	defer close(batches)
	payloads := make(chan []byte)
	defer close(payloads)
	msgTypes := map[byte]struct{}{}
	for _, t := range []byte(allow) {
		msgTypes[t] = struct{}{}
	}
	wg := sync.WaitGroup{}
	defer wg.Wait()
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]tstorage.Row, 0, 1<<16)
			flush := func(batch []tstorage.Row) {
				batches <- batch
			}
			defer flush(batch)
			for payload := range payloads {
				chunk, err := processPayload(msgTypes, payload)
				if err != io.EOF {
					fck(err)
				}
				if len(batch)+len(chunk) >= 1<<16 {
					batches <- batch
					batch = make([]tstorage.Row, 0, 1<<16)
				}
				batch = append(batch, chunk...)
			}
			batches <- batch
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for batch := range batches {
			err := db.InsertRows(batch)
			fck(err)
		}
	}()

	paths := flag.Args()
	for _, path := range paths {
		stat, err := os.Stat(path)
		fck(err)
		bar := progressbar.DefaultBytes(stat.Size(), fmt.Sprintf("ingest %s...", path))
		defer bar.Close()
		f, err := os.Open(path)
		fck(err)
		t := io.TeeReader(f, bar)
		g, err := pgzip.NewReader(t)
		fck(err)
		p, err := pcapgo.NewNgReader(g, pcapgo.DefaultNgReaderOptions)
		fck(err)
		defer g.Close()
		fck(err)
		src := gopacket.NewPacketSource(p, p.LinkType())
		src.DecodeOptions.Lazy = true
		src.DecodeOptions.NoCopy = true
		for packet := range src.Packets() {
			payloads <- packet.ApplicationLayer().LayerContents()
		}
	}
}
