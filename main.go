package main

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/pcapgo"
	"github.com/joho/godotenv"
	"github.com/klauspost/pgzip"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/schollz/progressbar/v3"
)

func fck(err error) {
	if err != nil {
		panic(err)
	}
}

func processPayload(steno api.WriteAPI, measurement string, allowTypeCodes map[byte]struct{}, payload []byte) (err error) {
	cursor := bytes.NewReader(payload)
	header := TransportHeader{}
	err = binary.Read(cursor, binary.LittleEndian, &header)
	if err != nil {
		return
	}
	offset, err := cursor.Seek(0, io.SeekCurrent)
	if err != nil {
		return
	}
	head := payload[offset:]
	length := int64(header.PayloadLength)
	head, tail := head[:length], head[length+1:]
	cursor = bytes.NewReader(head)
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
			var (
				tags      map[string]string
				fields    map[string]interface{}
				timestamp time.Time
			)
			switch typecode {
			case 'T':
				trade := TradeReport{}
				err = binary.Read(cursor, binary.LittleEndian, &trade)
				if err != nil {
					return
				}
				tags = map[string]string{
					"asset": trade.Symbol.String(),
					"event": trade.Typecode.String(),
				}
				fields = map[string]interface{}{
					"price": trade.Price.Value(),
				}
				timestamp = trade.Time()
			case 'Q':
				quote := QuoteUpdate{}
				err = binary.Read(cursor, binary.LittleEndian, &quote)
				if err != nil {
					return
				}
				tags = map[string]string{
					"asset": quote.Symbol.String(),
					"event": quote.Typecode.String(),
				}
				fields = map[string]interface{}{
					"ask_price": quote.AskPrice.Value(),
					"bid_price": quote.BidPrice.Value(),
				}
				timestamp = quote.Time()
			case '8', '5':
				order := PriceLevelUpdate{}
				err = binary.Read(cursor, binary.LittleEndian, &order)
				if err != nil {
					return
				}
				tags = map[string]string{
					"asset": order.Symbol.String(),
					"event": order.Typecode.String(),
				}
				fields = map[string]interface{}{
					"price": order.Price.Value(),
					"size":  order.Size,
				}
				timestamp = order.Time()
			default:
				_, err = cursor.Seek(int64(mLength), io.SeekCurrent)
				if err != nil {
					return
				}
				continue
			}
			point := influxdb2.NewPoint(measurement, tags, fields, timestamp)
			steno.WritePoint(point)
		}
	}
	if len(tail) > 0 {
		err = processPayload(steno, measurement, allowTypeCodes, tail)
	}
	return
}

func main() {
	godotenv.Load()
	var (
		dbName, allow, org, bucket, token, measurement string
	)
	flag.StringVar(&org, "org", "", "organization name")
	flag.StringVar(&dbName, "db", "http://localhost:8086", "url of destination influxdb")
	flag.StringVar(&allow, "allow", "TQ85", "allowed event typecodes")
	flag.StringVar(&bucket, "bucket", "hist", "destination bucket")
	flag.StringVar(&token, "token", "", "authentication token")
	flag.StringVar(&measurement, "measurement", "hist", "destination measurement")
	flag.Parse()
	if token == "" {
		token = os.Getenv("INFLUXDB_TOKEN")
	}
	if dbName == "" {
		fmt.Fprintf(os.Stderr, "missing -db\n")
		os.Exit(-1)
	}
	dbopt := influxdb2.DefaultOptions().SetUseGZip(false).SetTLSConfig(&tls.Config{InsecureSkipVerify: strings.Contains(dbName, "localhost")})
	dbopt.SetBatchSize(1 << 16)
	db := influxdb2.NewClientWithOptions(dbName, token, dbopt)
	steno := db.WriteAPI(org, bucket)
	defer db.Close()
	msgTypes := map[byte]struct{}{}
	for _, t := range []byte(allow) {
		msgTypes[t] = struct{}{}
	}
	wg := sync.WaitGroup{}
	defer wg.Wait()
	payloads := make(chan []byte)
	defer close(payloads)
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for payload := range payloads {
				err := processPayload(steno, measurement, msgTypes, payload)
				if err != io.EOF {
					fck(err)
				}
			}
		}()
	}
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
