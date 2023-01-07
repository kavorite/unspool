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

	"github.com/google/gopacket"
	"github.com/google/gopacket/pcapgo"
	"github.com/klauspost/pgzip"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/schollz/progressbar/v3"
)

func fck(err error) {
	if err != nil {
		panic(err)
	}
}

const keySep = "::"

func writePoint(batch *leveldb.Batch, msg Message, field string, value interface{}) {
	ksz := 0
	grp := []interface{}{field, msg.Typecode, msg.Symbol.String(), msg.Timestamp}
	for i, v := range grp {
		ksz += binary.Size(v)
		if i != len(grp)-1 {
			ksz += len(keySep)
		}
	}
	key := bytes.NewBuffer(make([]byte, 0, ksz))
	for i, v := range grp {
		switch s := v.(type) {
		case string:
			key.WriteString(s)
		default:
			binary.Write(key, binary.BigEndian, v)
		}
		if i != len(grp)-1 {
			key.WriteString(keySep)
		}
	}
	val := bytes.NewBuffer(make([]byte, 0, binary.Size(value)))
	binary.Write(val, binary.LittleEndian, value)
	k := key.Bytes()
	v := val.Bytes()
	batch.Put(k, v)
}

func processPayload(batch *leveldb.Batch, allowTypeCodes map[byte]struct{}, payload []byte) (err error) {
	cursor := bytes.NewReader(payload)
	header := TransportHeader{}
	err = binary.Read(cursor, binary.LittleEndian, &header)
	if err != nil {
		return
	}
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
				writePoint(batch, trade.Message, "price", trade.Price.Float())
			case 'Q':
				quote := QuoteUpdate{}
				err = binary.Read(cursor, binary.LittleEndian, &quote)
				if err != nil {
					return
				}
				writePoint(batch, quote.Message, "ask_price", quote.BidPrice.Float())
				writePoint(batch, quote.Message, "bid_price", quote.BidPrice.Float())
			case '8', '5':
				order := PriceLevelUpdate{}
				err = binary.Read(cursor, binary.LittleEndian, &order)
				if err != nil {
					return
				}
				var field string
				if typecode == '8' {
					field = "bid_price"
				} else {
					field = "ask_price"
				}
				writePoint(batch, order.Message, field, order.Price.Float())
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
	flag.StringVar(&dbName, "db", "hist.db", "path to destination LevelDB")
	flag.StringVar(&allow, "allow", "TQ85", "allowed event typecodes")
	flag.Parse()
	if dbName == "" {
		fmt.Fprintf(os.Stderr, "missing -db\n")
		os.Exit(-1)
	}
	db, err := leveldb.OpenFile(dbName, nil)
	fck(err)
	defer db.Close()
	msgTypes := map[byte]struct{}{}
	for _, t := range []byte(allow) {
		msgTypes[t] = struct{}{}
	}
	wg := sync.WaitGroup{}
	defer wg.Wait()
	payloads := make(chan []byte, 1024)
	defer close(payloads)
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := leveldb.Batch{}
			batchSize := 1 << 16
			flush := func() {
				err = db.Write(&batch, nil)
				fck(err)
				batch.Reset()
			}
			defer flush()
			for payload := range payloads {
				err := processPayload(&batch, msgTypes, payload)
				if err != io.EOF {
					fck(err)
				}
				if batch.Len() >= batchSize {
					flush()
				}
			}
		}()
	}
	paths := flag.Args()
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
}
