package main

import (
	"C"
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
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"wellquite.org/golmdb"

	"github.com/schollz/progressbar/v3"
)
import "errors"

func fck(err error) {
	if err != nil {
		panic(err)
	}
}

type Write struct {
	Key, Val []byte
}

type Batch []Write

func (b *Batch) Put(key, val []byte) {
	*b = append(*b, Write{key, val})
}

func writePoint(batch *Batch, msg Message, order Order) {
	ksz := binary.Size(msg.Timestamp)
	key := bytes.NewBuffer(make([]byte, 0, ksz+2))
	key.WriteString("::")
	binary.Write(key, binary.BigEndian, msg.Timestamp)
	var (
		symbol string
		price  float32
		size   Integer
	)
	symbol = msg.Symbol.String()
	price = order.Price.Float()
	size = order.Size
	val := bytes.NewBuffer(make([]byte, 0, binary.Size(msg.Typecode)+binary.Size(price)+binary.Size(size)))
	binary.Write(val, binary.LittleEndian, msg.Typecode)
	binary.Write(val, binary.LittleEndian, price)
	binary.Write(val, binary.LittleEndian, size)
	val.WriteString(symbol)
	k := key.Bytes()
	v := val.Bytes()
	batch.Put(k, v)
}

func processPayload(allowTypeCodes map[byte]struct{}, payload []byte) (batch Batch, err error) {
	cursor := bytes.NewReader(payload)
	header := TransportHeader{}
	err = binary.Read(cursor, binary.LittleEndian, &header)
	if err != nil {
		return
	}
	seen := make(map[string]struct{}, header.MessageCount)
	batch = make(Batch, 0, header.MessageCount)
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

		writeSymbol := func(s string) {
			if _, ok := seen[s]; !ok {
				seen[s] = struct{}{}
				batch.Put([]byte(fmt.Sprintf("symbol::%s", s)), []byte{})
			}
		}
		if _, allow := allowTypeCodes[typecode]; allow {
			switch typecode {
			case 'T':
				trade := TradeReport{}
				writeSymbol(trade.Symbol.String())
				err = binary.Read(cursor, binary.LittleEndian, &trade)
				if err != nil {
					return
				}
				writePoint(&batch, trade.Message, trade.Order)
			case '8', '5':
				order := PriceLevelUpdate{}
				err = binary.Read(cursor, binary.LittleEndian, &order)
				if err != nil {
					return
				}
				writeSymbol(order.Symbol.String())
				writePoint(&batch, order.Message, order.Order)
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
		dbName, dbPath, allow string
	)
	flag.StringVar(&dbName, "db", "hist", "name of the top-level LMDB database")
	flag.StringVar(&dbPath, "path", "./hist.lmdb", "path to destination LMDB")
	flag.StringVar(&allow, "allow", "T85", "allowed event typecodes")
	flag.Parse()
	if dbPath == "" {
		fmt.Fprintf(os.Stderr, "missing -db\n")
		os.Exit(-1)
	}
	zerolog.SetGlobalLevel(zerolog.ErrorLevel)

	if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
		fck(os.Mkdir(dbPath, 0755))
	}
	db, err := golmdb.NewLMDB(log.Logger, dbPath, 0644, 1, 1, golmdb.MapAsync, uint(runtime.NumCPU())*2)
	fck(err)
	defer db.Sync(true)
	defer db.TerminateSync()
	msgTypes := map[byte]struct{}{}
	for _, t := range []byte(allow) {
		msgTypes[t] = struct{}{}
	}
	wg := sync.WaitGroup{}
	defer wg.Wait()
	payloads := make(chan []byte, 1024)
	defer close(payloads)
	var ref golmdb.DBRef
	err = db.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
		ref, err = txn.DBRef("hist", golmdb.Create)
		return
	})
	fck(err)
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		var batch Batch
		go func() {
			defer wg.Done()
			flush := func() {
				err = db.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
					for _, put := range batch {
						err = txn.Put(ref, put.Key, put.Val, 0)
						if err != nil {
							return
						}
					}
					return
				})
				fck(err)
			}
			defer flush()
			for payload := range payloads {
				var err error
				batch, err = processPayload(msgTypes, payload)
				if err != io.EOF {
					fck(err)
				}
				flush()
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
