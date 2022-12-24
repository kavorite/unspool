package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"unicode"

	"github.com/google/gopacket"
	"github.com/google/gopacket/pcapgo"
	"github.com/klauspost/pgzip"

	"github.com/schollz/progressbar/v3"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func fck(err error) {
	if err != nil {
		panic(err)
	}
}

func entKey(msg Message) []byte {
	sym := []byte(strings.TrimRightFunc(string(msg.Symbol[:]), unicode.IsSpace))
	buf := bytes.NewBuffer(make([]uint8, 0, len(sym)+9))
	buf.WriteByte(byte(msg.Typecode))
	buf.WriteByte('/')
	buf.Write(sym)
	buf.WriteByte('/')
	binary.Write(buf, binary.BigEndian, msg.Timestamp)
	return buf.Bytes()
}

var pfxLength = binary.Size(Message{})

func processPayload(batch *leveldb.Batch, allowTypeCodes map[byte]struct{}, payload []byte) (err error) {
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
		if _, allow := allowTypeCodes[typecode]; allow {
			msg := Message{}
			err = binary.Read(cursor, binary.LittleEndian, &msg)
			if err != nil {
				return
			}
			cursor.Seek(-int64(pfxLength), io.SeekCurrent)
			key := entKey(msg)
			val := make([]byte, 0, mLength)
			_, err = cursor.Read(val)
			batch.Put(key, val)
			idx := bytes.NewBuffer(make([]byte, 0, 3+binary.Size(msg.Timestamp)))
			idx.WriteString("ts/")
			binary.Write(idx, binary.BigEndian, msg.Timestamp)
			batch.Put(idx.Bytes(), key)
			if err != nil {
				return
			}
		}
	}
	return
}

func main() {
	dbName := ""
	allow := ""
	flag.StringVar(&dbName, "db", "", "path to destination LevelDB")
	flag.StringVar(&allow, "allow", "TQ85", "allowed event typecodes")
	flag.Parse()
	if dbName == "" {
		fmt.Fprintf(os.Stderr, "missing -db\n")
		os.Exit(-1)
	}
	db, err := leveldb.OpenFile(dbName, &opt.Options{
		WriteBuffer: 64 << 20,
		// BlockSize:   128 << 20,
		// BlockCacheCapacity: 512 << 20,
		// DisableBlockCache: true,
	})
	// db, err := leveldb.OpenFile(dbName, nil)
	fck(err)
	defer db.Close()
	wg := sync.WaitGroup{}
	defer wg.Wait()
	batches := make(chan *leveldb.Batch, 128)
	payloads := make(chan []byte)
	defer close(payloads)
	msgTypes := map[byte]struct{}{}
	for _, t := range []byte(allow) {
		msgTypes[t] = struct{}{}
	}
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := &leveldb.Batch{}
			clear := func(batch *leveldb.Batch) {
				batches <- batch
			}
			defer clear(batch)
			for payload := range payloads {
				err := processPayload(batch, msgTypes, payload)
				if err != io.EOF {
					fck(err)
				}
				if batch.Len() >= 1<<16 {
					clear(batch)
					batch = &leveldb.Batch{}
				}
			}
		}()
	}

	go func() {
		for batch := range batches {
			err := db.Write(batch, &opt.WriteOptions{NoWriteMerge: true})
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
