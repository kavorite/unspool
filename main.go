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
	"github.com/tecbot/gorocksdb"

	"github.com/schollz/progressbar/v3"
)

func fck(err error) {
	if err != nil {
		panic(err)
	}
}

func processPayload(batch *gorocksdb.WriteBatch, allowTypeCodes map[byte]struct{}, payload []byte) (err error) {
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
			cursor.Seek(-int64(binary.Size(msg)), io.SeekCurrent)
			val := make([]byte, 0, mLength)
			_, err = cursor.Read(val)
			if err != nil {
				return
			}
			buf := bytes.NewBuffer(make([]byte, 0, binary.Size(msg.Timestamp)))
			binary.Write(buf, binary.BigEndian, msg.Timestamp)
			key := buf.Bytes()
			batch.Put(append([]byte("chron/"), key...), val)
			sym := []byte(fmt.Sprintf("asset/%s/", msg.Symbol.ToString()))
			sym = append(sym, key...)
			batch.Put(sym, []byte{})
			typ := []byte(fmt.Sprintf("event/%c/", byte(msg.Typecode)))
			typ = append(typ, key...)
			batch.Put(typ, []byte{})
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
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetNoBlockCache(true)
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetBlockBasedTableFactory(bbto)
	db, err := gorocksdb.OpenDb(opts, dbName)
	fck(err)
	defer db.Close()
	batches := make(chan *gorocksdb.WriteBatch, 128)
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
			batch := gorocksdb.NewWriteBatch()
			defer func() {
				batches <- batch
			}()
			for payload := range payloads {
				err := processPayload(batch, msgTypes, payload)
				if err != io.EOF {
					fck(err)
				}
				if batch.Count() >= 1<<16 {
					batches <- batch
					batch = gorocksdb.NewWriteBatch()
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		opt := gorocksdb.NewDefaultWriteOptions()
		opt.DisableWAL(true)
		defer opt.Destroy()
		defer wg.Done()
		for batch := range batches {
			err := db.Write(opt, batch)
			fck(err)
			batch.Destroy()
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
