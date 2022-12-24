package main

import (
	"bytes"
	"database/sql"
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

	_ "github.com/mattn/go-sqlite3"
	"github.com/schollz/progressbar/v3"
)

func fck(err error) {
	if err != nil {
		panic(err)
	}
}

type Record struct {
	Message
	Payload []byte
}

var pfxLength = binary.Size(Message{})

func processPayload(allowTypeCodes map[byte]struct{}, payload []byte) (records []Record, err error) {
	cursor := bytes.NewReader(payload)
	header := TransportHeader{}
	err = binary.Read(cursor, binary.LittleEndian, &header)
	if err != nil {
		return
	}
	records = make([]Record, 0, header.MessageCount)
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
			_, err = cursor.Seek(-int64(pfxLength), io.SeekCurrent)
			if err != nil {
				return
			}
			val := make([]byte, 0, mLength)
			_, err = cursor.Read(val)
			if err != nil {
				return
			}
			records = append(records, Record{Message: msg, Payload: val})
			if err != nil {
				return
			}
		}
	}
	return
}

func main() {
	var dbName, allow string
	flag.StringVar(&dbName, "db", "", "path to destination LevelDB")
	flag.StringVar(&allow, "allow", "TQ85", "allowed event typecodes")
	flag.Parse()
	if dbName == "" {
		fmt.Fprintf(os.Stderr, "missing -db\n")
		os.Exit(-1)
	}
	db, err := sql.Open("sqlite3", dbName)
	fck(err)
	defer db.Close()

	for _, cmd := range []string{
		`CREATE TABLE IF NOT EXISTS messages (
			message_id INTEGER PRIMARY KEY,
			type TINYINT NOT NULL,
			flags TINYINT NOT NULL,
			timestamp INTEGER NOT NULL,
			symbol TEXT NOT NULL,
			payload BLOB NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS type_index ON messages (type)`,
		`CREATE INDEX IF NOT EXISTS symbol_index ON messages (symbol)`,
		`CREATE INDEX IF NOT EXISTS timestamp_index ON messages (timestamp)`,
	} {
		_, err = db.Exec(cmd)
		fck(err)
	}
	wg := sync.WaitGroup{}
	lck := sync.Mutex{}
	defer wg.Wait()
	payloads := make(chan []byte)
	defer close(payloads)
	msgTypes := map[byte]struct{}{}
	for _, t := range []byte(allow) {
		msgTypes[t] = struct{}{}
	}
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func() {
			batchSize := 1024
			defer wg.Done()
			fck(err)
			batch := make([]Record, 0, batchSize)
			flush := func(batch []Record) {
				lck.Lock()
				defer lck.Unlock()
				txn, err := db.Begin()
				fck(err)
				stmt, err := txn.Prepare(
					`INSERT INTO messages
						(type, flags, timestamp, symbol, payload)
					VALUES (?, ?, ?, ?, ?)`,
				)
				fck(err)
				for _, record := range batch {
					_, err = stmt.Exec(record.Typecode, record.Flags, record.Timestamp, record.Symbol.ToString(), record.Payload)
					fck(err)
				}
				txn.Commit()
			}
			defer flush(batch)
			for payload := range payloads {
				chunk, err := processPayload(msgTypes, payload)
				batch = append(batch, chunk...)
				if err != io.EOF {
					fck(err)
				}
				if len(batch) >= 1024 {
					flush(batch)
					batch = make([]Record, 0, batchSize)
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
