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
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/klauspost/pgzip"

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
		if _, allow := allowTypeCodes[typecode]; mLength > 0 && allow {
			val := make([]byte, mLength)
			_, err = cursor.Read(val)
			fck(err)
			msg := Message{}
			err = binary.Read(bytes.NewBuffer(val[:binary.Size(msg)]), binary.LittleEndian, &msg)
			if err != nil {
				continue
			}
			records = append(records, Record{Message: msg, Payload: val})
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
	var dsn, allow string
	flag.StringVar(&dsn, "db", "", "PostgreSQL DSN")
	flag.StringVar(&allow, "allow", "TQ85", "allowed event typecodes")
	flag.Parse()
	if dsn == "" {
		fmt.Fprintf(os.Stderr, "missing -db\n")
		os.Exit(-1)
	}
	db, err := sql.Open("pgx", dsn)
	fck(err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		ts BIGINT NOT NULL,
		tcode CHAR(1) NOT NULL,
		flags BIT(8) NOT NULL,
		symbol VARCHAR(8) NOT NULL,
		payload BYTEA NOT NULL,
		CONSTRAINT messages_pkey PRIMARY KEY (ts, tcode, symbol)
	)`)
	fck(err)
	wg := sync.WaitGroup{}
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
			batchSize := 1 << 16
			defer wg.Done()
			fck(err)
			batch := make([]Record, 0, batchSize)
			flush := func(batch []Record) {
				txn, err := db.Begin()
				fck(err)
				stmt, err := txn.Prepare(
					`INSERT INTO messages (ts, tcode, flags, symbol, payload)
					VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT DO NOTHING`,
				)
				fck(err)
				for _, record := range batch {
					_, err = stmt.Exec(record.Timestamp, record.Typecode, record.Flags, record.Symbol.String(), record.Payload)
					if err != io.EOF {
						fck(err)
					}
				}
				err = txn.Commit()
				fck(err)
			}
			defer flush(batch)
			for payload := range payloads {
				chunk, err := processPayload(msgTypes, payload)
				batch = append(batch, chunk...)
				if err != io.EOF {
					fck(err)
				}
				if len(batch) >= batchSize {
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
