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

	"github.com/go-sql-driver/mysql"
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

func createIndexIfNotExist(db *sql.DB, table, field, index string) (err error) {
	_, err = db.Exec(fmt.Sprintf(`CREATE INDEX %s ON %s (%s)`, index, table, field))
	if err != nil {
		if merr, ok := err.(*mysql.MySQLError); ok {
			if merr.Message == fmt.Sprintf("Duplicate key name '%s'", index) {
				err = nil
			}
		}
		return
	}
	return
}

func main() {
	var dsn, allow string
	flag.StringVar(&dsn, "db", "", "MySQL DSN")
	flag.StringVar(&allow, "allow", "TQ85", "allowed event typecodes")
	flag.Parse()
	if dsn == "" {
		fmt.Fprintf(os.Stderr, "missing -db\n")
		os.Exit(-1)
	}
	db, err := sql.Open("mysql", dsn)
	fck(err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		id SERIAL PRIMARY KEY,
		event CHAR(1) NOT NULL,
		flags BIT(8) NOT NULL,
		time TIMESTAMP NOT NULL,
		symbol VARCHAR(8) NOT NULL,
		payload TINYBLOB NOT NULL
	)`)
	fck(err)
	fck(createIndexIfNotExist(db, "messages", "event", "event_index"))
	fck(createIndexIfNotExist(db, "messages", "symbol", "symbol_index"))
	fck(createIndexIfNotExist(db, "messages", "time", "time_index"))
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
			batchSize := 1024
			defer wg.Done()
			fck(err)
			batch := make([]Record, 0, batchSize)
			flush := func(batch []Record) {
				txn, err := db.Begin()
				fck(err)

				stmt, err := txn.Prepare(
					"INSERT INTO messages (event, flags, time, symbol, payload) VALUES (?, ?, ?, ?, ?)",
				)
				fck(err)
				for _, record := range batch {
					_, err = stmt.Exec(record.Typecode.String(), record.Flags, record.Timestamp.Time(), record.Symbol.String(), record.Payload)
					fck(err)
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
