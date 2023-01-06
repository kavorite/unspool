package main

import (
	"fmt"
	"strings"
	"time"
	"unicode"
)

type TransportHeader struct {
	Version            uint8
	Reserved           uint8
	MessageProtocol    Short
	ChannelId          Integer
	SessionId          Integer
	PayloadLength      Short
	MessageCount       Short
	StreamOffset       Long
	FirstMessageSeqNum Long
	Timestamp
}

type Short uint16

type Integer uint32

type Long int64

type Timestamp Long

func (ns Timestamp) Time() time.Time {
	return time.Unix(0, (int64)(ns))
}

type Symbol [8]byte

func (s Symbol) String() string {
	return strings.TrimRightFunc(string(s[:]), unicode.IsSpace)
}

type Typecode byte

func (t Typecode) String() string {
	return string(t)
}

type Flags uint8

func (f Flags) String() string {
	return fmt.Sprintf("%08b", f)
}

type Message struct {
	Typecode
	Flags
	Timestamp
	Symbol
}
