package main

import (
	"strings"
	"time"
	"unicode"
)

func (ns Timestamp) Time() time.Time {
	return time.Unix(0, (int64)(ns))
}

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

type Symbol [8]byte

func (s Symbol) ToString() string {
	return strings.TrimRightFunc(string(s[:]), unicode.IsSpace)
}

type Typecode uint8

type Message struct {
	Typecode
	Flags uint8
	Timestamp
	Symbol
}
