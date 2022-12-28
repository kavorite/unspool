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

type Price Long

func (p Price) Float64() float64 {
	return (float64)(p) / 1e4
}

type Timestamp Long

type Symbol [8]byte

func (s Symbol) String() string {
	return strings.TrimRightFunc(string(s[:]), unicode.IsSpace)
}

type Typecode uint8

func (t Typecode) String() string {
	return string(t)
}

type Message struct {
	Typecode
	Flags uint8
	Timestamp
	Symbol
}

type Order struct {
	Size Integer
	Price
}
type TradeReport struct {
	Message
	Order
	TradeID Long
}

type QuoteUpdate struct {
	Message
	BidSize  Integer
	BidPrice Price
	AskPrice Price
	AskSize  Integer
}

type PriceLevelUpdate struct {
	Message
	Order
}
