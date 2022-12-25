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

func (p Price) Value() float64 {
	return (float64)(p) / 1e4
}

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

type Order struct {
	Size Short
	Price
}
type TradeReport struct {
	Message
	Order
}

type QuoteUpdate struct {
	Message
	BidSize  Short
	BidPrice Price
	AskPrice Price
	AskSize  Short
}

type PriceLevelUpdate struct {
	Message
	Order
}
