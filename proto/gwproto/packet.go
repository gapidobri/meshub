package gwproto

import (
	"strconv"
	"time"
)

// Packet directions.
const (
	DirectionRx = "rx"
	DirectionTx = "tx"
)

// PacketEvent is a packet a gateway received or transmitted, as the gateway
// knows it. AppendJSON turns it into the uplink message.
type PacketEvent struct {
	Origin   string // gateway name
	OriginID string // gateway public key, upper-case hex
	Time     time.Time
	TX       bool // transmitted by the gateway rather than received

	Raw         []byte // the packet in wire format
	PayloadType uint8
	Direct      bool // route type is direct rather than flood
	PayloadLen  int
	Hash        []byte // meshcore.Packet.CalculateHash; may be empty

	// Receive metadata, ignored when TX is set.
	SNR       int
	RSSI      int
	Score     int // packet score * 1000
	AirtimeMs uint32
}

// AppendJSON appends the meshcoretomqtt-compatible JSON encoding of e.
func (e *PacketEvent) AppendJSON(dst []byte) []byte {
	dst = append(dst, '{')
	dst = appendStrField(dst, "origin", e.Origin)
	dst = appendStrField(dst, "origin_id", e.OriginID)
	dst = appendTimestamp(dst, "timestamp", e.Time)
	dst = appendStrField(dst, "type", "PACKET")
	if e.TX {
		dst = appendStrField(dst, "direction", DirectionTx)
	} else {
		dst = appendStrField(dst, "direction", DirectionRx)
	}

	// The firmware log prints its clock as "15:04:05" and "day/month/year".
	t := e.Time.UTC()
	dst = append(appendKey(dst, "time"), '"')
	dst = t.AppendFormat(dst, "15:04:05")
	dst = append(dst, '"')
	dst = append(appendKey(dst, "date"), '"')
	dst = strconv.AppendInt(dst, int64(t.Day()), 10)
	dst = append(dst, '/')
	dst = strconv.AppendInt(dst, int64(t.Month()), 10)
	dst = append(dst, '/')
	dst = strconv.AppendInt(dst, int64(t.Year()), 10)
	dst = append(dst, '"')

	dst = appendIntStrField(dst, "len", int64(len(e.Raw)))
	dst = appendIntStrField(dst, "packet_type", int64(e.PayloadType))
	if e.Direct {
		dst = appendStrField(dst, "route", "D")
	} else {
		dst = appendStrField(dst, "route", "F")
	}
	dst = appendIntStrField(dst, "payload_len", int64(e.PayloadLen))
	dst = append(appendKey(dst, "raw"), '"')
	dst = appendHex(dst, e.Raw)
	dst = append(dst, '"')

	if !e.TX {
		dst = appendIntStrField(dst, "SNR", int64(e.SNR))
		dst = appendIntStrField(dst, "RSSI", int64(e.RSSI))
		dst = appendIntStrField(dst, "score", int64(e.Score))
		dst = appendIntStrField(dst, "duration", int64(e.AirtimeMs))
		if len(e.Hash) > 0 {
			dst = append(appendKey(dst, "hash"), '"')
			dst = appendHex(dst, e.Hash)
			dst = append(dst, '"')
		}
	}
	return append(dst, '}')
}

// Packet is the decoded uplink message. Numeric fields are strings because
// that is how meshcoretomqtt publishes them; the Int helper parses them.
type Packet struct {
	Origin     string `json:"origin"`
	OriginID   string `json:"origin_id"`
	Timestamp  string `json:"timestamp"`
	Type       string `json:"type"`
	Direction  string `json:"direction"`
	Time       string `json:"time"`
	Date       string `json:"date"`
	Len        string `json:"len"`
	PacketType string `json:"packet_type"`
	Route      string `json:"route"`
	PayloadLen string `json:"payload_len"`
	Raw        string `json:"raw"`
	SNR        string `json:"SNR,omitempty"`
	RSSI       string `json:"RSSI,omitempty"`
	Score      string `json:"score,omitempty"`
	Duration   string `json:"duration,omitempty"`
	Hash       string `json:"hash,omitempty"`
	Path       string `json:"path,omitempty"` // set by meshcoretomqtt for direct packets only
}

// Int parses one of the string-encoded numeric fields of Packet, returning 0
// if it is empty or malformed.
func Int(field string) int {
	v, _ := strconv.Atoi(field)
	return v
}
