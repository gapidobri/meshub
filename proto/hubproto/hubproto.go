// Package hubproto encodes the messages between meshub nodes and the hub.
// See docs/protocol.md for the protocol. It is shared by the node library
// (TinyGo) and the backend, so it avoids reflection and encoding/json.
package hubproto

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// Version is the protocol version carried in every message.
const Version = 1

// Plaintext size limits of the MeshCore datagrams messages travel in.
const (
	MaxPlaintext     = 167 // REQ and RESPONSE
	MaxAnonPlaintext = 136 // ANON_REQ
)

// ChunkSize is how many config bytes a ConfigChunk carries at most.
const ChunkSize = 128

const headerLen = 6

// Type identifies a message.
type Type uint8

const (
	TypeJoin      Type = 0x01
	TypeHello     Type = 0x02
	TypeConfigGet Type = 0x03
	TypeEvent     Type = 0x04

	TypeJoinReply   Type = 0x81
	TypeHelloReply  Type = 0x82
	TypeConfigChunk Type = 0x83
	TypeEventAck    Type = 0x84
	TypeError       Type = 0xFF
)

// Wake reasons reported in Hello.
const (
	WakeUnknown = 0
	WakePowerOn = 1
	WakeTimer   = 2
	WakePin     = 3
	WakeOther   = 4
)

// JoinReply statuses.
const (
	JoinAccepted = 0
	JoinPending  = 1
	JoinRejected = 2
)

// Error codes.
const (
	ErrNotPaired     = 1
	ErrBadRequest    = 2
	ErrConfigChanged = 3
)

// Message is a protocol message.
type Message interface {
	Type() Type
	appendBody(dst []byte) ([]byte, error)
}

type Join struct {
	Program  string // what the node runs, e.g. "hydrant"
	Firmware string
	Claim    string // optional code printed on the device
}

type Hello struct {
	ConfigVersion uint32
	WakeReason    uint8
	BatteryMV     uint16
	UptimeS       uint32
	Firmware      string
}

type ConfigGet struct {
	ConfigVersion uint32
	Offset        uint16
}

type Event struct {
	EventSeq uint32
	Kind     string
	Data     Value
}

type JoinReply struct {
	Status uint8
}

type HelloReply struct {
	Time          uint32 // hub clock, unix seconds
	PathLen       uint8  // MeshCore encoded path length of the node's path to the hub
	Path          []byte
	ConfigVersion uint32
	ConfigLen     uint16
	ConfigCRC     uint32
	SleepS        uint32 // suggested sleep; 0 lets the node decide
}

type ConfigChunk struct {
	ConfigVersion uint32
	Offset        uint16
	Data          []byte
}

type EventAck struct {
	EventSeq uint32
}

type Error struct {
	Code uint8
}

func (*Join) Type() Type        { return TypeJoin }
func (*Hello) Type() Type       { return TypeHello }
func (*ConfigGet) Type() Type   { return TypeConfigGet }
func (*Event) Type() Type       { return TypeEvent }
func (*JoinReply) Type() Type   { return TypeJoinReply }
func (*HelloReply) Type() Type  { return TypeHelloReply }
func (*ConfigChunk) Type() Type { return TypeConfigChunk }
func (*EventAck) Type() Type    { return TypeEventAck }
func (*Error) Type() Type       { return TypeError }

var (
	errShort    = errors.New("hubproto: message truncated")
	errVersion  = errors.New("hubproto: unsupported protocol version")
	errType     = errors.New("hubproto: unknown message type")
	errStrLong  = errors.New("hubproto: string longer than 255 bytes")
	errTooLarge = errors.New("hubproto: message larger than a datagram")
	errPath     = errors.New("hubproto: invalid path")
)

// Encode appends the plaintext of m with sequence number seq. It fails if
// the message does not fit max bytes (MaxPlaintext or MaxAnonPlaintext).
func Encode(dst []byte, seq uint32, m Message, max int) ([]byte, error) {
	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, seq)
	dst = append(dst, Version, byte(m.Type()))
	dst, err := m.appendBody(dst)
	if err != nil {
		return dst[:start], err
	}
	if len(dst)-start > max {
		return dst[:start], errTooLarge
	}
	return dst, nil
}

// Decode parses a decrypted datagram. Trailing zero padding is ignored.
func Decode(src []byte) (seq uint32, m Message, err error) {
	if len(src) < headerLen {
		return 0, nil, errShort
	}
	seq = binary.LittleEndian.Uint32(src)
	if src[4] != Version {
		return seq, nil, errVersion
	}
	r := reader{b: src[headerLen:]}
	switch Type(src[5]) {
	case TypeJoin:
		m = &Join{Program: r.str(), Firmware: r.str(), Claim: r.str()}
	case TypeHello:
		m = &Hello{ConfigVersion: r.u32(), WakeReason: r.u8(), BatteryMV: r.u16(), UptimeS: r.u32(), Firmware: r.str()}
	case TypeConfigGet:
		m = &ConfigGet{ConfigVersion: r.u32(), Offset: r.u16()}
	case TypeEvent:
		e := &Event{EventSeq: r.u32(), Kind: r.str()}
		e.Data = r.value()
		m = e
	case TypeJoinReply:
		m = &JoinReply{Status: r.u8()}
	case TypeHelloReply:
		h := &HelloReply{Time: r.u32(), PathLen: r.u8()}
		h.Path = r.bytes(pathByteLen(h.PathLen))
		h.ConfigVersion, h.ConfigLen, h.ConfigCRC, h.SleepS = r.u32(), r.u16(), r.u32(), r.u32()
		m = h
	case TypeConfigChunk:
		c := &ConfigChunk{ConfigVersion: r.u32(), Offset: r.u16()}
		c.Data = r.bytes(int(r.u8()))
		m = c
	case TypeEventAck:
		m = &EventAck{EventSeq: r.u32()}
	case TypeError:
		m = &Error{Code: r.u8()}
	default:
		return seq, nil, errType
	}
	if r.err != nil {
		return seq, nil, r.err
	}
	return seq, m, nil
}

func (m *Join) appendBody(dst []byte) ([]byte, error) {
	return appendStrs(dst, m.Program, m.Firmware, m.Claim)
}

func (m *Hello) appendBody(dst []byte) ([]byte, error) {
	dst = binary.LittleEndian.AppendUint32(dst, m.ConfigVersion)
	dst = append(dst, m.WakeReason)
	dst = binary.LittleEndian.AppendUint16(dst, m.BatteryMV)
	dst = binary.LittleEndian.AppendUint32(dst, m.UptimeS)
	return appendStrs(dst, m.Firmware)
}

func (m *ConfigGet) appendBody(dst []byte) ([]byte, error) {
	dst = binary.LittleEndian.AppendUint32(dst, m.ConfigVersion)
	return binary.LittleEndian.AppendUint16(dst, m.Offset), nil
}

func (m *Event) appendBody(dst []byte) ([]byte, error) {
	dst = binary.LittleEndian.AppendUint32(dst, m.EventSeq)
	dst, err := appendStrs(dst, m.Kind)
	if err != nil {
		return dst, err
	}
	return m.Data.AppendCBOR(dst), nil
}

func (m *JoinReply) appendBody(dst []byte) ([]byte, error) { return append(dst, m.Status), nil }

func (m *HelloReply) appendBody(dst []byte) ([]byte, error) {
	n := pathByteLen(m.PathLen)
	if n < 0 || n != len(m.Path) {
		return dst, errPath
	}
	dst = binary.LittleEndian.AppendUint32(dst, m.Time)
	dst = append(dst, m.PathLen)
	dst = append(dst, m.Path...)
	dst = binary.LittleEndian.AppendUint32(dst, m.ConfigVersion)
	dst = binary.LittleEndian.AppendUint16(dst, m.ConfigLen)
	dst = binary.LittleEndian.AppendUint32(dst, m.ConfigCRC)
	return binary.LittleEndian.AppendUint32(dst, m.SleepS), nil
}

func (m *ConfigChunk) appendBody(dst []byte) ([]byte, error) {
	if len(m.Data) > ChunkSize {
		return dst, errTooLarge
	}
	dst = binary.LittleEndian.AppendUint32(dst, m.ConfigVersion)
	dst = binary.LittleEndian.AppendUint16(dst, m.Offset)
	dst = append(dst, byte(len(m.Data)))
	return append(dst, m.Data...), nil
}

func (m *EventAck) appendBody(dst []byte) ([]byte, error) {
	return binary.LittleEndian.AppendUint32(dst, m.EventSeq), nil
}

func (m *Error) appendBody(dst []byte) ([]byte, error) { return append(dst, m.Code), nil }

// ConfigCRC is the checksum HelloReply carries for an encoded config.
func ConfigCRC(config []byte) uint32 { return crc32.ChecksumIEEE(config) }

// pathByteLen returns the byte length of a MeshCore encoded path length, or
// -1 if it is invalid.
func pathByteLen(pathLen uint8) int {
	size := int(pathLen>>6) + 1
	if size == 4 {
		return -1
	}
	n := int(pathLen&63) * size
	if n > 64 {
		return -1
	}
	return n
}

func appendStrs(dst []byte, strs ...string) ([]byte, error) {
	for _, s := range strs {
		if len(s) > 255 {
			return dst, errStrLong
		}
		dst = append(dst, byte(len(s)))
		dst = append(dst, s...)
	}
	return dst, nil
}

// reader consumes a message body, recording the first error.
type reader struct {
	b   []byte
	err error
}

func (r *reader) bytes(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 {
		r.err = errPath
		return nil
	}
	if n > len(r.b) {
		r.err = errShort
		return nil
	}
	b := r.b[:n:n]
	r.b = r.b[n:]
	return b
}

func (r *reader) u8() uint8 {
	if b := r.bytes(1); b != nil {
		return b[0]
	}
	return 0
}

func (r *reader) u16() uint16 {
	if b := r.bytes(2); b != nil {
		return binary.LittleEndian.Uint16(b)
	}
	return 0
}

func (r *reader) u32() uint32 {
	if b := r.bytes(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}

func (r *reader) str() string { return string(r.bytes(int(r.u8()))) }

func (r *reader) value() Value {
	if r.err != nil {
		return Value{}
	}
	v, n, err := DecodeCBOR(r.b)
	if err != nil {
		r.err = err
		return Value{}
	}
	r.b = r.b[n:]
	return v
}
