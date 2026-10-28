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
const Version = 2

// Plaintext size limits of the MeshCore datagrams messages travel in.
const (
	MaxPlaintext     = 167 // REQ and RESPONSE
	MaxAnonPlaintext = 136 // ANON_REQ
)

// ChunkSize is how many config bytes a ConfigChunk carries at most.
const ChunkSize = 128

// MaxMessage is the largest message (type and body) a node may send split
// into Fragments.
const MaxMessage = 2048

// FragmentSize is how many bytes of a large message one Fragment carries.
const FragmentSize = MaxPlaintext - headerLen - fragmentHeader

const (
	headerLen      = 6
	fragmentHeader = 7 // xfer, total, offset, len
)

// Type identifies a message.
type Type uint8

const (
	TypeJoin          Type = 0x01
	TypeHello         Type = 0x02
	TypeConfigGet     Type = 0x03
	TypeEvents        Type = 0x04
	TypeConfigStatus  Type = 0x05
	TypeCommandGet    Type = 0x06
	TypeCommandResult Type = 0x07
	TypeFragment      Type = 0x08

	TypeJoinReply   Type = 0x81
	TypeHelloReply  Type = 0x82
	TypeConfigChunk Type = 0x83
	TypeAck         Type = 0x84
	TypeCommand     Type = 0x85
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

// ConfigStatus statuses.
const (
	ConfigApplied  = 0
	ConfigRejected = 1
)

// CommandResult statuses.
const (
	CommandOK      = 0
	CommandFailed  = 1 // Result is the error message
	CommandUnknown = 2
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
	Program  string // what the node runs, e.g. "weather"
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

// Events carries one or more events.
type Events struct {
	Events []Event
}

// Event is one event in an Events message.
type Event struct {
	Seq  uint32
	Time uint32 // when it happened, unix seconds by the node's clock; 0 if unknown
	Kind string
	Data Value
}

// ConfigStatus reports whether the node applied a config version.
type ConfigStatus struct {
	ConfigVersion uint32
	Status        uint8
	Reason        string // why it was rejected
}

type CommandGet struct{}

type CommandResult struct {
	ID     uint32
	Status uint8
	Result Value
}

// Fragment carries part of a message larger than a datagram. The hub
// acknowledges each fragment but the last, which it answers as it would
// the whole message.
type Fragment struct {
	Xfer   uint16 // identifies the message being sent
	Total  uint16 // its length
	Offset uint16
	Data   []byte
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
	Commands      uint8 // commands waiting for the node
}

type ConfigChunk struct {
	ConfigVersion uint32
	Offset        uint16
	Data          []byte
}

// Ack accepts a request that needs no other answer: events, a config
// status, a command result or a fragment.
type Ack struct{}

// Command is a command for the node. ID 0 means there is none.
type Command struct {
	ID   uint32
	Name string
	Args Value
}

type Error struct {
	Code uint8
}

func (*Join) Type() Type          { return TypeJoin }
func (*Hello) Type() Type         { return TypeHello }
func (*ConfigGet) Type() Type     { return TypeConfigGet }
func (*Events) Type() Type        { return TypeEvents }
func (*ConfigStatus) Type() Type  { return TypeConfigStatus }
func (*CommandGet) Type() Type    { return TypeCommandGet }
func (*CommandResult) Type() Type { return TypeCommandResult }
func (*Fragment) Type() Type      { return TypeFragment }
func (*JoinReply) Type() Type     { return TypeJoinReply }
func (*HelloReply) Type() Type    { return TypeHelloReply }
func (*ConfigChunk) Type() Type   { return TypeConfigChunk }
func (*Ack) Type() Type           { return TypeAck }
func (*Command) Type() Type       { return TypeCommand }
func (*Error) Type() Type         { return TypeError }

// ErrTooLarge is returned by Encode for a message that does not fit.
var ErrTooLarge = errors.New("hubproto: message larger than a datagram")

var (
	errShort   = errors.New("hubproto: message truncated")
	errVersion = errors.New("hubproto: unsupported protocol version")
	errType    = errors.New("hubproto: unknown message type")
	errStrLong = errors.New("hubproto: string longer than 255 bytes")
	errPath    = errors.New("hubproto: invalid path")
	errCount   = errors.New("hubproto: too many events")
)

// Encode appends the plaintext of m with sequence number seq. It fails with
// ErrTooLarge if the message does not fit max bytes (MaxPlaintext or
// MaxAnonPlaintext).
func Encode(dst []byte, seq uint32, m Message, max int) ([]byte, error) {
	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, seq)
	dst = append(dst, Version)
	dst, err := AppendMessage(dst, m)
	if err != nil {
		return dst[:start], err
	}
	if len(dst)-start > max {
		return dst[:start], ErrTooLarge
	}
	return dst, nil
}

// AppendMessage appends m's type and body, the part of a plaintext that
// Fragments carry.
func AppendMessage(dst []byte, m Message) ([]byte, error) {
	start := len(dst)
	dst, err := m.appendBody(append(dst, byte(m.Type())))
	if err != nil {
		return dst[:start], err
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
	m, err = DecodeMessage(src[5:])
	return seq, m, err
}

// DecodeMessage parses a message's type and body, as reassembled from
// Fragments.
func DecodeMessage(src []byte) (Message, error) {
	if len(src) < 1 {
		return nil, errShort
	}
	var m Message
	r := reader{b: src[1:]}
	switch Type(src[0]) {
	case TypeJoin:
		m = &Join{Program: r.str(), Firmware: r.str(), Claim: r.str()}
	case TypeHello:
		m = &Hello{ConfigVersion: r.u32(), WakeReason: r.u8(), BatteryMV: r.u16(), UptimeS: r.u32(), Firmware: r.str()}
	case TypeConfigGet:
		m = &ConfigGet{ConfigVersion: r.u32(), Offset: r.u16()}
	case TypeEvents:
		n := int(r.u8())
		e := &Events{Events: make([]Event, 0, n)}
		for i := 0; i < n && r.err == nil; i++ {
			ev := Event{Seq: r.u32(), Time: r.u32(), Kind: r.str()}
			ev.Data = r.value()
			e.Events = append(e.Events, ev)
		}
		m = e
	case TypeConfigStatus:
		m = &ConfigStatus{ConfigVersion: r.u32(), Status: r.u8(), Reason: r.str()}
	case TypeCommandGet:
		m = &CommandGet{}
	case TypeCommandResult:
		c := &CommandResult{ID: r.u32(), Status: r.u8()}
		c.Result = r.value()
		m = c
	case TypeFragment:
		f := &Fragment{Xfer: r.u16(), Total: r.u16(), Offset: r.u16()}
		f.Data = r.bytes(int(r.u8()))
		m = f
	case TypeJoinReply:
		m = &JoinReply{Status: r.u8()}
	case TypeHelloReply:
		h := &HelloReply{Time: r.u32(), PathLen: r.u8()}
		h.Path = r.bytes(pathByteLen(h.PathLen))
		h.ConfigVersion, h.ConfigLen, h.ConfigCRC, h.Commands = r.u32(), r.u16(), r.u32(), r.u8()
		m = h
	case TypeConfigChunk:
		c := &ConfigChunk{ConfigVersion: r.u32(), Offset: r.u16()}
		c.Data = r.bytes(int(r.u8()))
		m = c
	case TypeAck:
		m = &Ack{}
	case TypeCommand:
		c := &Command{ID: r.u32(), Name: r.str()}
		c.Args = r.value()
		m = c
	case TypeError:
		m = &Error{Code: r.u8()}
	default:
		return nil, errType
	}
	if r.err != nil {
		return nil, r.err
	}
	return m, nil
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

func (m *Events) appendBody(dst []byte) ([]byte, error) {
	if len(m.Events) > 255 {
		return dst, errCount
	}
	dst = append(dst, byte(len(m.Events)))
	for _, e := range m.Events {
		dst = binary.LittleEndian.AppendUint32(dst, e.Seq)
		dst = binary.LittleEndian.AppendUint32(dst, e.Time)
		var err error
		if dst, err = appendStrs(dst, e.Kind); err != nil {
			return dst, err
		}
		dst = e.Data.AppendCBOR(dst)
	}
	return dst, nil
}

func (m *ConfigStatus) appendBody(dst []byte) ([]byte, error) {
	dst = binary.LittleEndian.AppendUint32(dst, m.ConfigVersion)
	return appendStrs(append(dst, m.Status), m.Reason)
}

func (*CommandGet) appendBody(dst []byte) ([]byte, error) { return dst, nil }

func (m *CommandResult) appendBody(dst []byte) ([]byte, error) {
	dst = binary.LittleEndian.AppendUint32(dst, m.ID)
	return m.Result.AppendCBOR(append(dst, m.Status)), nil
}

func (m *Fragment) appendBody(dst []byte) ([]byte, error) {
	if len(m.Data) > FragmentSize {
		return dst, ErrTooLarge
	}
	dst = binary.LittleEndian.AppendUint16(dst, m.Xfer)
	dst = binary.LittleEndian.AppendUint16(dst, m.Total)
	dst = binary.LittleEndian.AppendUint16(dst, m.Offset)
	dst = append(dst, byte(len(m.Data)))
	return append(dst, m.Data...), nil
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
	return append(dst, m.Commands), nil
}

func (m *ConfigChunk) appendBody(dst []byte) ([]byte, error) {
	if len(m.Data) > ChunkSize {
		return dst, ErrTooLarge
	}
	dst = binary.LittleEndian.AppendUint32(dst, m.ConfigVersion)
	dst = binary.LittleEndian.AppendUint16(dst, m.Offset)
	dst = append(dst, byte(len(m.Data)))
	return append(dst, m.Data...), nil
}

func (*Ack) appendBody(dst []byte) ([]byte, error) { return dst, nil }

func (m *Command) appendBody(dst []byte) ([]byte, error) {
	dst = binary.LittleEndian.AppendUint32(dst, m.ID)
	dst, err := appendStrs(dst, m.Name)
	if err != nil {
		return dst, err
	}
	return m.Args.AppendCBOR(dst), nil
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
