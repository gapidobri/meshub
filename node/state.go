package node

import (
	"encoding/binary"
	"errors"

	"github.com/gapidobri/meshub/proto/hubproto"
)

// Storage keys.
const (
	keyIdentity     = "identity"  // 32-byte seed
	keyState        = "state"     // state, see encodeState
	keyConfig       = "config"    // config version (u32) + CBOR
	keyEvents       = "events"    // CBOR list of pending events
	keyConfigStatus = "cfgstatus" // configStatus, see encodeConfigStatus
	keyResult       = "cmdresult" // CBOR command result not yet acknowledged
)

// seqReserve is how many request numbers are reserved per write of the
// counter, so the node does not write storage on every request. Up to this
// many numbers are skipped after a reboot; they are never reused.
const seqReserve = 16

var errBadState = errors.New("node: stored state is corrupt")

// state is the small, frequently written part of what the node persists.
type state struct {
	seqLimit  uint32 // requests may use numbers below this without saving
	paired    bool
	hasPath   bool
	pathLen   uint8
	path      []byte
	nextEvent uint32
}

// encodeState lays out state as:
//
//	[0:4] seqLimit  [4] flags (1 paired, 2 hasPath)  [5:9] nextEvent
//	[9] pathLen  [10:] path
func encodeState(s *state) []byte {
	b := binary.LittleEndian.AppendUint32(nil, s.seqLimit)
	var flags byte
	if s.paired {
		flags |= 1
	}
	if s.hasPath {
		flags |= 2
	}
	b = append(b, flags)
	b = binary.LittleEndian.AppendUint32(b, s.nextEvent)
	b = append(b, s.pathLen)
	return append(b, s.path...)
}

func decodeState(b []byte) (state, error) {
	var s state
	if len(b) < 10 || b[4]&^3 != 0 {
		return s, errBadState
	}
	s.seqLimit = binary.LittleEndian.Uint32(b)
	s.paired = b[4]&1 != 0
	s.hasPath = b[4]&2 != 0
	s.nextEvent = binary.LittleEndian.Uint32(b[5:])
	s.pathLen = b[9]
	s.path = append([]byte(nil), b[10:]...)
	if s.hasPath && pathBytes(s.pathLen) != len(s.path) {
		return state{}, errBadState
	}
	return s, nil
}

// pathBytes returns the byte length of a MeshCore encoded path length, or
// -1 if it is invalid.
func pathBytes(pathLen uint8) int {
	size := int(pathLen>>6) + 1
	if size == 4 || int(pathLen&63)*size > 64 {
		return -1
	}
	return int(pathLen&63) * size
}

// pendingEvent is a reported event not yet acknowledged by the hub.
type pendingEvent struct {
	seq  uint32
	time uint32 // unix seconds, 0 if not known
	kind string
	data hubproto.Value

	// reportedAt is the Millis clock when it was reported, for stamping its
	// time once the hub sets the clock. It is not stored: after a reboot
	// the clock reading is meaningless.
	reportedAt uint32
	unstamped  bool
}

func (e *pendingEvent) wire() hubproto.Event {
	return hubproto.Event{Seq: e.seq, Time: e.time, Kind: e.kind, Data: e.data}
}

func encodeEvents(events []pendingEvent) []byte {
	items := make([]hubproto.Value, len(events))
	for i, e := range events {
		items[i] = hubproto.List(hubproto.Int(int64(e.seq)), hubproto.String(e.kind), e.data, hubproto.Int(int64(e.time)))
	}
	return hubproto.List(items...).AppendCBOR(nil)
}

func decodeEvents(b []byte) ([]pendingEvent, error) {
	if len(b) == 0 {
		return nil, nil
	}
	v, _, err := hubproto.DecodeCBOR(b)
	if err != nil {
		return nil, err
	}
	if v.Kind() != hubproto.KindList {
		return nil, errBadState
	}
	events := make([]pendingEvent, 0, v.Len())
	for i := 0; i < v.Len(); i++ {
		e := v.Index(i)
		// Version 1 firmware stored events without a time.
		if e.Kind() != hubproto.KindList || e.Len() < 3 || e.Len() > 4 ||
			e.Index(0).Kind() != hubproto.KindInt || e.Index(1).Kind() != hubproto.KindString {
			return nil, errBadState
		}
		pe := pendingEvent{
			seq:  uint32(e.Index(0).Int(0)),
			kind: e.Index(1).String(""),
			data: e.Index(2),
		}
		if e.Len() == 4 {
			pe.time = uint32(e.Index(3).Int(0))
		}
		events = append(events, pe)
	}
	return events, nil
}

// configStatus is the outcome of the last config the node received, kept
// until the hub has been told.
type configStatus struct {
	version  uint32
	rejected bool
	reported bool
	reason   string
}

// encodeConfigStatus lays out a configStatus as:
//
//	[0:4] version  [4] flags (1 rejected, 2 reported)  [5:] reason
func encodeConfigStatus(c *configStatus) []byte {
	b := binary.LittleEndian.AppendUint32(nil, c.version)
	var flags byte
	if c.rejected {
		flags |= 1
	}
	if c.reported {
		flags |= 2
	}
	return append(append(b, flags), c.reason...)
}

func decodeConfigStatus(b []byte) (configStatus, error) {
	if len(b) < 5 || b[4]&^3 != 0 {
		return configStatus{}, errBadState
	}
	return configStatus{
		version:  binary.LittleEndian.Uint32(b),
		rejected: b[4]&1 != 0,
		reported: b[4]&2 != 0,
		reason:   string(b[5:]),
	}, nil
}

func encodeResult(r *hubproto.CommandResult) []byte {
	return hubproto.List(hubproto.Int(int64(r.ID)), hubproto.Int(int64(r.Status)), r.Result).AppendCBOR(nil)
}

func decodeResult(b []byte) (*hubproto.CommandResult, error) {
	v, _, err := hubproto.DecodeCBOR(b)
	if err != nil {
		return nil, err
	}
	if v.Kind() != hubproto.KindList || v.Len() != 3 {
		return nil, errBadState
	}
	return &hubproto.CommandResult{ID: uint32(v.Index(0).Int(0)), Status: uint8(v.Index(1).Int(0)), Result: v.Index(2)}, nil
}
