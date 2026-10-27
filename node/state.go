package node

import (
	"encoding/binary"
	"errors"

	"github.com/gapidobri/meshub/proto/hubproto"
)

// Storage keys.
const (
	keyIdentity = "identity" // 32-byte seed
	keyState    = "state"    // state, see encodeState
	keyConfig   = "config"   // config version (u32) + CBOR
	keyEvents   = "events"   // CBOR list of pending events
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
	if len(b) < 10 {
		return s, errBadState
	}
	s.seqLimit = binary.LittleEndian.Uint32(b)
	s.paired = b[4]&1 != 0
	s.hasPath = b[4]&2 != 0
	s.nextEvent = binary.LittleEndian.Uint32(b[5:])
	s.pathLen = b[9]
	s.path = append([]byte(nil), b[10:]...)
	return s, nil
}

// pendingEvent is a reported event not yet acknowledged by the hub.
type pendingEvent struct {
	seq  uint32
	kind string
	data hubproto.Value
}

func encodeEvents(events []pendingEvent) []byte {
	items := make([]hubproto.Value, len(events))
	for i, e := range events {
		items[i] = hubproto.List(hubproto.Int(int64(e.seq)), hubproto.String(e.kind), e.data)
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
	events := make([]pendingEvent, 0, v.Len())
	for i := 0; i < v.Len(); i++ {
		e := v.Index(i)
		if e.Kind() != hubproto.KindList || e.Len() != 3 {
			return nil, errBadState
		}
		events = append(events, pendingEvent{
			seq:  uint32(e.Index(0).Int(0)),
			kind: e.Index(1).String(""),
			data: e.Index(2),
		})
	}
	return events, nil
}
