package node

import (
	"crypto/rand"
	"testing"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/proto/hubproto"
)

// fakeHub answers a node's requests the moment it transmits them, with a
// meshcore.Mesh that decrypts them like the real hub's. Its replies come
// back zero hop. Tests change its answers through its fields.
type fakeHub struct {
	t     *testing.T
	id    meshcore.LocalIdentity
	mesh  *meshcore.Mesh
	radio hubRadio
	node  meshcore.Identity // learnt from the join, or set by the test

	// Behaviour.
	joinStatus uint8
	time       uint32
	version    uint32
	config     []byte // CBOR
	commands   []hubproto.Command
	drop       func(m hubproto.Message) bool // leave a request unanswered
	override   func(m hubproto.Message) hubproto.Message

	// What it received.
	reqs     []hubReq
	events   []hubproto.Event
	statuses []hubproto.ConfigStatus
	results  []hubproto.CommandResult
	frag     []byte

	got    *hubReq
	toNode [][]byte
}

type hubReq struct {
	seq    uint32
	msg    hubproto.Message
	direct bool
	anon   bool
}

func newFakeHub(t *testing.T) *fakeHub {
	seed := make([]byte, 32)
	rand.Read(seed)
	h := &fakeHub{t: t, id: meshcore.LocalIdentityFromSeed(seed), time: 1_800_000_000}
	ms := meshcore.NewSystemMillis()
	h.mesh = meshcore.NewMesh(meshcore.Config{
		Radio:   &h.radio,
		Millis:  ms,
		RNG:     testRNG{},
		RTC:     meshcore.NewVolatileRTCClock(ms),
		Packets: meshcore.NewStaticPoolPacketManager(8),
		Tables:  &meshcore.SimpleMeshTables{},
		Handler: &hubHandler{h: h},
	})
	h.mesh.SelfID = h.id
	h.mesh.Begin()
	return h
}

// requests returns the messages received, fragments included.
func (h *fakeHub) requests() []hubproto.Message {
	out := make([]hubproto.Message, len(h.reqs))
	for i, r := range h.reqs {
		out[i] = r.msg
	}
	return out
}

func (h *fakeHub) count(typ hubproto.Type) int {
	n := 0
	for _, r := range h.reqs {
		if r.msg.Type() == typ {
			n++
		}
	}
	return n
}

// receive handles a packet the node transmitted.
func (h *fakeHub) receive(raw []byte) {
	h.radio.frame = raw
	h.got = nil
	h.mesh.Loop()
	req := h.got
	if req == nil {
		h.t.Errorf("hub could not decode a packet from the node")
		return
	}
	h.reqs = append(h.reqs, *req)
	if h.drop != nil && h.drop(req.msg) {
		return
	}
	reply := h.answer(req.msg)
	if reply == nil {
		return
	}
	pt, err := hubproto.Encode(nil, req.seq, reply, hubproto.MaxPlaintext)
	if err != nil {
		h.t.Fatalf("encode reply %T: %v", reply, err)
	}
	var secret [meshcore.PubKeySize]byte
	h.id.SharedSecret(&secret, &h.node.PubKey)
	pkt := h.mesh.CreateDatagram(meshcore.PayloadTypeResponse, &h.node, secret[:], pt)
	pkt.Header = pkt.Header&^meshcore.PHRouteMask | meshcore.RouteTypeDirect
	pkt.PathLen = 0
	buf := make([]byte, meshcore.MaxTransUnit)
	h.toNode = append(h.toNode, buf[:pkt.Encode(buf)])
	h.mesh.ReleasePacket(pkt)
}

func (h *fakeHub) answer(m hubproto.Message) hubproto.Message {
	if h.override != nil {
		if r := h.override(m); r != nil {
			return r
		}
	}
	switch m := m.(type) {
	case *hubproto.Join:
		return &hubproto.JoinReply{Status: h.joinStatus}
	case *hubproto.Hello:
		return &hubproto.HelloReply{
			Time: h.time, ConfigVersion: h.version, ConfigLen: uint16(len(h.config)),
			ConfigCRC: hubproto.ConfigCRC(h.config), Commands: uint8(len(h.commands)),
		}
	case *hubproto.ConfigGet:
		if m.ConfigVersion != h.version {
			return &hubproto.Error{Code: hubproto.ErrConfigChanged}
		}
		end := min(int(m.Offset)+hubproto.ChunkSize, len(h.config))
		return &hubproto.ConfigChunk{ConfigVersion: h.version, Offset: m.Offset, Data: h.config[m.Offset:end]}
	case *hubproto.Events:
		h.events = append(h.events, m.Events...)
		return &hubproto.Ack{}
	case *hubproto.ConfigStatus:
		h.statuses = append(h.statuses, *m)
		return &hubproto.Ack{}
	case *hubproto.CommandGet:
		if len(h.commands) == 0 {
			return &hubproto.Command{Args: hubproto.Null()}
		}
		return &h.commands[0]
	case *hubproto.CommandResult:
		h.results = append(h.results, *m)
		for i, c := range h.commands {
			if c.ID == m.ID {
				h.commands = append(h.commands[:i], h.commands[i+1:]...)
				break
			}
		}
		return &hubproto.Ack{}
	case *hubproto.Fragment:
		if int(m.Offset) > len(h.frag) {
			h.t.Errorf("fragment at %d after %d bytes", m.Offset, len(h.frag))
			return &hubproto.Error{Code: hubproto.ErrBadRequest}
		}
		h.frag = append(h.frag[:m.Offset], m.Data...)
		if len(h.frag) < int(m.Total) {
			return &hubproto.Ack{}
		}
		inner, err := hubproto.DecodeMessage(h.frag)
		if err != nil {
			h.t.Errorf("reassembled message: %v", err)
			return &hubproto.Error{Code: hubproto.ErrBadRequest}
		}
		return h.answer(inner)
	}
	return &hubproto.Error{Code: hubproto.ErrBadRequest}
}

// hubHandler is the fake hub's mesh handler.
type hubHandler struct {
	meshcore.BaseHandler
	h *fakeHub
}

func (*hubHandler) CalcRxDelay(float32, uint32) int { return 0 }

func (x *hubHandler) SearchPeersByHash(hash []byte) int {
	if x.h.node.IsHashMatch(hash) {
		return 1
	}
	return 0
}

func (x *hubHandler) PeerSharedSecret(dst *[meshcore.PubKeySize]byte, _ int) {
	x.h.id.SharedSecret(dst, &x.h.node.PubKey)
}

func (x *hubHandler) OnPeerDataRecv(p *meshcore.Packet, typ uint8, _ int, _ *[meshcore.PubKeySize]byte, data []byte) {
	if typ == meshcore.PayloadTypeReq {
		x.decode(p, data, false)
	}
}

func (x *hubHandler) OnAnonDataRecv(p *meshcore.Packet, _ *[meshcore.PubKeySize]byte, sender *meshcore.Identity, data []byte) {
	x.h.node = *sender
	x.decode(p, data, true)
}

func (x *hubHandler) decode(p *meshcore.Packet, data []byte, anon bool) {
	seq, m, err := hubproto.Decode(data)
	if err != nil {
		x.h.t.Errorf("hub decode: %v", err)
		return
	}
	x.h.got = &hubReq{seq: seq, msg: m, direct: p.IsRouteDirect(), anon: anon}
}

// hubRadio feeds one frame to the fake hub's mesh and sends nothing.
type hubRadio struct {
	meshcore.RadioBase
	frame []byte
}

func (r *hubRadio) RecvRaw(buf []byte) int {
	n := copy(buf, r.frame)
	r.frame = nil
	return n
}
func (*hubRadio) EstAirtimeFor(int) uint32         { return 0 }
func (*hubRadio) PacketScore(float32, int) float32 { return 1 }
func (*hubRadio) StartSendRaw([]byte) bool         { return true }
func (*hubRadio) IsSendComplete() bool             { return true }
func (*hubRadio) OnSendFinished()                  {}
func (*hubRadio) IsInRecvMode() bool               { return true }

// nodeRadio connects a node to a fakeHub.
type nodeRadio struct {
	meshcore.RadioBase
	hub *fakeHub
}

func (r *nodeRadio) RecvRaw(buf []byte) int {
	if len(r.hub.toNode) == 0 {
		return 0
	}
	n := copy(buf, r.hub.toNode[0])
	r.hub.toNode = r.hub.toNode[1:]
	return n
}

func (r *nodeRadio) StartSendRaw(b []byte) bool {
	r.hub.receive(append([]byte(nil), b...))
	return true
}

func (*nodeRadio) EstAirtimeFor(int) uint32         { return 1 }
func (*nodeRadio) PacketScore(float32, int) float32 { return 1 }
func (*nodeRadio) IsSendComplete() bool             { return true }
func (*nodeRadio) OnSendFinished()                  {}
func (*nodeRadio) IsInRecvMode() bool               { return true }

type testRNG struct{}

func (testRNG) Random(dst []byte) { rand.Read(dst) }

// countingStorage is a MemStorage that counts saves per key.
type countingStorage struct {
	MemStorage
	saves map[string]int
}

func (s *countingStorage) Save(key string, data []byte) error {
	if s.saves == nil {
		s.saves = map[string]int{}
	}
	s.saves[key]++
	return s.MemStorage.Save(key, data)
}

// testNode boots a node on storage, talking to hub. Timeouts are short,
// and there is no start delay or backoff unless opts set them.
func testNode(t *testing.T, hub *fakeHub, storage Storage, opts ...func(*Config)) *Node {
	t.Helper()
	ms := meshcore.NewSystemMillis()
	cfg := Config{
		Radio: &nodeRadio{hub: hub}, Millis: ms, RNG: testRNG{}, RTC: meshcore.NewVolatileRTCClock(ms),
		Storage: storage, Hub: hub.id.Identity, Program: "test", Firmware: "1",
		ReplyTimeout: 300 * time.Millisecond, MinReplyTimeout: 50 * time.Millisecond,
		StartDelay: -1, RetryBackoff: -1,
	}
	for _, o := range opts {
		o(&cfg)
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	hub.node = n.mesh.SelfID.Identity
	return n
}
