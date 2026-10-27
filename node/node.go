// Package node is the device side of meshub: it pairs a MeshCore node with
// the hub, keeps its config in sync and delivers its events. It wraps
// meshcore-go and is hardware independent; the application supplies the
// radio, clocks and storage.
//
// A typical wake cycle:
//
//	n, err := node.New(cfg)
//	err = n.Sync(ctx)                  // pair, check in, fetch new config
//	threshold := n.Config().Get("audio.threshold").Int(40)
//	n.Report("flow", data)             // queued until the hub acknowledges it
//	err = n.Flush(ctx)
//	sleep(n.SleepHint())
//
// The node only runs the mesh while it waits for the hub, and never
// repeats other nodes' packets.
package node

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/proto/hubproto"
)

// Config configures a Node.
type Config struct {
	Radio   meshcore.Radio
	Millis  meshcore.MillisecondClock
	RNG     meshcore.RNG
	RTC     meshcore.RTCClock // set from the hub's clock on every check-in
	Storage Storage

	// Hub is the hub's public key, from the backend's web UI.
	Hub meshcore.Identity

	Program  string // what this node runs, e.g. "hydrant"
	Firmware string
	Claim    string // optional code printed on the device

	// ReplyTimeout is how long to wait for each reply (default 8s).
	ReplyTimeout time.Duration
	// MaxEvents bounds the queue of unacknowledged events (default 16); the
	// oldest are dropped beyond it.
	MaxEvents int
}

// Errors returned by Sync.
var (
	ErrPending  = errors.New("node: waiting for the hub operator to approve pairing")
	ErrRejected = errors.New("node: the hub rejected pairing")
	ErrNoReply  = errors.New("node: no reply from the hub")
	ErrConfig   = errors.New("node: config transfer failed")
)

const poolSize = 8

// Node is a meshub node. It is not safe for concurrent use.
type Node struct {
	cfg    Config
	mesh   *meshcore.Mesh
	h      handler
	secret [meshcore.PubKeySize]byte

	st      state
	seq     uint32
	events  []pendingEvent
	config  hubproto.Value
	version uint32
	sleep   time.Duration

	wake      uint8
	batteryMV uint16
	start     time.Time
}

// New creates a node, loading its state from cfg.Storage and creating an
// identity on first use. It starts the radio.
func New(cfg Config) (*Node, error) {
	if cfg.ReplyTimeout == 0 {
		cfg.ReplyTimeout = 8 * time.Second
	}
	if cfg.MaxEvents == 0 {
		cfg.MaxEvents = 16
	}
	n := &Node{cfg: cfg, start: time.Now()}

	id, err := n.loadIdentity()
	if err != nil {
		return nil, err
	}
	if b, err := cfg.Storage.Load(keyState); err != nil {
		return nil, err
	} else if b != nil {
		if n.st, err = decodeState(b); err != nil {
			return nil, err
		}
	}
	n.seq = n.st.seqLimit // numbers below it may have been used before a reboot
	if err := n.loadConfig(); err != nil {
		return nil, err
	}
	if b, err := cfg.Storage.Load(keyEvents); err != nil {
		return nil, err
	} else if n.events, err = decodeEvents(b); err != nil {
		return nil, err
	}

	id.SharedSecret(&n.secret, &cfg.Hub.PubKey)
	n.h.n = n
	n.mesh = meshcore.NewMesh(meshcore.Config{
		Radio:   cfg.Radio,
		Millis:  cfg.Millis,
		RNG:     cfg.RNG,
		RTC:     cfg.RTC,
		Packets: meshcore.NewStaticPoolPacketManager(poolSize),
		Tables:  &meshcore.SimpleMeshTables{},
		Handler: &n.h,
	})
	n.mesh.SelfID = id
	n.mesh.Begin()
	return n, nil
}

func (n *Node) loadIdentity() (meshcore.LocalIdentity, error) {
	seed, err := n.cfg.Storage.Load(keyIdentity)
	if err != nil {
		return meshcore.LocalIdentity{}, err
	}
	if len(seed) != 32 {
		seed = make([]byte, 32)
		n.cfg.RNG.Random(seed)
		if err := n.cfg.Storage.Save(keyIdentity, seed); err != nil {
			return meshcore.LocalIdentity{}, err
		}
	}
	return meshcore.LocalIdentityFromSeed(seed), nil
}

func (n *Node) loadConfig() error {
	b, err := n.cfg.Storage.Load(keyConfig)
	if err != nil || len(b) < 4 {
		return err
	}
	v, _, err := hubproto.DecodeCBOR(b[4:])
	if err != nil {
		return err
	}
	n.version, n.config = binary.LittleEndian.Uint32(b), v
	return nil
}

// PublicKey returns the node's public key in upper-case hex, as the hub's
// web UI shows it.
func (n *Node) PublicKey() string { return n.mesh.SelfID.String() }

// Paired reports whether the hub has accepted this node.
func (n *Node) Paired() bool { return n.st.paired }

// Config returns the node's configuration, as last received from the hub.
// It is null until the first config arrives.
func (n *Node) Config() hubproto.Value { return n.config }

// ConfigVersion returns the version of Config, 0 if there is none.
func (n *Node) ConfigVersion() uint32 { return n.version }

// SleepHint returns how long the hub suggests sleeping before the next
// check-in, or 0 if it left that to the node.
func (n *Node) SleepHint() time.Duration { return n.sleep }

// SetWakeReason and SetBattery set what the next check-in reports.
func (n *Node) SetWakeReason(r uint8)     { n.wake = r }
func (n *Node) SetBattery(millivolts int) { n.batteryMV = uint16(millivolts) }

// Sync pairs the node if needed, checks in with the hub, fetches its config
// if it changed, and delivers pending events. It returns ErrPending while
// pairing awaits approval.
func (n *Node) Sync(ctx context.Context) error {
	if !n.st.paired {
		if err := n.join(ctx); err != nil {
			return err
		}
	}
	for attempt := 0; ; attempt++ {
		err := n.hello(ctx)
		if errors.Is(err, errConfigChanged) && attempt < 2 {
			continue // the config changed mid-transfer: start over
		}
		if err != nil {
			return err
		}
		break
	}
	return n.Flush(ctx)
}

func (n *Node) join(ctx context.Context) error {
	reply, err := n.request(ctx, &hubproto.Join{Program: n.cfg.Program, Firmware: n.cfg.Firmware, Claim: n.cfg.Claim}, true)
	if err != nil {
		return err
	}
	r, ok := reply.(*hubproto.JoinReply)
	if !ok {
		return ErrNoReply
	}
	switch r.Status {
	case hubproto.JoinAccepted:
		n.st.paired = true
		return n.saveState()
	case hubproto.JoinPending:
		return ErrPending
	default:
		return ErrRejected
	}
}

var errConfigChanged = errors.New("node: config changed during transfer")

func (n *Node) hello(ctx context.Context) error {
	reply, err := n.request(ctx, &hubproto.Hello{
		ConfigVersion: n.version,
		WakeReason:    n.wake,
		BatteryMV:     n.batteryMV,
		UptimeS:       uint32(time.Since(n.start) / time.Second),
		Firmware:      n.cfg.Firmware,
	}, false)
	if err != nil {
		return err
	}
	switch r := reply.(type) {
	case *hubproto.HelloReply:
		n.cfg.RTC.SetCurrentTime(r.Time)
		n.sleep = time.Duration(r.SleepS) * time.Second
		if err := n.setPath(r.PathLen, r.Path); err != nil {
			return err
		}
		if r.ConfigVersion != n.version {
			return n.fetchConfig(ctx, r)
		}
		return nil
	case *hubproto.Error:
		if r.Code == hubproto.ErrNotPaired {
			n.st.paired = false
			n.saveState()
		}
		return ErrNoReply
	}
	return ErrNoReply
}

func (n *Node) setPath(pathLen uint8, path []byte) error {
	if n.st.hasPath && n.st.pathLen == pathLen && string(n.st.path) == string(path) {
		return nil
	}
	n.st.hasPath, n.st.pathLen, n.st.path = true, pathLen, append([]byte(nil), path...)
	return n.saveState()
}

func (n *Node) fetchConfig(ctx context.Context, r *hubproto.HelloReply) error {
	buf := make([]byte, 0, r.ConfigLen)
	for len(buf) < int(r.ConfigLen) {
		reply, err := n.request(ctx, &hubproto.ConfigGet{ConfigVersion: r.ConfigVersion, Offset: uint16(len(buf))}, false)
		if err != nil {
			return err
		}
		switch c := reply.(type) {
		case *hubproto.ConfigChunk:
			if c.ConfigVersion != r.ConfigVersion || int(c.Offset) != len(buf) || len(c.Data) == 0 {
				return ErrConfig
			}
			buf = append(buf, c.Data...)
		case *hubproto.Error:
			if c.Code == hubproto.ErrConfigChanged {
				return errConfigChanged
			}
			return ErrConfig
		default:
			return ErrConfig
		}
	}
	if len(buf) != int(r.ConfigLen) || hubproto.ConfigCRC(buf) != r.ConfigCRC {
		return ErrConfig
	}
	v := hubproto.Null()
	if len(buf) > 0 {
		var err error
		if v, _, err = hubproto.DecodeCBOR(buf); err != nil {
			return ErrConfig
		}
	}
	stored := binary.LittleEndian.AppendUint32(make([]byte, 0, 4+len(buf)), r.ConfigVersion)
	if err := n.cfg.Storage.Save(keyConfig, append(stored, buf...)); err != nil {
		return err
	}
	n.version, n.config = r.ConfigVersion, v
	return nil
}

// Report queues an event for the hub. It is stored until the hub
// acknowledges it; Flush (or Sync) sends it.
func (n *Node) Report(kind string, data hubproto.Value) error {
	e := pendingEvent{seq: n.st.nextEvent, kind: kind, data: data}
	if _, err := hubproto.Encode(nil, 0, &hubproto.Event{EventSeq: e.seq, Kind: kind, Data: data}, hubproto.MaxPlaintext); err != nil {
		return err // too large for a datagram
	}
	n.st.nextEvent++
	n.events = append(n.events, e)
	if len(n.events) > n.cfg.MaxEvents {
		n.events = n.events[len(n.events)-n.cfg.MaxEvents:]
	}
	if err := n.saveState(); err != nil {
		return err
	}
	return n.cfg.Storage.Save(keyEvents, encodeEvents(n.events))
}

// Pending returns the number of events not yet acknowledged.
func (n *Node) Pending() int { return len(n.events) }

// Flush sends queued events, oldest first, until all are acknowledged or one
// fails.
func (n *Node) Flush(ctx context.Context) error {
	if !n.st.paired {
		return ErrPending
	}
	for len(n.events) > 0 {
		e := n.events[0]
		reply, err := n.request(ctx, &hubproto.Event{EventSeq: e.seq, Kind: e.kind, Data: e.data}, false)
		if err != nil {
			return err
		}
		if ack, ok := reply.(*hubproto.EventAck); !ok || ack.EventSeq != e.seq {
			return ErrNoReply
		}
		n.events = n.events[1:]
		if err := n.cfg.Storage.Save(keyEvents, encodeEvents(n.events)); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) saveState() error { return n.cfg.Storage.Save(keyState, encodeState(&n.st)) }

// nextSeq returns a fresh request number, reserving a block in storage when
// the current one runs out.
func (n *Node) nextSeq() (uint32, error) {
	if n.seq >= n.st.seqLimit {
		n.st.seqLimit = n.seq + seqReserve
		if err := n.saveState(); err != nil {
			return 0, err
		}
	}
	s := n.seq
	n.seq++
	return s, nil
}

// request sends m to the hub and waits for the reply. It goes direct when
// the node knows its path to the hub, falling back to a flood if that gets
// no reply.
func (n *Node) request(ctx context.Context, m hubproto.Message, anon bool) (hubproto.Message, error) {
	for attempt := 0; attempt < 2; attempt++ {
		direct := !anon && n.st.hasPath
		reply, err := n.send(ctx, m, anon, direct)
		if err == nil {
			return reply, nil
		}
		if !errors.Is(err, ErrNoReply) || !direct {
			return nil, err
		}
		// The route to the hub is stale: forget it and flood.
		n.st.hasPath = false
		if err := n.saveState(); err != nil {
			return nil, err
		}
	}
	return nil, ErrNoReply
}

func (n *Node) send(ctx context.Context, m hubproto.Message, anon, direct bool) (hubproto.Message, error) {
	seq, err := n.nextSeq()
	if err != nil {
		return nil, err
	}
	max := hubproto.MaxPlaintext
	if anon {
		max = hubproto.MaxAnonPlaintext
	}
	pt, err := hubproto.Encode(nil, seq, m, max)
	if err != nil {
		return nil, err
	}

	var pkt *meshcore.Packet
	if anon {
		pkt = n.mesh.CreateAnonDatagram(meshcore.PayloadTypeAnonReq, &n.mesh.SelfID, &n.cfg.Hub, n.secret[:], pt)
	} else {
		pkt = n.mesh.CreateDatagram(meshcore.PayloadTypeReq, &n.cfg.Hub, n.secret[:], pt)
	}
	if pkt == nil {
		return nil, errors.New("node: no packet buffer")
	}
	if direct {
		n.mesh.SendDirect(pkt, n.st.path, n.st.pathLen, 0)
	} else {
		n.mesh.SendFlood(pkt, 0, 1)
	}

	n.h.await(seq)
	deadline := time.Now().Add(n.cfg.ReplyTimeout)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n.mesh.Loop()
		if reply := n.h.take(); reply != nil {
			return reply, nil
		}
		time.Sleep(time.Millisecond)
	}
	return nil, ErrNoReply
}

// handler receives the hub's responses from the mesh.
type handler struct {
	meshcore.BaseHandler
	n       *Node
	waitSeq uint32
	waiting bool
	reply   hubproto.Message
}

func (h *handler) await(seq uint32) { h.waitSeq, h.waiting, h.reply = seq, true, nil }

func (h *handler) take() hubproto.Message {
	r := h.reply
	if r != nil {
		h.waiting, h.reply = false, nil
	}
	return r
}

// CalcRxDelay is zero: the node does not repeat, so it has no reason to
// hold packets back.
func (h *handler) CalcRxDelay(float32, uint32) int { return 0 }

func (h *handler) SearchPeersByHash(hash []byte) int {
	if h.n.cfg.Hub.IsHashMatch(hash) {
		return 1
	}
	return 0
}

func (h *handler) PeerSharedSecret(dst *[meshcore.PubKeySize]byte, _ int) { *dst = h.n.secret }

func (h *handler) OnPeerDataRecv(p *meshcore.Packet, typ uint8, _ int, _ *[meshcore.PubKeySize]byte, data []byte) {
	if typ != meshcore.PayloadTypeResponse || !h.waiting {
		return
	}
	seq, m, err := hubproto.Decode(data)
	if err != nil || seq != h.waitSeq {
		return // not the reply we are waiting for
	}
	h.reply = m
}
