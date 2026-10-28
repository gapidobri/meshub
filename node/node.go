// Package node is the device side of meshub: it pairs a MeshCore node with
// the hub, keeps its config in sync, delivers its events and runs the
// hub's commands. It wraps meshcore-go and is hardware independent; the
// application supplies the radio, clocks and storage.
//
// A typical wake cycle:
//
//	n, err := node.New(cfg)
//	err = n.Sync(ctx)                  // pair, check in, fetch new config, run commands, send events
//	threshold := n.Config().Get("audio.threshold").Int(40)
//	n.Report("flow", data)             // queued until the hub acknowledges it
//	err = n.Flush(ctx)
//	sleep(n.Config().Get("interval_s").Int(600))  // the program decides when to check in next
//
// The node only runs the mesh while it waits for the hub, and never
// repeats other nodes' packets. It leaves the radio receiving when Sync or
// Flush returns; a battery node puts the radio to sleep until the next one
// (and Begins it again first, if sleeping loses its configuration).
package node

import (
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/proto/hubproto"
)

// Config configures a Node. For the durations and Retries, zero means the
// default and a negative value means none.
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

	// ReplyTimeout is the longest wait for a reply (default 8s). The node
	// learns how quickly the hub answers over its route and waits less,
	// down to MinReplyTimeout (default 2s).
	ReplyTimeout    time.Duration
	MinReplyTimeout time.Duration
	// Retries is how often a request is resent when no reply comes
	// (default 2), each after a random backoff of up to RetryBackoff
	// (default 1s), doubled with every retry. Falling back from a stale
	// direct route to a flood is not counted.
	Retries      int
	RetryBackoff time.Duration
	// StartDelay is the longest random delay before Sync first transmits
	// (default 2s), so nodes waking together, after a power cut say, do not
	// all transmit at once.
	StartDelay time.Duration

	// MaxEvents bounds the queue of unacknowledged events (default 16), and
	// MaxEventBytes its stored size (default 4096); the oldest are dropped
	// beyond either. Storage must hold this, a command result of up to
	// hubproto.MaxMessage and the config at once: a flashstore with two
	// 4 KiB blocks per region does, for a config of up to 1 KB or so.
	MaxEvents     int
	MaxEventBytes int

	// Validate, if set, checks a new config before it is applied. A config
	// it returns an error for is not applied, and the hub shows the error.
	Validate func(config hubproto.Value) error
	// Command, if set, runs the hub's commands other than the built-in
	// "logs" and "reset". It returns a result for the hub to show, or an
	// error; ErrUnknownCommand marks a command it does not know.
	Command func(name string, args hubproto.Value) (hubproto.Value, error)

	// LogSize is the size of the log buffer the "logs" command returns, in
	// bytes (default 1024, at most MaxLogSize).
	LogSize int
}

// Errors returned by Sync.
var (
	ErrPending  = errors.New("node: waiting for the hub operator to approve pairing")
	ErrRejected = errors.New("node: the hub rejected pairing")
	ErrNoReply  = errors.New("node: no reply from the hub")
	ErrConfig   = errors.New("node: config transfer failed")
)

// ErrTooLarge is returned by Report for an event too large to send.
var ErrTooLarge = errors.New("node: event too large")

// ErrUnknownCommand is returned by a Config.Command handler for a command
// it does not know.
var ErrUnknownCommand = errors.New("node: unknown command")

const (
	poolSize = 8
	// maxCommands bounds the commands run per Sync.
	maxCommands = 8
	// maxReason bounds the config rejection reason sent to the hub.
	maxReason = 120
)

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
	cs      configStatus
	result  *hubproto.CommandResult // not yet acknowledged
	pending int                     // commands the hub has waiting

	floodRTT, directRTT rttEstimator
	clockSet            bool // the hub set the RTC since boot
	logs                logBuffer

	wake      uint8
	batteryMV uint16
	start     time.Time
}

func defaultDuration(d *time.Duration, def time.Duration) {
	if *d == 0 {
		*d = def
	} else if *d < 0 {
		*d = 0
	}
}

// New creates a node, loading its state from cfg.Storage and creating an
// identity on first use. Stored state that is corrupt is discarded, and the
// node pairs again. It starts the radio.
func New(cfg Config) (*Node, error) {
	defaultDuration(&cfg.ReplyTimeout, 8*time.Second)
	defaultDuration(&cfg.MinReplyTimeout, 2*time.Second)
	defaultDuration(&cfg.RetryBackoff, time.Second)
	defaultDuration(&cfg.StartDelay, 2*time.Second)
	if cfg.Retries == 0 {
		cfg.Retries = 2
	} else if cfg.Retries < 0 {
		cfg.Retries = 0
	}
	if cfg.MaxEvents <= 0 {
		cfg.MaxEvents = 16
	}
	if cfg.MaxEventBytes <= 0 {
		cfg.MaxEventBytes = 4096
	}
	if cfg.LogSize == 0 {
		cfg.LogSize = 1024
	}
	cfg.LogSize = min(cfg.LogSize, MaxLogSize)
	n := &Node{cfg: cfg, start: time.Now(), logs: logBuffer{size: cfg.LogSize}}

	id, err := n.loadIdentity()
	if err != nil {
		return nil, err
	}
	if err := n.loadState(); err != nil {
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

// loadState loads everything but the identity. Only storage errors are
// returned; a corrupt value is logged and replaced by its default.
func (n *Node) loadState() error {
	load := func(key string, decode func([]byte) error) error {
		b, err := n.cfg.Storage.Load(key)
		if err != nil || len(b) == 0 {
			return err
		}
		if err := decode(b); err != nil {
			n.Log("stored " + key + " is corrupt, discarded")
		}
		return nil
	}
	fresh := true
	err := load(keyState, func(b []byte) (err error) {
		n.st, err = decodeState(b)
		fresh = err != nil
		return err
	})
	if err != nil {
		return err
	}
	// Numbers below the limit may have been used before a reboot. Without
	// a stored state the node joins, which resets the hub's count, from a
	// random number: restarting at 0 would repeat the packets of an earlier
	// life, which the hub drops as duplicates.
	n.seq = n.st.seqLimit
	if fresh {
		var b [4]byte
		n.cfg.RNG.Random(b[:])
		n.seq = binary.LittleEndian.Uint32(b[:]) >> 1
	}
	err = load(keyConfig, func(b []byte) error {
		if len(b) < 4 {
			return errBadState
		}
		v, _, err := hubproto.DecodeCBOR(b[4:])
		if err != nil {
			return err
		}
		n.version, n.config = binary.LittleEndian.Uint32(b), v
		return nil
	})
	if err != nil {
		return err
	}
	err = load(keyEvents, func(b []byte) (err error) {
		n.events, err = decodeEvents(b)
		return err
	})
	if err != nil {
		return err
	}
	err = load(keyConfigStatus, func(b []byte) (err error) {
		n.cs, err = decodeConfigStatus(b)
		return err
	})
	if err != nil {
		return err
	}
	return load(keyResult, func(b []byte) (err error) {
		n.result, err = decodeResult(b)
		return err
	})
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

// SetWakeReason and SetBattery set what the next check-in reports.
func (n *Node) SetWakeReason(r uint8)     { n.wake = r }
func (n *Node) SetBattery(millivolts int) { n.batteryMV = uint16(millivolts) }

// Sync pairs the node if needed, checks in with the hub, fetches its config
// if it changed, runs the hub's commands and delivers pending events. It
// returns ErrPending while pairing awaits approval.
func (n *Node) Sync(ctx context.Context) error {
	if err := wait(ctx, n.randDuration(n.cfg.StartDelay)); err != nil {
		return err
	}
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
	if err := n.runCommands(ctx); err != nil {
		return err
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
		if !n.clockSet {
			n.clockSet = true
			if err := n.stampEvents(); err != nil {
				return err
			}
		}
		n.pending = int(r.Commands)
		if err := n.setPath(r.PathLen, r.Path); err != nil {
			return err
		}
		rejected := n.cs.rejected && n.cs.version == r.ConfigVersion
		if r.ConfigVersion != n.version && !rejected {
			if err := n.fetchConfig(ctx, r); err != nil {
				return err
			}
		}
		return n.reportConfig(ctx)
	case *hubproto.Error:
		n.notPaired(r)
	}
	return ErrNoReply
}

// notPaired forgets the pairing if the hub says it does not know the node,
// so the next Sync joins again.
func (n *Node) notPaired(e *hubproto.Error) {
	if e.Code == hubproto.ErrNotPaired && n.st.paired {
		n.st.paired = false
		n.saveState()
		n.Log("the hub no longer knows this node: pairing again")
	}
}

func (n *Node) setPath(pathLen uint8, path []byte) error {
	if n.st.hasPath && n.st.pathLen == pathLen && string(n.st.path) == string(path) {
		return nil
	}
	n.st.hasPath, n.st.pathLen, n.st.path = true, pathLen, append([]byte(nil), path...)
	n.directRTT = rttEstimator{} // a new route answers in its own time
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
	if n.cfg.Validate != nil {
		if err := n.cfg.Validate(v); err != nil {
			reason := err.Error()
			if len(reason) > maxReason {
				reason = reason[:maxReason]
			}
			n.Log("config v" + strconv.Itoa(int(r.ConfigVersion)) + " rejected: " + reason)
			return n.setConfigStatus(configStatus{version: r.ConfigVersion, rejected: true, reason: reason})
		}
	}
	stored := binary.LittleEndian.AppendUint32(make([]byte, 0, 4+len(buf)), r.ConfigVersion)
	if err := n.cfg.Storage.Save(keyConfig, append(stored, buf...)); err != nil {
		return err
	}
	n.version, n.config = r.ConfigVersion, v
	n.Log("config v" + strconv.Itoa(int(r.ConfigVersion)) + " applied")
	return n.setConfigStatus(configStatus{version: r.ConfigVersion})
}

func (n *Node) setConfigStatus(cs configStatus) error {
	n.cs = cs
	return n.cfg.Storage.Save(keyConfigStatus, encodeConfigStatus(&n.cs))
}

// reportConfig tells the hub whether the last config was applied, unless
// it already knows.
func (n *Node) reportConfig(ctx context.Context) error {
	if n.cs.reported || n.cs.version == 0 {
		return nil
	}
	status := uint8(hubproto.ConfigApplied)
	if n.cs.rejected {
		status = hubproto.ConfigRejected
	}
	reply, err := n.request(ctx, &hubproto.ConfigStatus{ConfigVersion: n.cs.version, Status: status, Reason: n.cs.reason}, false)
	if err != nil {
		return err
	}
	if _, ok := reply.(*hubproto.Ack); !ok {
		return ErrNoReply
	}
	n.cs.reported = true
	return n.setConfigStatus(n.cs)
}

// Report queues an event for the hub. It is stored until the hub
// acknowledges it; Flush (or Sync) sends it. Events are stamped with the
// time from the node's clock, which the hub sets at every check-in; one
// reported before the first check-in since boot is stamped when it
// happens.
func (n *Node) Report(kind string, data hubproto.Value) error {
	e := pendingEvent{seq: n.st.nextEvent, kind: kind, data: data}
	if n.clockSet {
		e.time = n.cfg.RTC.CurrentTime()
	} else {
		e.reportedAt, e.unstamped = n.cfg.Millis.Millis(), true
	}
	body, err := hubproto.AppendMessage(nil, &hubproto.Events{Events: []hubproto.Event{e.wire()}})
	if err != nil {
		return err
	}
	if len(body) > hubproto.MaxMessage || len(encodeEvents([]pendingEvent{e})) > n.cfg.MaxEventBytes {
		return ErrTooLarge
	}
	n.st.nextEvent++
	n.events = append(n.events, e)
	stored := encodeEvents(n.events)
	dropped := 0
	for len(n.events) > n.cfg.MaxEvents || len(stored) > n.cfg.MaxEventBytes {
		n.events = n.events[1:]
		stored = encodeEvents(n.events)
		dropped++
	}
	if dropped > 0 {
		n.Log("event queue full: dropped " + strconv.Itoa(dropped) + " oldest")
	}
	if err := n.saveState(); err != nil {
		return err
	}
	return n.cfg.Storage.Save(keyEvents, stored)
}

// stampEvents gives events reported before the clock was set the time they
// happened, now that it is.
func (n *Node) stampEvents() error {
	now, ms := n.cfg.RTC.CurrentTime(), n.cfg.Millis.Millis()
	changed := false
	for i := range n.events {
		e := &n.events[i]
		if e.unstamped {
			e.time = now - (ms-e.reportedAt)/1000
			e.unstamped, changed = false, true
		}
	}
	if !changed {
		return nil
	}
	return n.cfg.Storage.Save(keyEvents, encodeEvents(n.events))
}

// Pending returns the number of events not yet acknowledged.
func (n *Node) Pending() int { return len(n.events) }

// Flush sends queued events, oldest first and as many per message as fit,
// until all are acknowledged or one fails.
func (n *Node) Flush(ctx context.Context) error {
	if !n.st.paired {
		return ErrPending
	}
	for len(n.events) > 0 {
		batch := n.batch()
		reply, err := n.request(ctx, batch, false)
		if err != nil {
			return err
		}
		if _, ok := reply.(*hubproto.Ack); !ok {
			if e, ok := reply.(*hubproto.Error); ok {
				n.notPaired(e)
			}
			return ErrNoReply
		}
		n.events = n.events[len(batch.Events):]
		if err := n.cfg.Storage.Save(keyEvents, encodeEvents(n.events)); err != nil {
			return err
		}
	}
	return nil
}

// batch returns the longest run of queued events, from the oldest, that
// fits one datagram, or the oldest alone if it does not fit by itself (it
// is then sent in fragments).
func (n *Node) batch() *hubproto.Events {
	b := &hubproto.Events{Events: []hubproto.Event{n.events[0].wire()}}
	for _, e := range n.events[1:min(len(n.events), 255)] {
		b.Events = append(b.Events, e.wire())
		if _, err := hubproto.Encode(nil, 0, b, hubproto.MaxPlaintext); err != nil {
			b.Events = b.Events[:len(b.Events)-1]
			break
		}
	}
	return b
}

// runCommands sends the result of a command run before, then fetches and
// runs the commands the hub has waiting.
func (n *Node) runCommands(ctx context.Context) error {
	if err := n.sendResult(ctx); err != nil {
		return err
	}
	for i := 0; i < min(n.pending, maxCommands); i++ {
		reply, err := n.request(ctx, &hubproto.CommandGet{}, false)
		if err != nil {
			return err
		}
		c, ok := reply.(*hubproto.Command)
		if !ok {
			return ErrNoReply
		}
		if c.ID == 0 {
			break // cancelled since the hello
		}
		// The result is stored before it is sent, so a command runs once
		// even if the reply is lost.
		n.result = n.runCommand(c)
		if err := n.cfg.Storage.Save(keyResult, encodeResult(n.result)); err != nil {
			return err
		}
		if err := n.sendResult(ctx); err != nil {
			return err
		}
	}
	n.pending = 0
	return nil
}

func (n *Node) sendResult(ctx context.Context) error {
	if n.result == nil {
		return nil
	}
	reply, err := n.request(ctx, n.result, false)
	if err != nil {
		return err
	}
	if _, ok := reply.(*hubproto.Ack); !ok {
		return ErrNoReply
	}
	n.result = nil
	return n.cfg.Storage.Save(keyResult, nil)
}

func (n *Node) runCommand(c *hubproto.Command) *hubproto.CommandResult {
	n.Log("command " + c.Name)
	r := &hubproto.CommandResult{ID: c.ID, Status: hubproto.CommandOK}
	var err error
	switch {
	case c.Name == "logs":
		r.Result = hubproto.String(n.logs.String())
		if c.Args.Get("clear").Bool(false) {
			n.logs.clear()
		}
	case c.Name == "reset":
		err = n.reset()
	case n.cfg.Command != nil:
		r.Result, err = n.cfg.Command(c.Name, c.Args)
	default:
		err = ErrUnknownCommand
	}
	switch {
	case errors.Is(err, ErrUnknownCommand):
		r.Status, r.Result = hubproto.CommandUnknown, hubproto.Null()
	case err != nil:
		r.Status, r.Result = hubproto.CommandFailed, hubproto.String(err.Error())
	default:
		// A result too large to send is replaced by an error.
		if body, _ := hubproto.AppendMessage(nil, r); len(body) > hubproto.MaxMessage {
			r.Status, r.Result = hubproto.CommandFailed, hubproto.String("result too large")
		}
	}
	return r
}

// reset implements the "reset" command: it drops queued events, the config
// and the route to the hub, keeping the identity and pairing. The node
// fetches its config again at the next check-in.
func (n *Node) reset() error {
	n.events = nil
	n.version, n.config = 0, hubproto.Null()
	n.cs = configStatus{}
	n.st.hasPath = false
	for _, key := range []string{keyEvents, keyConfig, keyConfigStatus} {
		if err := n.cfg.Storage.Save(key, nil); err != nil {
			return err
		}
	}
	return n.saveState()
}

func (n *Node) saveState() error { return n.cfg.Storage.Save(keyState, encodeState(&n.st)) }
