package hub

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/proto/hubproto"
	"github.com/gapidobri/meshub/server/internal/store"
)

// collectWindow is how long the hub waits after first hearing a request for
// other gateways to report it, before choosing which one replies.
var collectWindow = 500 * time.Millisecond

// receiver decodes requests to the hub with a meshcore.Mesh that never
// transmits: received packets are fed through a virtual radio, and MeshCore
// checks, decrypts and deduplicates them. It also builds the replies.
type receiver struct {
	meshcore.BaseHandler
	hub   *Hub
	mesh  *meshcore.Mesh
	radio feedRadio

	mu sync.Mutex // guards the mesh and the fields below during a Receive

	ctx        context.Context
	candidates []store.Device
	secrets    map[string][meshcore.PubKeySize]byte
	got        *request
}

// request is a decoded request from a device.
type request struct {
	device  string // public key
	id      meshcore.Identity
	secret  [meshcore.PubKeySize]byte
	seq     uint32
	msg     hubproto.Message
	anon    bool
	direct  bool
	hash    string
	gateway string // first gateway that reported it
}

// feedRadio is a meshcore.Radio that receives one injected frame and sends
// nothing.
type feedRadio struct {
	meshcore.RadioBase
	frame []byte
}

func (r *feedRadio) RecvRaw(buf []byte) int {
	n := copy(buf, r.frame)
	r.frame = nil
	return n
}
func (*feedRadio) EstAirtimeFor(int) uint32         { return 0 }
func (*feedRadio) PacketScore(float32, int) float32 { return 1 }
func (*feedRadio) StartSendRaw([]byte) bool         { return true }
func (*feedRadio) IsSendComplete() bool             { return true }
func (*feedRadio) OnSendFinished()                  {}
func (*feedRadio) IsInRecvMode() bool               { return true }

type cryptoRNG struct{}

func (cryptoRNG) Random(dst []byte) { rand.Read(dst) }

type wallClock struct{}

func (wallClock) CurrentTime() uint32   { return uint32(time.Now().Unix()) }
func (wallClock) SetCurrentTime(uint32) {}
func (wallClock) Tick()                 {}

func newReceiver(h *Hub) *receiver {
	r := &receiver{hub: h, secrets: map[string][meshcore.PubKeySize]byte{}}
	r.mesh = meshcore.NewMesh(meshcore.Config{
		Radio:   &r.radio,
		Millis:  meshcore.NewSystemMillis(),
		RNG:     cryptoRNG{},
		RTC:     wallClock{},
		Packets: meshcore.NewStaticPoolPacketManager(8),
		Tables:  &meshcore.SimpleMeshTables{},
		Handler: r,
	})
	r.mesh.SelfID = h.id
	r.mesh.Begin()
	return r
}

// IsForHub reports whether a packet is a request addressed to the hub, by
// its payload type and destination hash.
func (h *Hub) IsForHub(payloadType uint8, payload []byte) bool {
	if payloadType != meshcore.PayloadTypeReq && payloadType != meshcore.PayloadTypeAnonReq {
		return false
	}
	return len(payload) > 0 && h.id.IsHashMatch(payload[:meshcore.PathHashSize])
}

// Receive handles a request packet a gateway heard. Copies of the same
// packet from other gateways are recognised and dropped; the hub replies
// once, through the best of them.
func (h *Hub) Receive(ctx context.Context, raw []byte, hash, gateway string) {
	r := h.rx
	r.mu.Lock()
	r.ctx, r.got = ctx, nil
	r.radio.frame = raw
	r.mesh.Loop()
	req := r.got
	r.got = nil
	r.mu.Unlock()
	if req == nil {
		return
	}
	req.hash, req.gateway = hash, gateway
	if !h.accept() {
		h.log.Debug("device request dropped: shutting down", "device", short(req.device))
		return
	}
	time.AfterFunc(collectWindow, func() {
		defer h.answered()
		h.answer(ctx, req)
	})
}

// CalcRxDelay is zero so floods are decoded as they are injected.
func (r *receiver) CalcRxDelay(float32, uint32) int { return 0 }

func (r *receiver) SearchPeersByHash(hash []byte) int {
	devs, err := r.hub.st.DevicesByHash(r.ctx, meshcore.ToHex(hash))
	if err != nil {
		r.hub.log.Warn("device lookup", "err", err)
		return 0
	}
	r.candidates = devs
	return len(devs)
}

func (r *receiver) PeerSharedSecret(dst *[meshcore.PubKeySize]byte, i int) {
	pk := r.candidates[i].PubKey
	if s, ok := r.secrets[pk]; ok {
		*dst = s
		return
	}
	id, ok := meshcore.IdentityFromHex(pk)
	if !ok {
		return
	}
	r.hub.id.SharedSecret(dst, &id.PubKey)
	r.secrets[pk] = *dst
}

func (r *receiver) OnPeerDataRecv(p *meshcore.Packet, typ uint8, i int, secret *[meshcore.PubKeySize]byte, data []byte) {
	if typ != meshcore.PayloadTypeReq {
		return
	}
	dev := r.candidates[i]
	id, _ := meshcore.IdentityFromHex(dev.PubKey)
	r.decode(p, id, secret, data, false)
}

func (r *receiver) OnAnonDataRecv(p *meshcore.Packet, secret *[meshcore.PubKeySize]byte, sender *meshcore.Identity, data []byte) {
	r.decode(p, *sender, secret, data, true)
}

func (r *receiver) decode(p *meshcore.Packet, id meshcore.Identity, secret *[meshcore.PubKeySize]byte, data []byte, anon bool) {
	seq, msg, err := hubproto.Decode(data)
	if err != nil {
		r.hub.log.Warn("device request", "device", short(id.String()), "err", err)
		return
	}
	if _, isJoin := msg.(*hubproto.Join); isJoin != anon {
		return // joins come anonymous, everything else from a known device
	}
	r.got = &request{device: id.String(), id: id, secret: *secret, seq: seq, msg: msg, anon: anon, direct: p.IsRouteDirect()}
}

// answer handles a request and sends the reply.
func (h *Hub) answer(ctx context.Context, req *request) {
	log := h.log.With("device", short(req.device), "seq", req.seq)
	route, err := h.replyRoute(ctx, req)
	if err != nil {
		log.Warn("device route", "err", err)
		return
	}
	reply, err := h.handle(ctx, req, route)
	if err != nil {
		log.Warn("device request", "err", err)
		return
	}
	if reply == nil {
		return
	}
	if err := h.reply(req, route, reply); err != nil {
		log.Warn("device reply", "err", err)
		return
	}
	log.Info("device request", "req", typeName(req.msg.Type()), "reply", typeName(reply.Type()),
		"gateway", short(route.Gateway), "hops", int(route.PathLen&63), "direct", req.direct)
}

// replyRoute picks the gateway and path to reply through. A flood request
// is answered along the path of its best reception, which becomes the
// device's stored route; a direct request, whose path is used up on
// arrival, along the stored route.
func (h *Hub) replyRoute(ctx context.Context, req *request) (store.Route, error) {
	if !req.direct {
		best, ok, err := h.st.BestReception(ctx, req.hash)
		if err != nil {
			return store.Route{}, err
		}
		if ok {
			r := store.Route{Gateway: best.Gateway, PathLen: best.PathLen, Path: best.Path}
			if !req.anon {
				err = h.st.SetRoute(ctx, req.device, r, h.now())
			}
			return r, err
		}
	} else if r, ok, err := h.st.Route(ctx, req.device); err != nil || ok {
		return r, err
	}
	// No route known: answer through the gateway that heard it, zero hop.
	return store.Route{Gateway: req.gateway}, nil
}

// handle applies a request and returns the reply, or nil to send none.
func (h *Hub) handle(ctx context.Context, req *request, route store.Route) (hubproto.Message, error) {
	if j, ok := req.msg.(*hubproto.Join); ok {
		return h.handleJoin(ctx, req, j)
	}
	dev, ok, err := h.st.Device(ctx, req.device)
	if err != nil || !ok {
		return nil, err
	}
	if dev.Status != store.DevicePaired {
		return &hubproto.Error{Code: hubproto.ErrNotPaired}, nil
	}
	if fresh, err := h.st.AcceptSeq(ctx, req.device, req.seq); err != nil || !fresh {
		return nil, err // a replay, or a copy already answered
	}
	if f, ok := req.msg.(*hubproto.Fragment); ok {
		m, done, err := h.reassemble(req.device, f)
		if err != nil {
			h.log.Warn("device fragment", "device", short(req.device), "err", err)
			return &hubproto.Error{Code: hubproto.ErrBadRequest}, nil
		}
		if !done {
			return &hubproto.Ack{}, nil
		}
		switch m.(type) {
		case *hubproto.Events, *hubproto.CommandResult:
			return h.handleMessage(ctx, req, route, m)
		}
		return &hubproto.Error{Code: hubproto.ErrBadRequest}, nil
	}
	return h.handleMessage(ctx, req, route, req.msg)
}

// handleMessage applies a paired device's request, or the message its
// fragments carried.
func (h *Hub) handleMessage(ctx context.Context, req *request, route store.Route, msg hubproto.Message) (hubproto.Message, error) {
	switch m := msg.(type) {
	case *hubproto.Hello:
		err := h.st.RecordHello(ctx, req.device, store.Hello{
			ConfigVersion: m.ConfigVersion, WakeReason: m.WakeReason, BatteryMV: m.BatteryMV,
			UptimeS: m.UptimeS, Firmware: m.Firmware,
		}, h.now())
		if err != nil {
			return nil, err
		}
		h.Notify.DeviceChanged(req.device)
		cfg, err := h.st.Config(ctx, req.device)
		if err != nil {
			return nil, err
		}
		commands, err := h.st.PendingCommands(ctx, req.device)
		if err != nil {
			return nil, err
		}
		return &hubproto.HelloReply{
			Time:          uint32(h.now().Unix()),
			PathLen:       route.PathLen,
			Path:          route.Path,
			ConfigVersion: cfg.Version,
			ConfigLen:     uint16(len(cfg.CBOR)),
			ConfigCRC:     hubproto.ConfigCRC(cfg.CBOR),
			Commands:      uint8(min(commands, 255)),
		}, nil

	case *hubproto.ConfigGet:
		cfg, err := h.st.Config(ctx, req.device)
		if err != nil {
			return nil, err
		}
		if m.ConfigVersion != cfg.Version {
			return &hubproto.Error{Code: hubproto.ErrConfigChanged}, nil
		}
		if int(m.Offset) >= len(cfg.CBOR) {
			return &hubproto.Error{Code: hubproto.ErrBadRequest}, nil
		}
		end := min(int(m.Offset)+hubproto.ChunkSize, len(cfg.CBOR))
		return &hubproto.ConfigChunk{ConfigVersion: cfg.Version, Offset: m.Offset, Data: cfg.CBOR[m.Offset:end]}, nil

	case *hubproto.ConfigStatus:
		rejected := m.Status == hubproto.ConfigRejected
		if err := h.st.RecordConfigStatus(ctx, req.device, m.ConfigVersion, rejected, m.Reason); err != nil {
			return nil, err
		}
		h.Notify.DeviceChanged(req.device)
		if rejected {
			h.log.Warn("device rejected config", "device", short(req.device), "version", m.ConfigVersion, "reason", m.Reason)
		}
		return &hubproto.Ack{}, nil

	case *hubproto.Events:
		for _, e := range m.Events {
			data, _ := json.Marshal(e.Data.Any())
			ev := store.Event{Seq: e.Seq, Kind: e.Kind, Data: string(data), ReceivedAt: h.now()}
			if e.Time != 0 {
				ev.DeviceTime = time.Unix(int64(e.Time), 0)
			}
			if err := h.st.AddEvent(ctx, req.device, ev); err != nil {
				return nil, err
			}
			h.log.Info("device event", "device", short(req.device), "kind", e.Kind, "data", string(data))
		}
		h.Notify.EventsAdded()
		return &hubproto.Ack{}, nil

	case *hubproto.CommandGet:
		c, ok, err := h.st.NextCommand(ctx, req.device)
		if err != nil || !ok {
			return &hubproto.Command{Args: hubproto.Null()}, err
		}
		args, _, err := hubproto.DecodeCBOR(c.ArgsCBOR)
		if err != nil {
			return nil, err
		}
		return &hubproto.Command{ID: c.ID, Name: c.Name, Args: args}, nil

	case *hubproto.CommandResult:
		status := store.CommandOK
		switch m.Status {
		case hubproto.CommandFailed:
			status = store.CommandFailed
		case hubproto.CommandUnknown:
			status = store.CommandUnknown
		}
		result, _ := json.Marshal(m.Result.Any())
		if err := h.st.FinishCommand(ctx, req.device, m.ID, status, string(result), h.now()); err != nil {
			return nil, err
		}
		h.Notify.CommandChanged(req.device, m.ID)
		h.log.Info("device command done", "device", short(req.device), "id", m.ID, "status", status)
		return &hubproto.Ack{}, nil
	}
	return &hubproto.Error{Code: hubproto.ErrBadRequest}, nil
}

// assemblyTTL is how long a partly received large message is kept.
const assemblyTTL = 10 * time.Minute

// assembly is a large message arriving in fragments.
type assembly struct {
	xfer  uint16
	total uint16
	buf   []byte
	at    time.Time
}

var errFragment = errors.New("hub: fragment out of order")

// reassemble adds a fragment to the device's large message, returning the
// message once it is complete. A fragment resent after a lost reply
// replaces what it carried, and a resent last fragment completes the
// message again: the messages that travel in fragments are idempotent.
func (h *Hub) reassemble(device string, f *hubproto.Fragment) (hubproto.Message, bool, error) {
	now := h.now()
	h.asmMu.Lock()
	defer h.asmMu.Unlock()
	for d, a := range h.asm {
		if now.Sub(a.at) > assemblyTTL {
			delete(h.asm, d)
		}
	}
	a := h.asm[device]
	if a == nil || a.xfer != f.Xfer || a.total != f.Total {
		if f.Offset != 0 {
			return nil, false, errFragment
		}
		a = &assembly{xfer: f.Xfer, total: f.Total}
		h.asm[device] = a
	}
	if f.Total > hubproto.MaxMessage || int(f.Offset) > len(a.buf) || int(f.Offset)+len(f.Data) > int(f.Total) {
		return nil, false, errFragment
	}
	a.buf = append(a.buf[:f.Offset], f.Data...)
	a.at = now
	if len(a.buf) < int(a.total) {
		return nil, false, nil
	}
	m, err := hubproto.DecodeMessage(a.buf)
	return m, err == nil, err
}

func (h *Hub) handleJoin(ctx context.Context, req *request, j *hubproto.Join) (hubproto.Message, error) {
	status := store.DevicePending
	if h.AutoPair {
		status = store.DevicePaired
	}
	prev, known, err := h.st.Device(ctx, req.device)
	if err != nil {
		return nil, err
	}
	dev, err := h.st.Join(ctx, req.device, j.Program, j.Firmware, j.Claim, status, req.seq, h.now())
	if err != nil {
		return nil, err
	}
	if known && prev.Program != dev.Program {
		h.Notify.DeviceDeleted(prev) // its status moves to the new program's topic
	}
	h.Notify.DeviceChanged(req.device)
	switch dev.Status {
	case store.DevicePaired:
		return &hubproto.JoinReply{Status: hubproto.JoinAccepted}, nil
	case store.DeviceRejected:
		return &hubproto.JoinReply{Status: hubproto.JoinRejected}, nil
	}
	h.log.Info("device waiting for approval", "device", short(req.device), "program", j.Program)
	return &hubproto.JoinReply{Status: hubproto.JoinPending}, nil
}

var errReplyBuild = errors.New("hub: could not build reply packet")

// reply encrypts m for the device and sends it along route.
func (h *Hub) reply(req *request, route store.Route, m hubproto.Message) error {
	pt, err := hubproto.Encode(nil, req.seq, m, hubproto.MaxPlaintext)
	if err != nil {
		return err
	}
	r := h.rx
	r.mu.Lock()
	pkt := r.mesh.CreateDatagram(meshcore.PayloadTypeResponse, &req.id, req.secret[:], pt)
	var raw []byte
	if pkt != nil {
		pkt.Header = pkt.Header&^meshcore.PHRouteMask | meshcore.RouteTypeDirect
		pkt.PathLen = route.PathLen
		reversePath(pkt.Path[:], route.Path, route.PathLen)
		raw = make([]byte, meshcore.MaxTransUnit)
		raw = raw[:pkt.Encode(raw)]
		r.mesh.ReleasePacket(pkt)
	}
	r.mu.Unlock()
	if raw == nil {
		return errReplyBuild
	}
	return h.transmit(route.Gateway, "reply", raw, 0, 0)
}

// reversePath writes the hop hashes of path in reverse order: a request's
// path lists repeaters from the device outwards, a reply visits them from
// the gateway back.
func reversePath(dst, path []byte, pathLen uint8) {
	size := int(pathLen>>6) + 1
	count := int(pathLen & 63)
	for i := 0; i < count && (i+1)*size <= len(path); i++ {
		copy(dst[(count-1-i)*size:], path[i*size:(i+1)*size])
	}
}

func typeName(t hubproto.Type) string {
	switch t {
	case hubproto.TypeJoin:
		return "join"
	case hubproto.TypeHello:
		return "hello"
	case hubproto.TypeConfigGet:
		return "config-get"
	case hubproto.TypeEvents:
		return "events"
	case hubproto.TypeConfigStatus:
		return "config-status"
	case hubproto.TypeCommandGet:
		return "command-get"
	case hubproto.TypeCommandResult:
		return "command-result"
	case hubproto.TypeFragment:
		return "fragment"
	case hubproto.TypeJoinReply:
		return "join-reply"
	case hubproto.TypeHelloReply:
		return "hello-reply"
	case hubproto.TypeConfigChunk:
		return "config-chunk"
	case hubproto.TypeAck:
		return "ack"
	case hubproto.TypeCommand:
		return "command"
	case hubproto.TypeError:
		return "error"
	}
	return "unknown"
}

// SetConfig validates a JSON config and stores it as the device's next
// config version, which the device fetches on its next check-in.
func (h *Hub) SetConfig(ctx context.Context, device string, jsonConfig []byte) (uint32, error) {
	var x any
	if err := json.Unmarshal(jsonConfig, &x); err != nil {
		return 0, invalid(err)
	}
	v, ok := hubproto.FromAny(x)
	if !ok || v.Kind() != hubproto.KindMap {
		return 0, invalid(errors.New("config must be a JSON object"))
	}
	cbor := v.AppendCBOR(nil)
	if len(cbor) > 0xFFFF {
		return 0, invalid(errors.New("config too large"))
	}
	if err := h.known(ctx, device); err != nil {
		return 0, err
	}
	pretty, _ := json.MarshalIndent(v.Any(), "", "  ")
	version, err := h.st.SetConfig(ctx, device, cbor, string(pretty), h.now())
	if err == nil {
		h.log.Info("device config set", "device", short(device), "version", version, "bytes", len(cbor))
		h.Notify.DeviceChanged(device)
	}
	return version, err
}

// maxCommandName bounds a command's name.
const maxCommandName = 32

// QueueCommand validates a command, with its arguments as JSON (empty for
// none), and queues it for the device, which fetches it at its next
// check-in. It returns the command's ID.
func (h *Hub) QueueCommand(ctx context.Context, device, name string, jsonArgs []byte) (uint32, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > maxCommandName {
		return 0, invalid(fmt.Errorf("command name must be 1 to %d characters", maxCommandName))
	}
	if err := h.known(ctx, device); err != nil {
		return 0, err
	}
	args := hubproto.Null()
	if len(bytes.TrimSpace(jsonArgs)) > 0 {
		var x any
		if err := json.Unmarshal(jsonArgs, &x); err != nil {
			return 0, invalid(fmt.Errorf("arguments: %w", err))
		}
		var ok bool
		if args, ok = hubproto.FromAny(x); !ok {
			return 0, invalid(errors.New("arguments must be plain JSON"))
		}
	}
	if _, err := hubproto.Encode(nil, 0, &hubproto.Command{ID: math.MaxUint32, Name: name, Args: args}, hubproto.MaxPlaintext); err != nil {
		return 0, invalid(errors.New("command too large for a datagram"))
	}
	argsJSON, _ := json.Marshal(args.Any())
	id, err := h.st.AddCommand(ctx, device, name, string(argsJSON), args.AppendCBOR(nil), h.now())
	if err == nil {
		h.log.Info("device command queued", "device", short(device), "id", id, "name", name)
		h.Notify.CommandChanged(device, id)
	}
	return id, err
}

// CancelCommand cancels a command the device has not run yet.
func (h *Hub) CancelCommand(ctx context.Context, device string, id uint32) error {
	ok, err := h.st.CancelCommand(ctx, device, id, h.now())
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotPending
	}
	h.Notify.CommandChanged(device, id)
	return nil
}

// SetDeviceStatus approves (paired) or rejects a device. It takes effect
// the next time the device asks to pair.
func (h *Hub) SetDeviceStatus(ctx context.Context, device, status string) error {
	if status != store.DevicePaired && status != store.DeviceRejected {
		return invalid(fmt.Errorf("status must be %s or %s", store.DevicePaired, store.DeviceRejected))
	}
	if err := h.known(ctx, device); err != nil {
		return err
	}
	if err := h.st.SetDeviceStatus(ctx, device, status); err != nil {
		return err
	}
	h.log.Info("device status set", "device", short(device), "status", status)
	h.Notify.DeviceChanged(device)
	return nil
}

// DeleteDevice forgets a device with its config, events and commands. If
// it asks to pair again, it is a new, pending device.
func (h *Hub) DeleteDevice(ctx context.Context, device string) error {
	d, ok, err := h.st.Device(ctx, device)
	if err != nil {
		return err
	}
	if !ok {
		return ErrUnknownDevice
	}
	if err := h.st.DeleteDevice(ctx, device); err != nil {
		return err
	}
	h.log.Info("device deleted", "device", short(device))
	h.Notify.DeviceDeleted(d)
	return nil
}

// known returns ErrUnknownDevice for a device the hub does not know.
func (h *Hub) known(ctx context.Context, device string) error {
	_, ok, err := h.st.Device(ctx, device)
	if err == nil && !ok {
		err = ErrUnknownDevice
	}
	return err
}
