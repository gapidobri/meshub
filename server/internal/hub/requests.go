package hub

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
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
	time.AfterFunc(collectWindow, func() { h.answer(ctx, req) })
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

	switch m := req.msg.(type) {
	case *hubproto.Hello:
		err := h.st.RecordHello(ctx, req.device, store.Hello{
			ConfigVersion: m.ConfigVersion, WakeReason: m.WakeReason, BatteryMV: m.BatteryMV,
			UptimeS: m.UptimeS, Firmware: m.Firmware,
		}, h.now())
		if err != nil {
			return nil, err
		}
		cfg, err := h.st.Config(ctx, req.device)
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

	case *hubproto.Event:
		data, _ := json.Marshal(m.Data.Any())
		err := h.st.AddEvent(ctx, req.device, store.Event{Seq: m.EventSeq, Kind: m.Kind, Data: string(data), ReceivedAt: h.now()})
		if err != nil {
			return nil, err
		}
		h.log.Info("device event", "device", short(req.device), "kind", m.Kind, "data", string(data))
		return &hubproto.EventAck{EventSeq: m.EventSeq}, nil
	}
	return &hubproto.Error{Code: hubproto.ErrBadRequest}, nil
}

func (h *Hub) handleJoin(ctx context.Context, req *request, j *hubproto.Join) (hubproto.Message, error) {
	status := store.DevicePending
	if h.AutoPair {
		status = store.DevicePaired
	}
	dev, err := h.st.Join(ctx, req.device, j.Program, j.Firmware, j.Claim, status, req.seq, h.now())
	if err != nil {
		return nil, err
	}
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
	case hubproto.TypeEvent:
		return "event"
	case hubproto.TypeJoinReply:
		return "join-reply"
	case hubproto.TypeHelloReply:
		return "hello-reply"
	case hubproto.TypeConfigChunk:
		return "config-chunk"
	case hubproto.TypeEventAck:
		return "event-ack"
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
		return 0, err
	}
	v, ok := hubproto.FromAny(x)
	if !ok || v.Kind() != hubproto.KindMap {
		return 0, errors.New("config must be a JSON object")
	}
	cbor := v.AppendCBOR(nil)
	if len(cbor) > 0xFFFF {
		return 0, errors.New("config too large")
	}
	pretty, _ := json.MarshalIndent(v.Any(), "", "  ")
	version, err := h.st.SetConfig(ctx, device, cbor, string(pretty), h.now())
	if err == nil {
		h.log.Info("device config set", "device", short(device), "version", version, "bytes", len(cbor))
	}
	return version, err
}
