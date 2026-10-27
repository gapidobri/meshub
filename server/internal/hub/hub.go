// Package hub is the backend's own presence on the mesh: its MeshCore
// identity, and transmitting through gateways on its behalf.
package hub

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/proto/gwproto"
	"github.com/gapidobri/meshub/server/internal/mqttconn"
	"github.com/gapidobri/meshub/server/internal/store"
)

// TxResultFilter matches every gateway's transmit results.
const TxResultFilter = "meshub/gw/+/tx/result"

const (
	identityKey = "identity_seed"
	// advertStagger spaces out the same advert sent from several gateways,
	// so they do not collide on air. Repeaters drop the later copies.
	advertStagger = 2 * time.Second
	// pendingTTL is how long to wait for a gateway's transmit result.
	pendingTTL = 10 * time.Minute
	// maxResults is how many recent transmit results are kept for display.
	maxResults = 10
)

// ErrNoGateway is returned when no gateway is online to transmit through.
var ErrNoGateway = errors.New("hub: no gateway online")

// Publisher publishes MQTT messages.
type Publisher interface {
	Publish(topic string, payload []byte) error
}

// Hub is the backend on the mesh: it advertises, and pairs and serves
// devices (see docs/protocol.md).
type Hub struct {
	// AutoPair accepts every join without operator approval.
	AutoPair bool

	id   meshcore.LocalIdentity
	name string
	pub  Publisher
	st   *store.Store
	log  *slog.Logger
	now  func() time.Time

	nextID atomic.Uint32
	rx     *receiver

	mu         sync.Mutex
	pending    map[uint32]pendingTx
	results    []TxResult
	lastAdvert time.Time
	lastEcho   *Echo
}

type pendingTx struct {
	gateway string
	what    string
	sentAt  time.Time
}

// TxResult is a gateway's report on something the hub asked it to send.
type TxResult struct {
	Gateway string    `json:"gateway"`
	What    string    `json:"what"`
	Status  string    `json:"status"`
	At      time.Time `json:"at"`
}

// Echo is the hub's own advert heard back from the mesh, which shows it went
// out on air and was repeated.
type Echo struct {
	Gateway string    `json:"gateway"`
	Hops    int       `json:"hops"`
	SNR     int       `json:"snr"`
	At      time.Time `json:"at"`
}

// LoadIdentity returns the hub's identity, creating and storing a new one
// on first use.
func LoadIdentity(ctx context.Context, st *store.Store) (meshcore.LocalIdentity, error) {
	seed, err := st.Setting(ctx, identityKey)
	if err != nil {
		return meshcore.LocalIdentity{}, err
	}
	if len(seed) != 32 {
		seed = make([]byte, 32)
		rand.Read(seed)
		if err := st.SetSetting(ctx, identityKey, seed); err != nil {
			return meshcore.LocalIdentity{}, err
		}
	}
	return meshcore.LocalIdentityFromSeed(seed), nil
}

// New returns a Hub with identity id, advertising as name.
func New(id meshcore.LocalIdentity, name string, pub Publisher, st *store.Store, log *slog.Logger) *Hub {
	h := &Hub{
		id:      id,
		name:    name,
		pub:     pub,
		st:      st,
		log:     log,
		now:     time.Now,
		pending: map[uint32]pendingTx{},
	}
	var b [4]byte
	rand.Read(b[:])
	h.nextID.Store(binary.LittleEndian.Uint32(b[:])) // so IDs do not repeat across restarts
	h.rx = newReceiver(h)
	return h
}

// PublicKey returns the hub's public key in upper-case hex.
func (h *Hub) PublicKey() string { return h.id.String() }

// Name returns the name the hub advertises.
func (h *Hub) Name() string { return h.name }

// Subscriptions returns the hub's MQTT subscriptions.
func (h *Hub) Subscriptions() []mqttconn.Subscription {
	return []mqttconn.Subscription{{Filter: TxResultFilter, Handle: h.handleTxResult}}
}

// advert returns a signed flood advert for the hub, in wire format.
func (h *Hub) advert() []byte {
	ad := meshcore.AdvertData{Type: meshcore.AdvTypeChat, Name: h.name}
	var app [meshcore.MaxAdvertDataSize]byte
	appData := app[:ad.Encode(&app)]
	ts := uint32(h.now().Unix())

	msg := make([]byte, 0, meshcore.PubKeySize+4+len(appData))
	msg = append(msg, h.id.PubKey[:]...)
	msg = binary.LittleEndian.AppendUint32(msg, ts)
	msg = append(msg, appData...)
	var sig [meshcore.SignatureSize]byte
	h.id.Sign(&sig, msg)

	var p meshcore.Packet
	p.Header = meshcore.PayloadTypeAdvert<<meshcore.PHTypeShift | meshcore.RouteTypeFlood
	p.SetPathHashSizeAndCount(1, 0) // like Mesh.SendFlood(p, 0, 1)
	n := copy(p.Payload[:], h.id.PubKey[:])
	binary.LittleEndian.PutUint32(p.Payload[n:], ts)
	n += 4
	n += copy(p.Payload[n:], sig[:])
	n += copy(p.Payload[n:], appData)
	p.PayloadLen = n

	raw := make([]byte, meshcore.MaxTransUnit)
	return raw[:p.Encode(raw)]
}

// SendAdvert floods the hub's advert from every online gateway, staggered.
// It returns how many gateways it was sent to.
func (h *Hub) SendAdvert(ctx context.Context) (int, error) {
	gws, err := h.st.OnlineGateways(ctx, h.now().Add(-store.GatewayStaleAfter))
	if err != nil {
		return 0, err
	}
	if len(gws) == 0 {
		return 0, ErrNoGateway
	}
	raw := h.advert()
	sent := 0
	for i, g := range gws {
		delay := time.Duration(i) * advertStagger
		if delay > time.Duration(1<<16-1)*time.Millisecond {
			break
		}
		if err := h.transmit(g.PubKey, "advert", raw, 1, delay); err != nil {
			h.log.Warn("advert", "gateway", g.Name, "err", err)
			continue
		}
		sent++
	}
	if sent == 0 {
		return 0, errors.New("hub: advert could not be sent to any gateway")
	}
	h.mu.Lock()
	h.lastAdvert = h.now()
	h.mu.Unlock()
	h.log.Info("advert sent", "gateways", sent)
	return sent, nil
}

// transmit asks a gateway to send raw.
func (h *Hub) transmit(gateway, what string, raw []byte, priority uint8, delay time.Duration) error {
	id := h.nextID.Add(1)
	req := gwproto.TxRequest{ID: id, Priority: priority, DelayMs: uint16(delay / time.Millisecond), Raw: raw}

	now := h.now()
	h.mu.Lock()
	for k, p := range h.pending {
		if now.Sub(p.sentAt) > pendingTTL {
			delete(h.pending, k)
		}
	}
	h.pending[id] = pendingTx{gateway: gateway, what: what, sentAt: now}
	h.mu.Unlock()

	if err := h.pub.Publish(gwproto.TxTopic(gateway), req.AppendBinary(nil)); err != nil {
		h.mu.Lock()
		delete(h.pending, id)
		h.mu.Unlock()
		return err
	}
	return nil
}

func (h *Hub) handleTxResult(topic string, payload []byte) {
	var r gwproto.TxResult
	if err := json.Unmarshal(payload, &r); err != nil {
		h.log.Warn("tx result", "topic", topic, "err", err)
		return
	}
	h.mu.Lock()
	p, ok := h.pending[r.ID]
	if ok {
		delete(h.pending, r.ID)
		h.results = append(h.results, TxResult{Gateway: p.gateway, What: p.what, Status: r.Status, At: h.now()})
		if len(h.results) > maxResults {
			h.results = h.results[len(h.results)-maxResults:]
		}
	}
	h.mu.Unlock()
	if !ok {
		return // another backend's request, or one we gave up on
	}
	level := slog.LevelInfo
	if r.Status != gwproto.TxSent {
		level = slog.LevelWarn
	}
	h.log.Log(context.Background(), level, "tx result", "what", p.what, "gateway", short(p.gateway), "status", r.Status)
}

// NoteOwnAdvert records the hub's own advert heard back through a gateway.
func (h *Hub) NoteOwnAdvert(gateway string, hops, snr int, at time.Time) {
	h.mu.Lock()
	h.lastEcho = &Echo{Gateway: gateway, Hops: hops, SNR: snr, At: at}
	h.mu.Unlock()
	h.log.Info("own advert heard", "gateway", short(gateway), "hops", hops, "snr", snr)
}

// Status is the hub's state for display.
type Status struct {
	PublicKey  string     `json:"pubkey"`
	Name       string     `json:"name"`
	LastAdvert time.Time  `json:"last_advert"`
	LastEcho   *Echo      `json:"last_echo"`
	Results    []TxResult `json:"results"` // newest first
}

// Status returns the hub's current state.
func (h *Hub) Status() Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := Status{PublicKey: h.PublicKey(), Name: h.name, LastAdvert: h.lastAdvert, Results: []TxResult{}}
	if h.lastEcho != nil {
		e := *h.lastEcho
		s.LastEcho = &e
	}
	for i := len(h.results) - 1; i >= 0; i-- {
		s.Results = append(s.Results, h.results[i])
	}
	return s
}

// RunAdverts sends the hub's advert every interval until ctx is done. It
// does not advertise on start, so restarts do not flood the mesh.
func (h *Hub) RunAdverts(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := h.SendAdvert(ctx); err != nil {
				h.log.Warn("periodic advert", "err", err)
			}
		}
	}
}

func short(pubKey string) string { return strings.ToUpper(pubKey[:min(8, len(pubKey))]) }
