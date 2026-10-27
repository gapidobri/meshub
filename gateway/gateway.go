// Package gateway bridges a MeshCore radio and an MQTT broker. It publishes
// every packet it receives or transmits to the backend, and transmits raw
// packets the backend asks it to (see package gwproto for the topics and
// encodings). It holds no keys of other nodes and never decrypts anything:
// the backend runs the mesh logic, the gateway only moves packets.
//
// The package is hardware independent; cmd/t3s3 is the firmware for the
// LilyGo T3S3.
package gateway

import (
	"sync"
	"sync/atomic"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/proto/gwproto"
)

// Config configures a Gateway.
type Config struct {
	// Name is the gateway's human readable name, published as "origin".
	Name string
	// IATA is the region code used in the uplink topics, e.g. "LJU".
	IATA string
	// Identity is the gateway's MeshCore identity. Its public key names the
	// gateway in topics.
	Identity meshcore.LocalIdentity

	Model           string
	FirmwareVersion string
	// Radio describes the modulation as "freq_mhz,bw_khz,sf,cr".
	Radio string

	// Repeat makes the gateway forward packets like a MeshCore repeater. When
	// false it only listens and transmits what the backend asks for.
	Repeat bool
	// DutyCycle is the fraction of time the gateway may transmit, e.g. 0.1.
	// Zero keeps the MeshCore default.
	DutyCycle float32

	// MQTT credentials; leave Username empty for anonymous access.
	Username string
	Password string

	// StatusInterval is how often the online status is republished
	// (default 5 minutes).
	StatusInterval time.Duration
	// OnOffline is called from RunMQTT before each reconnect attempt with how
	// long the gateway has been without a broker connection. Firmware can use
	// it to reset a gateway that stays offline.
	OnOffline func(offline time.Duration)
}

// clientVersion is published as the status "client_version".
const clientVersion = "meshub-gateway/0.1.0"

const (
	uplinkQueueLen  = 32
	txQueueLen      = 8
	outcomeQueueLen = 16
	txTimeout       = 5 * time.Minute // beyond the requested delay, before reporting TxExpired
	statsInterval   = time.Second
)

// Gateway is a MeshCore Handler that forwards radio traffic to MQTT. Loop
// must be called from a single goroutine (the one driving the Mesh); RunMQTT
// runs in its own.
type Gateway struct {
	meshcore.BaseHandler

	cfg    Config
	mesh   *meshcore.Mesh
	pubKey string
	start  time.Time

	topicTx                                     string
	topicPacketsB, topicStatusB, topicTxResultB []byte

	uplink   chan uplinkEvent
	txReqs   chan txRequest
	outcomes chan txOutcome

	// Mesh goroutine state.
	scratch   meshcore.Packet
	txRaw     [meshcore.MaxTransUnit]byte
	ev        uplinkEvent
	pending   []pendingTx
	nextStats time.Time

	statsMu sync.Mutex
	stats   gwproto.Stats

	uplinkDropped atomic.Uint32
	reconnects    atomic.Uint32
	connected     atomic.Bool

	// MQTT goroutine state.
	mqttRx   [2048]byte
	pubBuf   [gwproto.MaxTxRequestLen + 1]byte
	txBuf    []byte
	evOut    gwproto.PacketEvent
	clientID string
}

// uplinkEvent is a packet waiting to be published. It is a value type so it
// can be queued without referring to Mesh-owned buffers.
type uplinkEvent struct {
	t           time.Time
	tx          bool
	raw         [meshcore.MaxTransUnit]byte
	n           uint8
	payloadType uint8
	direct      bool
	payloadLen  uint8
	hash        [meshcore.MaxHashSize]byte
	snr, rssi   int16
	score       int16
	airtimeMs   uint32
}

type txRequest struct {
	id       uint32
	priority uint8
	delayMs  uint16
	raw      [meshcore.MaxTransUnit]byte
	n        uint8
}

type txOutcome struct {
	id      uint32
	status  string
	hash    [meshcore.MaxHashSize]byte
	hasHash bool
}

type pendingTx struct {
	pkt      *meshcore.Packet
	id       uint32
	hash     [meshcore.MaxHashSize]byte
	deadline time.Time
}

// New creates a Gateway and the Mesh it drives. mc.Handler is ignored; the
// Gateway is the handler. poolSize must match the size of mc.Packets.
func New(cfg Config, mc meshcore.Config, poolSize int) *Gateway {
	if cfg.StatusInterval == 0 {
		cfg.StatusInterval = 5 * time.Minute
	}
	g := &Gateway{
		cfg:      cfg,
		pubKey:   cfg.Identity.String(),
		start:    time.Now(),
		uplink:   make(chan uplinkEvent, uplinkQueueLen),
		txReqs:   make(chan txRequest, txQueueLen),
		outcomes: make(chan txOutcome, outcomeQueueLen),
		pending:  make([]pendingTx, poolSize),
		txBuf:    make([]byte, 0, 1536),
	}
	g.topicTx = gwproto.TxTopic(g.pubKey)
	g.topicPacketsB = []byte(gwproto.PacketsTopic(cfg.IATA, g.pubKey))
	g.topicStatusB = []byte(gwproto.StatusTopic(cfg.IATA, g.pubKey))
	g.topicTxResultB = []byte(gwproto.TxResultTopic(g.pubKey))
	g.clientID = "meshub_gw_" + g.pubKey[:12]

	mc.Handler = g
	g.mesh = meshcore.NewMesh(mc)
	g.mesh.SelfID = cfg.Identity
	return g
}

// PublicKey returns the gateway's public key in upper-case hex.
func (g *Gateway) PublicKey() string { return g.pubKey }

// Mesh returns the Mesh the gateway drives.
func (g *Gateway) Mesh() *meshcore.Mesh { return g.mesh }

// Connected reports whether the gateway currently has a broker session.
func (g *Gateway) Connected() bool { return g.connected.Load() }

// Begin starts the radio.
func (g *Gateway) Begin() { g.mesh.Begin() }

// Loop runs one iteration of the mesh and handles queued transmit requests.
func (g *Gateway) Loop() {
	g.mesh.Loop()

	for i := 0; i < txQueueLen && g.nextTxRequest(); i++ {
	}

	now := time.Now()
	g.expirePending(now)
	if !now.Before(g.nextStats) {
		g.snapshotStats(now)
		g.nextStats = now.Add(statsInterval)
	}
}

func (g *Gateway) AllowPacketForward(p *meshcore.Packet) bool { return g.cfg.Repeat }

// CalcRxDelay delays processing of weak flood packets so a repeater
// forwards the best copy first. A gateway that does not repeat has no reason
// to hold them, and doing so would tie up the packet pool.
func (g *Gateway) CalcRxDelay(score float32, airTime uint32) int {
	if !g.cfg.Repeat {
		return 0
	}
	return g.BaseHandler.CalcRxDelay(score, airTime)
}

func (g *Gateway) AirtimeBudgetFactor() float32 {
	if g.cfg.DutyCycle <= 0 || g.cfg.DutyCycle >= 1 {
		return g.BaseHandler.AirtimeBudgetFactor()
	}
	// The dispatcher's duty cycle is 1 / (1 + factor).
	return 1/g.cfg.DutyCycle - 1
}

// LogRxRaw publishes every packet the radio receives. It runs before the
// Mesh allocates from its pool, so packets are reported even when the pool
// is exhausted.
func (g *Gateway) LogRxRaw(snr, rssi float32, raw []byte) {
	p := &g.scratch
	if !g.mesh.TryParsePacket(p, raw) {
		return
	}
	ev := &g.ev
	g.fillEvent(ev, p, raw)
	ev.tx = false
	ev.snr = int16(snr)
	ev.rssi = int16(rssi)
	ev.score = int16(g.mesh.Radio().PacketScore(snr, len(raw)) * 1000)
	ev.airtimeMs = g.mesh.Radio().EstAirtimeFor(len(raw))
	g.queueUplink(ev)
}

func (g *Gateway) LogTx(p *meshcore.Packet, length int) {
	ev := &g.ev
	n := p.Encode(g.txRaw[:])
	g.fillEvent(ev, p, g.txRaw[:n])
	ev.tx = true
	ev.snr, ev.rssi, ev.score, ev.airtimeMs = 0, 0, 0, 0
	g.queueUplink(ev)
	g.resolvePending(p, gwproto.TxSent)
}

func (g *Gateway) LogTxFail(p *meshcore.Packet, length int) {
	g.resolvePending(p, gwproto.TxFailed)
}

func (g *Gateway) fillEvent(ev *uplinkEvent, p *meshcore.Packet, raw []byte) {
	ev.t = time.Now()
	ev.n = uint8(copy(ev.raw[:], raw))
	ev.payloadType = p.PayloadType()
	ev.direct = p.IsRouteDirect()
	ev.payloadLen = uint8(p.PayloadLen)
	p.CalculateHash(&ev.hash)
}

func (g *Gateway) queueUplink(ev *uplinkEvent) {
	select {
	case g.uplink <- *ev:
	default:
		g.uplinkDropped.Add(1)
	}
}

func (g *Gateway) queueOutcome(o txOutcome) {
	select {
	case g.outcomes <- o:
	default: // the backend will see no result and can retry
	}
}

// nextTxRequest transmits the next queued request, reporting whether there
// was one.
func (g *Gateway) nextTxRequest() bool {
	select {
	case r := <-g.txReqs:
		g.transmit(&r)
		return true
	default:
		return false
	}
}

// transmit queues a backend transmit request on the mesh.
func (g *Gateway) transmit(r *txRequest) {
	m := g.mesh
	slot := -1
	for i := range g.pending {
		if g.pending[i].pkt == nil {
			slot = i
			break
		}
	}
	if slot < 0 {
		g.queueOutcome(txOutcome{id: r.id, status: gwproto.TxFull})
		return
	}
	pkt := m.ObtainNewPacket()
	if pkt == nil {
		g.queueOutcome(txOutcome{id: r.id, status: gwproto.TxFull})
		return
	}
	if !m.TryParsePacket(pkt, r.raw[:r.n]) || !meshcore.IsValidPathLen(pkt.PathLen) {
		m.ReleasePacket(pkt)
		g.queueOutcome(txOutcome{id: r.id, status: gwproto.TxInvalid})
		return
	}
	pt := &g.pending[slot]
	pt.pkt = pkt
	pt.id = r.id
	pkt.CalculateHash(&pt.hash)
	pt.deadline = time.Now().Add(time.Duration(r.delayMs)*time.Millisecond + txTimeout)
	m.SendPacket(pkt, r.priority, uint32(r.delayMs))
}

func (g *Gateway) resolvePending(p *meshcore.Packet, status string) {
	for i := range g.pending {
		pt := &g.pending[i]
		if pt.pkt == p {
			g.queueOutcome(txOutcome{id: pt.id, status: status, hash: pt.hash, hasHash: true})
			pt.pkt = nil
			return
		}
	}
}

func (g *Gateway) expirePending(now time.Time) {
	for i := range g.pending {
		pt := &g.pending[i]
		if pt.pkt != nil && now.After(pt.deadline) {
			g.queueOutcome(txOutcome{id: pt.id, status: gwproto.TxExpired, hash: pt.hash, hasHash: true})
			pt.pkt = nil
		}
	}
}

func (g *Gateway) snapshotStats(now time.Time) {
	m := g.mesh
	st := gwproto.Stats{
		UptimeSecs:  uint32(now.Sub(g.start) / time.Second),
		DebugFlags:  m.ErrFlags,
		QueueLen:    m.Packets().OutboundTotal(),
		NoiseFloor:  m.Radio().NoiseFloor(),
		TxAirSecs:   m.TotalAirTime() / 1000,
		RxAirSecs:   m.ReceiveAirTime() / 1000,
		FreePackets: m.Packets().FreeCount(),
		TxBudgetMs:  m.RemainingTxBudget(),
	}
	g.statsMu.Lock()
	g.stats = st
	g.statsMu.Unlock()
}
