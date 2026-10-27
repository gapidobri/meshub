// Package ingest turns gateway MQTT traffic into backend state: it tracks
// gateway status, deduplicates packets heard by several gateways and learns
// nodes from their adverts.
package ingest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/proto/gwproto"
	"github.com/gapidobri/meshub/server/internal/mqttconn"
	"github.com/gapidobri/meshub/server/internal/store"
)

// Topic filters the ingester subscribes to.
const (
	StatusFilter  = "meshcore/+/+/status"
	PacketsFilter = "meshcore/+/+/packets"
)

// maxClockSkew is how far a gateway's timestamp may be ahead of the
// backend's clock before it is distrusted.
const maxClockSkew = time.Minute

// Ingester handles gateway messages.
type Ingester struct {
	store *store.Store
	log   *slog.Logger
	now   func() time.Time

	// Self is the backend's own public key. Its adverts heard back from the
	// mesh go to OnSelfAdvert rather than into the node list.
	Self         string
	OnSelfAdvert func(gateway string, hops, snr int, at time.Time)

	// Hub, when set, gets the requests addressed to the backend.
	Hub Requests
}

// Requests handles packets addressed to the hub.
type Requests interface {
	IsForHub(payloadType uint8, payload []byte) bool
	Receive(ctx context.Context, raw []byte, hash, gateway string)
}

// New returns an Ingester writing to st.
func New(st *store.Store, log *slog.Logger) *Ingester {
	return &Ingester{store: st, log: log, now: time.Now}
}

// gatewayTopic splits "meshcore/{IATA}/{PUBLIC_KEY}/{kind}".
func gatewayTopic(topic string) (iata, pubKey, kind string, ok bool) {
	parts := strings.Split(topic, "/")
	if len(parts) != 4 || parts[0] != "meshcore" {
		return "", "", "", false
	}
	return parts[1], strings.ToUpper(parts[2]), parts[3], true
}

// Subscriptions returns the ingester's MQTT subscriptions; ctx bounds the
// database writes.
func (in *Ingester) Subscriptions(ctx context.Context) []mqttconn.Subscription {
	h := func(topic string, payload []byte) { in.Handle(ctx, topic, payload) }
	return []mqttconn.Subscription{{Filter: StatusFilter, Handle: h}, {Filter: PacketsFilter, Handle: h}}
}

// Handle processes one MQTT message.
func (in *Ingester) Handle(ctx context.Context, topic string, payload []byte) {
	iata, pubKey, kind, ok := gatewayTopic(topic)
	if !ok {
		return
	}
	var err error
	switch kind {
	case "status":
		err = in.handleStatus(ctx, iata, pubKey, payload)
	case "packets":
		err = in.handlePacket(ctx, pubKey, payload)
	default:
		return
	}
	if err != nil {
		in.log.Warn("ingest", "topic", topic, "err", err)
	}
}

func (in *Ingester) handleStatus(ctx context.Context, iata, pubKey string, payload []byte) error {
	var s gwproto.Status
	if err := json.Unmarshal(payload, &s); err != nil {
		return err
	}
	stats := []byte("{}")
	if s.Stats != nil {
		stats, _ = json.Marshal(s.Stats)
	}
	name := s.Origin
	if name == "" {
		name = pubKey[:min(8, len(pubKey))]
	}
	return in.store.UpsertGateway(ctx, store.Gateway{
		PubKey:       pubKey,
		IATA:         iata,
		Name:         name,
		Status:       s.Status,
		Model:        s.Model,
		Firmware:     s.FirmwareVersion,
		Radio:        s.Radio,
		Stats:        string(stats),
		LastStatusAt: in.now(),
	})
}

func (in *Ingester) handlePacket(ctx context.Context, gateway string, payload []byte) error {
	var msg gwproto.Packet
	if err := json.Unmarshal(payload, &msg); err != nil {
		return err
	}
	raw, err := hex.DecodeString(msg.Raw)
	if err != nil {
		return err
	}
	var p meshcore.Packet
	if !p.Decode(raw) {
		return errMalformedPacket
	}
	var h [meshcore.MaxHashSize]byte
	p.CalculateHash(&h)
	route := "F"
	if p.IsRouteDirect() {
		route = "D"
	}
	at := in.receivedAt(msg.Timestamp)
	snr, rssi := gwproto.Int(msg.SNR), gwproto.Int(msg.RSSI)
	direction := msg.Direction
	if direction != gwproto.DirectionTx {
		direction = gwproto.DirectionRx
	}

	first, err := in.store.AddReception(ctx, store.Reception{
		Hash:        meshcore.ToHex(h[:]),
		PayloadType: p.PayloadType(),
		Route:       route,
		Raw:         raw,
		Gateway:     gateway,
		Direction:   direction,
		SNR:         snr,
		RSSI:        rssi,
		Hops:        p.PathHashCount(), // hops travelled for flood packets, hops left for direct
		PathLen:     p.PathLen,
		Path:        p.PathBytes(),
		ReceivedAt:  at,
	})
	if err != nil {
		return err
	}
	if direction != gwproto.DirectionRx {
		return nil
	}
	if in.Hub != nil && in.Hub.IsForHub(p.PayloadType(), p.PayloadBytes()) {
		in.Hub.Receive(ctx, raw, meshcore.ToHex(h[:]), gateway)
		return nil
	}
	if p.PayloadType() != meshcore.PayloadTypeAdvert {
		return nil
	}

	a, err := parseAdvert(p.PayloadBytes())
	if err != nil {
		return err
	}
	if a.id.String() == in.Self {
		if in.OnSelfAdvert != nil {
			in.OnSelfAdvert(gateway, p.PathHashCount(), snr, at)
		}
		return nil
	}
	if first {
		in.log.Info("advert", "node", a.id.String()[:8], "name", a.data.Name(), "gateway", gateway[:8], "hops", p.PathHashCount())
	}
	return in.store.AddAdvert(ctx, store.Advert{
		PubKey:     a.id.String(),
		Name:       a.data.Name(),
		Type:       a.data.Type(),
		HasLoc:     a.data.HasLatLon(),
		Lat:        a.data.LatDegrees(),
		Lon:        a.data.LonDegrees(),
		AdvertTime: a.timestamp,
		Gateway:    gateway,
		SNR:        snr,
		RSSI:       rssi,
		Hops:       p.PathHashCount(),
		HeardAt:    at,
	})
}

// receivedAt trusts the gateway's timestamp, which survives the gateway
// queueing packets while offline, unless it is unparseable or in the future.
func (in *Ingester) receivedAt(ts string) time.Time {
	now := in.now()
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil || t.After(now.Add(maxClockSkew)) {
		return now
	}
	return t
}
