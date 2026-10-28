package ingest

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/proto/gwproto"
	"github.com/gapidobri/meshub/server/internal/mqttconn"
	"github.com/gapidobri/meshub/server/internal/store"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

const (
	gwA = "AAAA000000000000000000000000000000000000000000000000000000000000"
	gwB = "BBBB000000000000000000000000000000000000000000000000000000000000"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type nullRadio struct{ meshcore.RadioBase }

func (nullRadio) RecvRaw([]byte) int               { return 0 }
func (nullRadio) EstAirtimeFor(int) uint32         { return 0 }
func (nullRadio) PacketScore(float32, int) float32 { return 0 }
func (nullRadio) StartSendRaw([]byte) bool         { return true }
func (nullRadio) IsSendComplete() bool             { return true }
func (nullRadio) OnSendFinished()                  {}
func (nullRadio) IsInRecvMode() bool               { return true }

type fixedRTC struct{ t uint32 }

func (c *fixedRTC) CurrentTime() uint32     { return c.t }
func (c *fixedRTC) SetCurrentTime(t uint32) { c.t = t }
func (c *fixedRTC) Tick()                   {}

type seqRNG struct{ n byte }

func (s *seqRNG) Random(dst []byte) {
	for i := range dst {
		s.n++
		dst[i] = s.n
	}
}

// node is a MeshCore node that can produce signed adverts.
type node struct {
	mesh *meshcore.Mesh
	rtc  *fixedRTC
}

func newNode(seed byte) *node {
	rtc := &fixedRTC{t: 1_800_000_000}
	ms := meshcore.NewSystemMillis()
	m := meshcore.NewMesh(meshcore.Config{
		Radio: nullRadio{}, Millis: ms, RNG: &seqRNG{}, RTC: rtc,
		Packets: meshcore.NewStaticPoolPacketManager(4), Tables: &meshcore.SimpleMeshTables{},
	})
	m.SelfID = meshcore.LocalIdentityFromSeed(bytes.Repeat([]byte{seed}, 32))
	return &node{mesh: m, rtc: rtc}
}

// advert returns the raw wire bytes of a flood advert that crossed hops
// repeaters.
func (n *node) advert(t *testing.T, ad meshcore.AdvertData, at uint32, hops int) []byte {
	t.Helper()
	n.rtc.t = at
	var app [meshcore.MaxAdvertDataSize]byte
	p := n.mesh.CreateAdvert(&n.mesh.SelfID, app[:ad.Encode(&app)])
	if p == nil {
		t.Fatal("CreateAdvert failed")
	}
	defer n.mesh.ReleasePacket(p)
	p.Header |= meshcore.RouteTypeFlood
	for i := 0; i < hops; i++ {
		p.Path[i] = byte(0x10 + i)
	}
	p.SetPathHashCount(hops)
	raw := make([]byte, meshcore.MaxTransUnit)
	return raw[:p.Encode(raw)]
}

func packetMsg(raw []byte, snr, rssi int) []byte {
	e := gwproto.PacketEvent{Origin: "gw", Time: time.Now(), Raw: raw, SNR: snr, RSSI: rssi}
	return e.AppendJSON(nil)
}

func newIngester(t *testing.T) (*Ingester, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return New(st, quiet), st
}

func TestGatewayStatus(t *testing.T) {
	in, st := newIngester(t)
	ctx := context.Background()

	online := gwproto.Status{Status: gwproto.StatusOnline, Origin: "roof", Radio: "869.618,62.5,8,5", Stats: &gwproto.Stats{UptimeSecs: 42}}
	in.Handle(ctx, "meshcore/LJU/"+gwA+"/status", online.AppendJSON(nil, time.Now()))
	offline := gwproto.Status{Status: gwproto.StatusOffline, Origin: "roof"}
	in.Handle(ctx, "meshcore/LJU/"+gwA+"/status", offline.AppendJSON(nil, time.Now()))

	gws, err := st.Gateways(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(gws) != 1 {
		t.Fatalf("gateways = %+v", gws)
	}
	g := gws[0]
	if g.PubKey != gwA || g.IATA != "LJU" || g.Name != "roof" || g.Status != gwproto.StatusOffline || g.Radio != "869.618,62.5,8,5" {
		t.Errorf("gateway = %+v", g)
	}
	if !bytes.Contains([]byte(g.Stats), []byte(`"uptime_secs":42`)) {
		t.Errorf("offline status (no stats) dropped the last stats: %s", g.Stats)
	}
}

func TestAdvertsFromSeveralGateways(t *testing.T) {
	in, st := newIngester(t)
	ctx := context.Background()
	n := newNode(3)
	raw := n.advert(t, meshcore.NewAdvertDataLatLon(meshcore.AdvTypeSensor, "weather 1", 46.05, 14.5), 1_800_000_000, 2)

	// The same advert heard by two gateways, B over fewer hops.
	in.Handle(ctx, "meshcore/LJU/"+gwA+"/packets", packetMsg(raw, 3, -110))
	rawB := n.advert(t, meshcore.NewAdvertDataLatLon(meshcore.AdvTypeSensor, "weather 1", 46.05, 14.5), 1_800_000_000, 0)
	in.Handle(ctx, "meshcore/LJU/"+gwB+"/packets", packetMsg(rawB, 9, -80))

	packets, receptions, err := st.PacketCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if packets != 1 || receptions != 2 {
		t.Errorf("packets, receptions = %d, %d; want 1, 2 (deduplicated by hash)", packets, receptions)
	}
	nodes, err := st.Nodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("nodes = %+v", nodes)
	}
	got := nodes[0]
	if got.PubKey != n.mesh.SelfID.String() || got.Name != "weather 1" || got.Type != meshcore.AdvTypeSensor ||
		!got.HasLoc || got.Lat != 46.05 || got.Lon != 14.5 {
		t.Errorf("node = %+v", got)
	}
	if len(got.Links) != 2 || got.Links[0].Gateway != gwB || got.Links[0].Hops != 0 || got.Links[0].SNR != 9 ||
		got.Links[1].Gateway != gwA || got.Links[1].Hops != 2 {
		t.Errorf("links = %+v; want B (0 hops) before A (2 hops)", got.Links)
	}
}

func TestAdvertReplayAndForgery(t *testing.T) {
	in, st := newIngester(t)
	ctx := context.Background()
	n := newNode(4)
	topic := "meshcore/LJU/" + gwA + "/packets"

	in.Handle(ctx, topic, packetMsg(n.advert(t, meshcore.AdvertData{Type: meshcore.AdvTypeChat, Name: "new"}, 2_000_000_000, 0), 0, 0))
	in.Handle(ctx, topic, packetMsg(n.advert(t, meshcore.AdvertData{Type: meshcore.AdvTypeChat, Name: "old"}, 1_000_000_000, 0), 0, 0))

	forged := n.advert(t, meshcore.AdvertData{Type: meshcore.AdvTypeChat, Name: "evil"}, 2_100_000_000, 0)
	forged[len(forged)-1] ^= 1 // change the signed name
	in.Handle(ctx, topic, packetMsg(forged, 0, 0))

	nodes, err := st.Nodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].Name != "new" {
		t.Errorf("nodes = %+v; want one node still named \"new\"", nodes)
	}
}

func TestOverMQTT(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	srv := mochi.New(&mochi.Options{InlineClient: true, Logger: quiet})
	srv.AddHook(new(auth.AllowHook), nil)
	if err := srv.AddListener(listeners.NewTCP(listeners.Config{ID: "t", Address: addr})); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	defer srv.Close()

	// Published before the ingester connects: the retained status must still
	// be picked up.
	status := gwproto.Status{Status: gwproto.StatusOnline, Origin: "roof"}
	if err := srv.Publish("meshcore/LJU/"+gwA+"/status", status.AppendJSON(nil, time.Now()), true, 0); err != nil {
		t.Fatal(err)
	}

	in, st := newIngester(t)
	ctx := context.Background()
	conn := mqttconn.Connect(mqttconn.Config{Broker: "tcp://" + addr, ClientID: "test"}, quiet, in.Subscriptions(ctx)...)
	defer conn.Close()

	waitFor(t, func() bool { gws, _ := st.Gateways(ctx); return len(gws) == 1 })

	raw := newNode(5).advert(t, meshcore.AdvertData{Type: meshcore.AdvTypeRepeater, Name: "rpt"}, 1_800_000_000, 1)
	if err := srv.Publish("meshcore/LJU/"+gwA+"/packets", packetMsg(raw, 1, -100), false, 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { nodes, _ := st.Nodes(ctx); return len(nodes) == 1 && nodes[0].Name == "rpt" })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatal("condition not met in time")
}

func TestOwnAdvert(t *testing.T) {
	in, st := newIngester(t)
	ctx := context.Background()
	self := newNode(6)
	in.Self = self.mesh.SelfID.String()
	var echoes []int
	in.OnSelfAdvert = func(gateway string, hops, snr int, at time.Time) { echoes = append(echoes, hops) }

	raw := self.advert(t, meshcore.AdvertData{Type: meshcore.AdvTypeChat, Name: "meshub"}, 1_800_000_000, 2)
	in.Handle(ctx, "meshcore/LJU/"+gwA+"/packets", packetMsg(raw, 4, -95))

	if len(echoes) != 1 || echoes[0] != 2 {
		t.Errorf("echoes = %v, want one with 2 hops", echoes)
	}
	if nodes, _ := st.Nodes(ctx); len(nodes) != 0 {
		t.Errorf("own advert added as a node: %+v", nodes)
	}
}
