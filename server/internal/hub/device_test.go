package hub

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/node"
	"github.com/gapidobri/meshub/node/flashstore"
	"github.com/gapidobri/meshub/proto/gwproto"
	"github.com/gapidobri/meshub/proto/hubproto"
	"github.com/gapidobri/meshub/server/internal/ingest"
	"github.com/gapidobri/meshub/server/internal/store"
)

// link is how the device reaches a gateway: through these repeaters, in
// order from the device.
type link struct {
	gateway   string
	repeaters []byte
	snr       int
}

// air simulates the radio between one device and the gateways, routing
// like MeshCore: floods collect repeater hashes, and direct packets only
// arrive along a path that matches the route.
type air struct {
	t   *testing.T
	in  *ingest.Ingester
	ctx context.Context

	mu    sync.Mutex
	links []link
	toDev [][]byte // packets arriving at the device
	tx    int      // packets the hub transmitted
}

func (a *air) setLinks(links ...link) {
	a.mu.Lock()
	a.links = links
	a.mu.Unlock()
}

// uplink delivers a device transmission to the gateways that hear it.
func (a *air) uplink(raw []byte) {
	var p meshcore.Packet
	if !p.Decode(raw) {
		a.t.Errorf("device sent an undecodable packet")
		return
	}
	a.mu.Lock()
	links := append([]link(nil), a.links...)
	a.mu.Unlock()
	for _, l := range links {
		heard := p
		if p.IsRouteFlood() {
			n := copy(heard.Path[heard.PathByteLen():], l.repeaters)
			heard.SetPathHashCount(heard.PathHashCount() + n)
		} else if !bytes.Equal(p.PathBytes(), l.repeaters) {
			continue // the direct route does not lead to this gateway
		} else {
			heard.SetPathHashCount(0) // each repeater removed itself
		}
		buf := make([]byte, meshcore.MaxTransUnit)
		ev := gwproto.PacketEvent{Time: time.Now(), Raw: buf[:heard.Encode(buf)], SNR: l.snr, RSSI: -90}
		a.in.Handle(a.ctx, "meshcore/LJU/"+l.gateway+"/packets", ev.AppendJSON(nil))
	}
}

// Publish receives the hub's transmit requests and delivers them to the
// device if the route reaches it.
func (a *air) Publish(topic string, payload []byte) error {
	var req gwproto.TxRequest
	if err := gwproto.ParseTxRequest(&req, payload); err != nil {
		return err
	}
	gateway := strings.Split(topic, "/")[2]
	var p meshcore.Packet
	if !p.Decode(req.Raw) {
		return errors.New("hub sent an undecodable packet")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tx++
	for _, l := range a.links {
		if l.gateway != gateway {
			continue
		}
		want := make([]byte, len(l.repeaters))
		for i, r := range l.repeaters {
			want[len(want)-1-i] = r
		}
		if p.IsRouteDirect() && !bytes.Equal(p.PathBytes(), want) {
			return nil // the path does not lead to the device
		}
		p.SetPathHashCount(0)
		buf := make([]byte, meshcore.MaxTransUnit)
		a.toDev = append(a.toDev, buf[:p.Encode(buf)])
	}
	return nil
}

func (a *air) transmissions() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.tx
}

// devRadio is the device's radio on the simulated air.
type devRadio struct {
	meshcore.RadioBase
	air *air
}

func (r *devRadio) RecvRaw(buf []byte) int {
	r.air.mu.Lock()
	defer r.air.mu.Unlock()
	if len(r.air.toDev) == 0 {
		return 0
	}
	n := copy(buf, r.air.toDev[0])
	r.air.toDev = r.air.toDev[1:]
	return n
}

func (r *devRadio) StartSendRaw(b []byte) bool {
	r.air.uplink(append([]byte(nil), b...))
	return true
}

func (*devRadio) EstAirtimeFor(int) uint32         { return 1 }
func (*devRadio) PacketScore(float32, int) float32 { return 1 }
func (*devRadio) IsSendComplete() bool             { return true }
func (*devRadio) OnSendFinished()                  {}
func (*devRadio) IsInRecvMode() bool               { return true }

type randRNG struct{}

func (randRNG) Random(dst []byte) { rand.Read(dst) }

const (
	gw1 = "1111000000000000000000000000000000000000000000000000000000000000"
	gw2 = "2222000000000000000000000000000000000000000000000000000000000000"
)

func setupDevice(t *testing.T, opts ...func(*node.Config)) (*Hub, *store.Store, *air, *node.Node, *node.MemStorage) {
	t.Helper()
	old := collectWindow
	collectWindow = 30 * time.Millisecond
	t.Cleanup(func() { collectWindow = old })

	h, st, _ := newHub(t)
	a := &air{t: t, ctx: context.Background()}
	h.pub = a
	a.in = ingest.New(st, quiet)
	a.in.Self = h.PublicKey()
	a.in.Hub = h

	hubID, _ := meshcore.IdentityFromHex(h.PublicKey())
	ms := meshcore.NewSystemMillis()
	storage := &node.MemStorage{}
	cfg := node.Config{
		Radio: &devRadio{air: a}, Millis: ms, RNG: randRNG{}, RTC: meshcore.NewVolatileRTCClock(ms),
		Storage: storage, Hub: hubID, Program: "hydrant", Firmware: "0.1", ReplyTimeout: time.Second,
		StartDelay: -1, RetryBackoff: -1, Retries: -1,
	}
	for _, o := range opts {
		o(&cfg)
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return h, st, a, n, storage
}

// recorder is a Notifier that remembers what it heard.
type recorder struct {
	mu    sync.Mutex
	heard []string
}

func (r *recorder) note(s string) {
	r.mu.Lock()
	r.heard = append(r.heard, s)
	r.mu.Unlock()
}

func (r *recorder) DeviceChanged(string)          { r.note("device") }
func (r *recorder) DeviceDeleted(store.Device)    { r.note("deleted") }
func (r *recorder) EventsAdded()                  { r.note("events") }
func (r *recorder) CommandChanged(string, uint32) { r.note("command") }
func (r *recorder) count(s string) (n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, h := range r.heard {
		if h == s {
			n++
		}
	}
	return n
}

func TestDevicePairingAndConfig(t *testing.T) {
	h, st, a, n, _ := setupDevice(t)
	rec := &recorder{}
	h.Notify = rec
	ctx := context.Background()
	a.setLinks(link{gateway: gw1, repeaters: []byte{0xAA}, snr: 5})

	// Pairing waits for approval.
	if err := n.Sync(ctx); !errors.Is(err, node.ErrPending) {
		t.Fatalf("first Sync = %v, want ErrPending", err)
	}
	dev, ok, _ := st.Device(ctx, n.PublicKey())
	if !ok || dev.Status != store.DevicePending || dev.Program != "hydrant" {
		t.Fatalf("device = %+v, %v", dev, ok)
	}
	if err := h.SetDeviceStatus(ctx, n.PublicKey(), store.DevicePaired); err != nil {
		t.Fatal(err)
	}
	if err := n.Sync(ctx); err != nil {
		t.Fatalf("Sync after approval: %v", err)
	}
	if rec.count("device") < 4 { // join, approval, join, hello
		t.Errorf("notifications = %v", rec.heard)
	}
	if !n.Paired() || n.ConfigVersion() != 0 {
		t.Errorf("paired %v, config version %d", n.Paired(), n.ConfigVersion())
	}

	// A config larger than one chunk arrives in pieces.
	cfg := fmt.Sprintf(`{"audio":{"threshold":42,"bands":[200,2000]},"name":"hydrant 7","notes":%q}`, strings.Repeat("x", 200))
	version, err := h.SetConfig(ctx, n.PublicKey(), []byte(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Sync(ctx); err != nil {
		t.Fatalf("Sync with new config: %v", err)
	}
	if n.ConfigVersion() != version || n.Config().Get("audio.threshold").Int(0) != 42 ||
		len(n.Config().Get("notes").String("")) != 200 {
		t.Errorf("config v%d = %v", n.ConfigVersion(), n.Config().Any())
	}

	// Events are stored and acknowledged.
	data := hubproto.Map()
	data.Set("duration_s", hubproto.Int(93))
	if err := n.Report("flow", data); err != nil {
		t.Fatal(err)
	}
	if err := n.Sync(ctx); err != nil {
		t.Fatalf("Sync with event: %v", err)
	}
	if n.Pending() != 0 {
		t.Errorf("%d events still pending", n.Pending())
	}
	events, _ := st.Events(ctx, n.PublicKey(), 10)
	if len(events) != 1 || events[0].Kind != "flow" || events[0].Data != `{"duration_s":93}` {
		t.Errorf("events = %+v", events)
	}
	if rec.count("events") != 1 {
		t.Errorf("notifications = %v", rec.heard)
	}
	dev, _, _ = st.Device(ctx, n.PublicKey())
	if dev.ConfigVersion != version || dev.WantVersion != version {
		t.Errorf("device reports config v%d, hub wants v%d; want both %d", dev.ConfigVersion, dev.WantVersion, version)
	}
}

func TestDeviceRouting(t *testing.T) {
	h, st, a, n, _ := setupDevice(t)
	h.AutoPair = true
	ctx := context.Background()

	// Two gateways hear the device: gw1 through a repeater, gw2 directly.
	// The hub must answer through gw2.
	a.setLinks(link{gateway: gw1, repeaters: []byte{0xAA}, snr: 9}, link{gateway: gw2, snr: 2})
	if err := n.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	r, ok, _ := st.Route(ctx, n.PublicKey())
	if !ok || r.Gateway != gw2 || r.PathLen != 0 {
		t.Errorf("route = %+v, want gw2 with no repeaters", r)
	}

	// Only the repeater path is left. The device's direct request (zero
	// hop) goes unanswered, so it floods and learns the new route.
	a.setLinks(link{gateway: gw1, repeaters: []byte{0xAA}, snr: 9})
	if err := n.Sync(ctx); err != nil {
		t.Fatalf("Sync after the route changed: %v", err)
	}
	r, _, _ = st.Route(ctx, n.PublicKey())
	if r.Gateway != gw1 || !bytes.Equal(r.Path, []byte{0xAA}) {
		t.Errorf("route = %+v, want gw1 via AA", r)
	}

	// The next check-in goes direct along it.
	before := a.transmissions()
	if err := n.Sync(ctx); err != nil {
		t.Fatalf("direct Sync: %v", err)
	}
	if got := a.transmissions() - before; got != 1 {
		t.Errorf("hub transmitted %d packets for one direct hello, want 1", got)
	}

	// With two repeaters the reply must visit them in reverse order.
	a.setLinks(link{gateway: gw1, repeaters: []byte{0xAA, 0xBB}, snr: 9})
	if err := n.Sync(ctx); err != nil { // the stale direct route fails, then a flood
		t.Fatalf("Sync over two repeaters: %v", err)
	}
	if err := n.Sync(ctx); err != nil { // direct along AA, BB
		t.Fatalf("direct Sync over two repeaters: %v", err)
	}
	r, _, _ = st.Route(ctx, n.PublicKey())
	if !bytes.Equal(r.Path, []byte{0xAA, 0xBB}) {
		t.Errorf("route path = %X, want AABB", r.Path)
	}
}

func TestDeviceStatePersists(t *testing.T) {
	h, _, a, _, _ := setupDevice(t)
	h.AutoPair = true
	ctx := context.Background()
	a.setLinks(link{gateway: gw1, snr: 5})

	// The device keeps its state in flash, as on hardware (nRF52: 4-byte
	// writes, 4 KiB erase blocks).
	flash := flashstore.NewMemDevice(64*1024, 4, 4096)
	boot := func() *node.Node {
		t.Helper()
		storage, err := flashstore.Open(flash, 2)
		if err != nil {
			t.Fatal(err)
		}
		hubID, _ := meshcore.IdentityFromHex(h.PublicKey())
		ms := meshcore.NewSystemMillis()
		n, err := node.New(node.Config{
			Radio: &devRadio{air: a}, Millis: ms, RNG: randRNG{}, RTC: meshcore.NewVolatileRTCClock(ms),
			Storage: storage, Hub: hubID, Program: "hydrant", ReplyTimeout: time.Second,
			StartDelay: -1, RetryBackoff: -1, Retries: -1,
		})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	n := boot()
	if _, err := h.SetConfig(ctx, n.PublicKey(), []byte(`{"interval_s":600}`)); err == nil {
		t.Fatal("config accepted for an unknown device")
	}
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SetConfig(ctx, n.PublicKey(), []byte(`{"interval_s":600}`)); err != nil {
		t.Fatal(err)
	}
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Many check-ins use up several request-number reservations, each
	// saved to flash.
	for i := 0; i < 40; i++ {
		if err := n.Sync(ctx); err != nil {
			t.Fatalf("check-in %d: %v", i, err)
		}
	}

	// A reboot keeps the identity, pairing, config and route, and its
	// requests are still accepted.
	n2 := boot()
	if n2.PublicKey() != n.PublicKey() || !n2.Paired() || n2.Config().Get("interval_s").Int(0) != 600 {
		t.Fatalf("after reboot: key match %v, paired %v, config %v", n2.PublicKey() == n.PublicKey(), n2.Paired(), n2.Config().Any())
	}
	if err := n2.Sync(ctx); err != nil {
		t.Fatalf("Sync after reboot: %v", err)
	}
}

func TestDeviceCommands(t *testing.T) {
	h, st, a, n, _ := setupDevice(t, func(c *node.Config) {
		c.Command = func(name string, args hubproto.Value) (hubproto.Value, error) {
			switch name {
			case "read":
				v := hubproto.Map()
				v.Set("level", hubproto.Int(7*args.Get("n").Int(1)))
				return v, nil
			case "fail":
				return hubproto.Null(), errors.New("sensor offline")
			}
			return hubproto.Null(), node.ErrUnknownCommand
		}
		c.LogSize = node.MaxLogSize
	})
	h.AutoPair = true
	ctx := context.Background()
	a.setLinks(link{gateway: gw1, snr: 5})
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// A log larger than one datagram comes back in fragments.
	for i := 0; i < 40; i++ {
		n.Log(fmt.Sprintf("line %02d %s", i, strings.Repeat("x", 30)))
	}
	ids := map[string]uint32{}
	for _, c := range []struct{ name, args string }{{"read", `{"n":3}`}, {"fail", ""}, {"bogus", ""}, {"logs", `{"clear":true}`}} {
		id, err := h.QueueCommand(ctx, n.PublicKey(), c.name, []byte(c.args))
		if err != nil {
			t.Fatalf("queue %s: %v", c.name, err)
		}
		ids[c.name] = id
	}
	cancelled, _ := h.QueueCommand(ctx, n.PublicKey(), "read", nil)
	if err := h.CancelCommand(ctx, n.PublicKey(), cancelled); err != nil {
		t.Fatal(err)
	}
	if err := n.Sync(ctx); err != nil {
		t.Fatalf("Sync with commands: %v", err)
	}

	cmds, _ := st.Commands(ctx, n.PublicKey(), 10)
	got := map[uint32]store.Command{}
	for _, c := range cmds {
		got[c.ID] = c
	}
	for name, want := range map[string]struct{ status, result string }{
		"read":  {store.CommandOK, `{"level":21}`},
		"fail":  {store.CommandFailed, `"sensor offline"`},
		"bogus": {store.CommandUnknown, `null`},
	} {
		if c := got[ids[name]]; c.Status != want.status || c.Result != want.result {
			t.Errorf("%s: %s %s, want %s %s", name, c.Status, c.Result, want.status, want.result)
		}
	}
	logs := got[ids["logs"]]
	if logs.Status != store.CommandOK || len(logs.Result) < hubproto.MaxPlaintext || !strings.Contains(logs.Result, "line 39") {
		t.Errorf("logs: %s, %d bytes", logs.Status, len(logs.Result))
	}
	if n.Logs() != "" {
		t.Errorf("log not cleared: %q", n.Logs())
	}
	if got[cancelled].Status != store.CommandCancelled {
		t.Errorf("cancelled command: %s", got[cancelled].Status)
	}
	if left, _ := st.PendingCommands(ctx, n.PublicKey()); left != 0 {
		t.Errorf("%d commands still pending", left)
	}

	if _, err := h.QueueCommand(ctx, n.PublicKey(), "", nil); err == nil {
		t.Error("empty command name accepted")
	}
	if _, err := h.QueueCommand(ctx, n.PublicKey(), "x", []byte(fmt.Sprintf(`{"a":%q}`, strings.Repeat("x", 200)))); err == nil {
		t.Error("command too large for a datagram accepted")
	}
}

func TestDeviceConfigValidation(t *testing.T) {
	h, st, a, n, _ := setupDevice(t, func(c *node.Config) {
		c.Validate = func(cfg hubproto.Value) error {
			if cfg.Get("interval_s").Int(1) <= 0 {
				return errors.New("interval_s must be positive")
			}
			return nil
		}
	})
	h.AutoPair = true
	ctx := context.Background()
	a.setLinks(link{gateway: gw1, snr: 5})

	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	good, _ := h.SetConfig(ctx, n.PublicKey(), []byte(`{"interval_s":600}`))
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	bad, _ := h.SetConfig(ctx, n.PublicKey(), []byte(`{"interval_s":0}`))
	if err := n.Sync(ctx); err != nil {
		t.Fatalf("Sync with a bad config: %v", err)
	}
	if n.ConfigVersion() != good || n.Config().Get("interval_s").Int(0) != 600 {
		t.Errorf("node has config v%d %v, want the good one", n.ConfigVersion(), n.Config().Any())
	}
	dev, _, _ := st.Device(ctx, n.PublicKey())
	if dev.RejectedVersion != bad || dev.ConfigError != "interval_s must be positive" || dev.ConfigVersion != good {
		t.Errorf("device = %+v", dev)
	}

	// The rejected config is not fetched again.
	before := a.transmissions()
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got := a.transmissions() - before; got != 1 {
		t.Errorf("hub sent %d packets for a check-in with a rejected config, want 1", got)
	}

	fixed, _ := h.SetConfig(ctx, n.PublicKey(), []byte(`{"interval_s":60}`))
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if dev, _, _ := st.Device(ctx, n.PublicKey()); n.ConfigVersion() != fixed || dev.ConfigVersion != fixed {
		t.Errorf("node v%d, hub records v%d, want %d", n.ConfigVersion(), dev.ConfigVersion, fixed)
	}
}

func TestDeviceEvents(t *testing.T) {
	h, st, a, n, _ := setupDevice(t)
	h.AutoPair = true
	ctx := context.Background()
	a.setLinks(link{gateway: gw1, snr: 5})

	// Events reported before the first check-in are stamped once the hub
	// sets the clock.
	data := hubproto.Map()
	data.Set("n", hubproto.Int(0))
	n.Report("early", data)
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Many small events go in few messages; a large one in fragments.
	for i := 1; i <= 10; i++ {
		d := hubproto.Map()
		d.Set("n", hubproto.Int(int64(i)))
		n.Report("small", d)
	}
	big := hubproto.Map()
	big.Set("samples", hubproto.String(strings.Repeat("0123456789", 50)))
	if err := n.Report("big", big); err != nil {
		t.Fatal(err)
	}
	huge := hubproto.Map()
	huge.Set("samples", hubproto.String(strings.Repeat("x", hubproto.MaxMessage)))
	if err := n.Report("huge", huge); !errors.Is(err, node.ErrTooLarge) {
		t.Errorf("Report of a huge event = %v, want ErrTooLarge", err)
	}
	before := a.transmissions()
	if err := n.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// 10 small events fit two datagrams; 500 bytes need four fragments.
	if got := a.transmissions() - before; got > 6 {
		t.Errorf("hub sent %d replies for 11 events, want at most 6", got)
	}
	events, _ := st.Events(ctx, n.PublicKey(), 20)
	if len(events) != 12 {
		t.Fatalf("%d events stored, want 12", len(events))
	}
	for _, e := range events {
		if e.DeviceTime.IsZero() || time.Since(e.DeviceTime) > time.Minute {
			t.Errorf("event %d %s: device time %v", e.Seq, e.Kind, e.DeviceTime)
		}
		if e.Kind == "big" && !strings.Contains(e.Data, "0123456789") {
			t.Errorf("big event = %s", e.Data)
		}
	}
}

func TestShutdownAnswersPending(t *testing.T) {
	h, _, a, n, _ := setupDevice(t)
	ctx := context.Background()
	a.setLinks(link{gateway: gw1, snr: 5})
	if err := n.Sync(ctx); !errors.Is(err, node.ErrPending) {
		t.Fatalf("first Sync = %v", err)
	}
	if err := h.SetDeviceStatus(ctx, n.PublicKey(), store.DevicePaired); err != nil {
		t.Fatal(err)
	}
	if err := n.Sync(ctx); err != nil {
		t.Fatalf("Sync after approval: %v", err)
	}

	// A check-in heard before the shutdown is still answered.
	collectWindow = 300 * time.Millisecond
	synced := make(chan error, 1)
	go func() { synced <- n.Sync(ctx) }()
	for deadline := time.Now().Add(time.Second); h.inFlight.Load() == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the check-in never reached the hub")
		}
	}
	shutdown, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := h.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
	if err := <-synced; err != nil {
		t.Errorf("Sync during shutdown: %v", err)
	}

	// Later ones are not.
	if err := n.Sync(ctx); err == nil {
		t.Error("Sync after shutdown succeeded")
	}
}
