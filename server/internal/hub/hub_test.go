package hub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/proto/gwproto"
	"github.com/gapidobri/meshub/server/internal/store"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type published struct {
	topic   string
	payload []byte
}

type fakePublisher struct {
	mu   sync.Mutex
	msgs []published
}

func (f *fakePublisher) Publish(topic string, payload []byte) error {
	f.mu.Lock()
	f.msgs = append(f.msgs, published{topic, payload})
	f.mu.Unlock()
	return nil
}

func newHub(t *testing.T) (*Hub, *store.Store, *fakePublisher) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	id, err := LoadIdentity(context.Background(), st)
	if err != nil {
		t.Fatal(err)
	}
	pub := &fakePublisher{}
	return New(id, "test hub", pub, st, quiet), st, pub
}

func addGateway(t *testing.T, st *store.Store, pubKey, status string, at time.Time) {
	t.Helper()
	err := st.UpsertGateway(context.Background(), store.Gateway{PubKey: pubKey, IATA: "LJU", Name: pubKey[:4], Status: status, Stats: "{}", LastStatusAt: at})
	if err != nil {
		t.Fatal(err)
	}
}

type advertCatcher struct {
	meshcore.BaseHandler
	id   meshcore.Identity
	name string
	got  bool
}

func (c *advertCatcher) OnAdvertRecv(p *meshcore.Packet, id *meshcore.Identity, ts uint32, app []byte) {
	ad := meshcore.ParseAdvertData(app)
	c.id, c.name, c.got = *id, ad.Name(), true
}

type seqRNG struct{ n byte }

func (s *seqRNG) Random(dst []byte) {
	for i := range dst {
		s.n++
		dst[i] = s.n
	}
}

// TestAdvertAcceptedByMesh checks the hub's advert against meshcore-go's
// receive path, which is byte-compatible with the C++ firmware.
func TestAdvertAcceptedByMesh(t *testing.T) {
	h, _, _ := newHub(t)
	radio := &feedRadio{frame: h.advert()}
	catcher := &advertCatcher{}
	ms := meshcore.NewSystemMillis()
	m := meshcore.NewMesh(meshcore.Config{
		Radio: radio, Millis: ms, RNG: &seqRNG{}, RTC: meshcore.NewVolatileRTCClock(ms),
		Packets: meshcore.NewStaticPoolPacketManager(4), Tables: &meshcore.SimpleMeshTables{}, Handler: catcher,
	})
	m.SelfID = meshcore.NewLocalIdentity(&seqRNG{n: 100})
	m.Begin()
	m.Loop()
	if !catcher.got {
		t.Fatal("mesh did not accept the hub advert")
	}
	if catcher.id.String() != h.PublicKey() || catcher.name != "test hub" {
		t.Errorf("advert from %s named %q, want %s %q", catcher.id.String(), catcher.name, h.PublicKey(), "test hub")
	}
}

func TestSendAdvert(t *testing.T) {
	h, st, pub := newHub(t)
	ctx := context.Background()
	if _, err := h.SendAdvert(ctx); !errors.Is(err, ErrNoGateway) {
		t.Fatalf("SendAdvert without gateways: err = %v, want ErrNoGateway", err)
	}

	now := time.Now()
	gws := []string{
		"AAAA000000000000000000000000000000000000000000000000000000000000",
		"BBBB000000000000000000000000000000000000000000000000000000000000",
	}
	addGateway(t, st, gws[0], gwproto.StatusOnline, now)
	addGateway(t, st, gws[1], gwproto.StatusOnline, now)
	addGateway(t, st, "CCCC000000000000000000000000000000000000000000000000000000000000", gwproto.StatusOffline, now)
	addGateway(t, st, "DDDD000000000000000000000000000000000000000000000000000000000000", gwproto.StatusOnline, now.Add(-time.Hour)) // stale

	n, err := h.SendAdvert(ctx)
	if err != nil || n != 2 {
		t.Fatalf("SendAdvert = %d, %v; want 2 gateways", n, err)
	}
	if len(pub.msgs) != 2 {
		t.Fatalf("published %d messages, want 2", len(pub.msgs))
	}
	var ids []uint32
	for i, m := range pub.msgs {
		var req gwproto.TxRequest
		if err := gwproto.ParseTxRequest(&req, m.payload); err != nil {
			t.Fatal(err)
		}
		if m.topic != gwproto.TxTopic(gws[i]) {
			t.Errorf("request %d went to %s", i, m.topic)
		}
		if want := uint16(i) * uint16(advertStagger/time.Millisecond); req.DelayMs != want {
			t.Errorf("request %d delay = %dms, want %dms", i, req.DelayMs, want)
		}
		ids = append(ids, req.ID)
	}
	if ids[0] == ids[1] {
		t.Error("requests share an ID")
	}

	// Results for our requests are recorded; unknown IDs are ignored.
	h.handleTxResult("meshub/gw/x/tx/result", []byte(fmt.Sprintf(`{"id":%d,"status":"sent"}`, ids[0])))
	h.handleTxResult("meshub/gw/x/tx/result", []byte(fmt.Sprintf(`{"id":%d,"status":"full"}`, ids[1])))
	h.handleTxResult("meshub/gw/x/tx/result", []byte(`{"id":1,"status":"sent"}`))
	s := h.Status()
	if len(s.Results) != 2 || s.Results[0].Status != gwproto.TxFull || s.Results[0].Gateway != gws[1] ||
		s.Results[1].Status != gwproto.TxSent || s.Results[1].What != "advert" {
		t.Errorf("results = %+v", s.Results)
	}
	if s.LastAdvert.IsZero() {
		t.Error("last advert not recorded")
	}
}

func TestIdentityPersists(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	a, err := LoadIdentity(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadIdentity(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	if a.String() != b.String() {
		t.Error("identity changed between loads")
	}
}
