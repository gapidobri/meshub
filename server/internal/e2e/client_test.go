// Package e2e tests the backend as applications use it: through package
// client, over HTTP and a real MQTT broker.
package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/gapidobri/meshub/client"
	"github.com/gapidobri/meshub/proto/appproto"
	"github.com/gapidobri/meshub/server/internal/api"
	"github.com/gapidobri/meshub/server/internal/appmqtt"
	"github.com/gapidobri/meshub/server/internal/hub"
	"github.com/gapidobri/meshub/server/internal/mqttconn"
	"github.com/gapidobri/meshub/server/internal/store"
)

const (
	token = "0123456789abcdef"
	pk    = "ABCD000000000000000000000000000000000000000000000000000000000000"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// connRef publishes through a connection made after it.
type connRef struct{ atomic.Pointer[mqttconn.Conn] }

func (r *connRef) Publish(topic string, payload []byte) error {
	return r.PublishMessage(topic, 0, false, payload)
}

func (r *connRef) PublishMessage(topic string, qos byte, retain bool, payload []byte) error {
	if c := r.Load(); c != nil {
		return c.PublishMessage(topic, qos, retain, payload)
	}
	return errors.New("not connected")
}

func (r *connRef) Connected() bool { c := r.Load(); return c != nil && c.Connected() }

type backend struct {
	st     *store.Store
	hub    *hub.Hub
	app    *appmqtt.App
	url    string // HTTP
	broker string
	mqtt   *mochi.Server
}

func start(t *testing.T) *backend {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	broker := mochi.New(&mochi.Options{InlineClient: true, Logger: quiet})
	broker.AddHook(new(auth.AllowHook), nil)
	if err := broker.AddListener(listeners.NewTCP(listeners.Config{ID: "t", Address: addr})); err != nil {
		t.Fatal(err)
	}
	go broker.Serve()
	t.Cleanup(func() { broker.Close() })

	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	pub := &connRef{}
	h := hub.New(meshcore.LocalIdentityFromSeed(make([]byte, 32)), "test hub", pub, st, quiet)
	app := appmqtt.New(st, pub, quiet)
	h.Notify = app
	conn := mqttconn.Connect(mqttconn.Config{Broker: "tcp://" + addr, ClientID: "backend", OnConnect: app.Resync}, quiet)
	pub.Store(conn)
	t.Cleanup(conn.Close)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { app.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	mux := http.NewServeMux()
	api.Register(mux, st, h, quiet, api.Options{Version: "test", Tokens: []string{token}})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &backend{st: st, hub: h, app: app, url: srv.URL, broker: "tcp://" + addr, mqtt: broker}
}

func (b *backend) addEvent(t *testing.T, seq uint32) {
	t.Helper()
	if err := b.st.AddEvent(context.Background(), pk, store.Event{Seq: seq, Kind: "open", Data: `{"flow":true}`, ReceivedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	b.app.EventsAdded()
}

func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("no %s", what)
		panic("unreachable")
	}
}

func TestHTTP(t *testing.T) {
	b := start(t)
	ctx := context.Background()
	c := client.New(b.url, client.WithToken(token))
	if _, err := b.st.Join(ctx, pk, "weather", "1.0", "XY12", store.DevicePending, 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	if _, err := client.New(b.url, client.WithToken("wrong")).Devices(ctx, client.DeviceFilter{}); !errors.Is(err, client.ErrUnauthorized) {
		t.Errorf("wrong token: %v", err)
	}
	if _, err := c.Device(ctx, "EEEE000000000000000000000000000000000000000000000000000000000000"); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("unknown device: %v", err)
	}
	pending, err := c.Devices(ctx, client.DeviceFilter{Program: "weather", Status: appproto.DevicePending})
	if err != nil || len(pending) != 1 || pending[0].Claim != "XY12" {
		t.Fatalf("pending = %+v, %v", pending, err)
	}
	if d, err := c.ApproveDevice(ctx, pk); err != nil || d.Status != appproto.DevicePaired {
		t.Errorf("approve = %+v, %v", d, err)
	}

	cfg, err := c.SetConfig(ctx, pk, map[string]any{"threshold": 0.4, "interval_s": 300})
	if err != nil || cfg.Version != 1 {
		t.Errorf("set config = %+v, %v", cfg, err)
	}
	if _, err := c.SetConfig(ctx, pk, []int{1}); !errors.Is(err, client.ErrInvalid) {
		t.Errorf("array config: %v", err)
	}

	cmd, err := c.QueueCommand(ctx, pk, "selftest", map[string]int{"seconds": 5})
	if err != nil || cmd.Status != appproto.CommandPending || string(cmd.Args) != `{"seconds":5}` {
		t.Fatalf("queue = %+v, %v", cmd, err)
	}
	if cmd, err = c.CancelCommand(ctx, pk, cmd.ID); err != nil || cmd.Status != appproto.CommandCancelled {
		t.Errorf("cancel = %+v, %v", cmd, err)
	}
	if _, err = c.CancelCommand(ctx, pk, cmd.ID); !errors.Is(err, client.ErrConflict) {
		t.Errorf("cancel again: %v", err)
	}

	for seq := range uint32(5) {
		b.addEvent(t, seq)
	}
	var ids []int64
	for e, err := range c.EventsAfter(ctx, "weather", 2) {
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, e.ID)
	}
	if len(ids) != 3 || ids[0] != 3 {
		t.Errorf("events after 2 = %v", ids)
	}
	if latest, err := c.Events(ctx, client.EventQuery{Device: pk, Desc: true, Limit: 1}); err != nil || len(latest) != 1 || latest[0].ID != 5 {
		t.Errorf("latest = %+v, %v", latest, err)
	}

	if err := c.DeleteDevice(ctx, pk); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Device(ctx, pk); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("deleted device: %v", err)
	}
}

func TestListen(t *testing.T) {
	b := start(t)
	ctx := context.Background()
	c := client.New(b.url, client.WithToken(token))
	if _, err := b.st.Join(ctx, pk, "weather", "1.0", "", store.DevicePending, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Stored before Listen: these come from the backfill, and again over
	// MQTT once the backend publishes them, which Listen must drop.
	b.addEvent(t, 1)
	b.addEvent(t, 2)

	events := make(chan appproto.Event, 10)
	statuses := make(chan appproto.Device, 10)
	deleted := make(chan string, 10)
	commands := make(chan appproto.Command, 10)
	lctx, cancel := context.WithCancel(ctx)
	listened := make(chan error, 1)
	go func() {
		listened <- c.Listen(lctx, client.Subscription{
			Broker:  b.broker,
			Program: "weather",
			OnEvent: func(_ context.Context, e appproto.Event) error {
				events <- e
				return nil
			},
			OnStatus:  func(d appproto.Device) { statuses <- d },
			OnDeleted: func(pk string) { deleted <- pk },
			OnCommand: func(c appproto.Command) { commands <- c },
			Logger:    quiet,
		})
	}()

	for want := int64(1); want <= 2; want++ {
		if e := receive(t, events, "backfilled event"); e.ID != want || e.Device != pk {
			t.Fatalf("event = %+v, want ID %d", e, want)
		}
	}

	if _, err := c.ApproveDevice(ctx, pk); err != nil {
		t.Fatal(err)
	}
	for d := receive(t, statuses, "status"); d.Status != appproto.DevicePaired; d = receive(t, statuses, "paired status") {
	}

	b.addEvent(t, 3)
	if e := receive(t, events, "live event"); e.ID != 3 || string(e.Data) != `{"flow":true}` {
		t.Fatalf("live event = %+v", e)
	}

	if _, err := c.QueueCommand(ctx, pk, "selftest", nil); err != nil {
		t.Fatal(err)
	}
	if cmd := receive(t, commands, "command"); cmd.Name != "selftest" || cmd.Status != appproto.CommandPending {
		t.Errorf("command = %+v", cmd)
	}

	if err := c.DeleteDevice(ctx, pk); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, deleted, "deletion"); got != pk {
		t.Errorf("deleted %q", got)
	}

	cancel()
	if err := <-listened; err != nil {
		t.Errorf("Listen = %v", err)
	}
	select {
	case e := <-events:
		t.Errorf("event %d handled twice", e.ID)
	default:
	}
}

func TestListenHandlerError(t *testing.T) {
	b := start(t)
	ctx := context.Background()
	if _, err := b.st.Join(ctx, pk, "weather", "1.0", "", store.DevicePaired, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	b.addEvent(t, 1)
	b.addEvent(t, 2)

	failed := errors.New("database down")
	var seen []int64
	err := client.New(b.url, client.WithToken(token)).Listen(ctx, client.Subscription{
		Broker: b.broker,
		After:  1, // the application handled event 1 before
		OnEvent: func(_ context.Context, e appproto.Event) error {
			seen = append(seen, e.ID)
			return failed
		},
		Logger: quiet,
	})
	if !errors.Is(err, failed) || len(seen) != 1 || seen[0] != 2 {
		t.Errorf("Listen = %v after events %v; want %v after [2]", err, seen, failed)
	}
}

// subscribeHook runs funcs as the broker handles a client's subscription:
// before it takes effect, and after but before the client is told.
type subscribeHook struct {
	mochi.HookBase
	before, after func(cl *mochi.Client)
}

func (h *subscribeHook) ID() string { return "subscribe" }

func (h *subscribeHook) Provides(b byte) bool {
	return b == mochi.OnSubscribe || b == mochi.OnSubscribed
}

func (h *subscribeHook) OnSubscribe(cl *mochi.Client, pk packets.Packet) packets.Packet {
	h.before(cl)
	return pk
}

func (h *subscribeHook) OnSubscribed(cl *mochi.Client, _ packets.Packet, _ []byte) { h.after(cl) }

func TestListenReconnect(t *testing.T) {
	b := start(t)
	ctx := context.Background()
	if _, err := b.st.Join(ctx, pk, "weather", "1.0", "", store.DevicePaired, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	published := make(chan int64, 10)
	if err := b.mqtt.Subscribe(appproto.DevicesFilter("weather"), 1, func(_ *mochi.Client, _ packets.Subscription, p packets.Packet) {
		if _, _, kind, _ := appproto.ParseTopic(p.TopicName); kind == appproto.KindEvents {
			var e appproto.Event
			json.Unmarshal(p.Payload, &e)
			published <- e.ID
		}
	}); err != nil {
		t.Fatal(err)
	}
	add := func(seq uint32) {
		if err := b.st.AddEvent(ctx, pk, store.Event{Seq: seq, Kind: "open", Data: `{}`, ReceivedAt: time.Now()}); err != nil {
			t.Error(err)
		}
		b.app.EventsAdded()
	}

	// On the app's second subscription, events 2 to 4 are published before
	// it takes effect, so the app misses them, and event 5 reaches it
	// before the subscription is acknowledged.
	var subscribes atomic.Int32
	b.mqtt.AddHook(&subscribeHook{
		before: func(cl *mochi.Client) {
			if cl.ID != "app" || subscribes.Add(1) != 2 {
				return
			}
			for seq := uint32(2); seq <= 4; seq++ {
				add(seq)
			}
			for id := int64(0); id != 4; {
				select {
				case id = <-published:
				case <-time.After(5 * time.Second):
					t.Error("events 2 to 4 not published")
					return
				}
			}
		},
		after: func(cl *mochi.Client) {
			if cl.ID != "app" || subscribes.Load() != 2 {
				return
			}
			add(5)
			time.Sleep(200 * time.Millisecond)
		},
	}, nil)

	events := make(chan appproto.Event, 10)
	lctx, cancel := context.WithCancel(ctx)
	listened := make(chan error, 1)
	go func() {
		listened <- client.New(b.url, client.WithToken(token)).Listen(lctx, client.Subscription{
			Broker:   b.broker,
			ClientID: "app",
			Program:  "weather",
			OnEvent: func(_ context.Context, e appproto.Event) error {
				events <- e
				return nil
			},
			Logger: quiet,
		})
	}()

	add(1)
	if e := receive(t, events, "first event"); e.ID != 1 {
		t.Fatalf("event %d, want 1", e.ID)
	}
	cl, ok := b.mqtt.Clients.Get("app")
	if !ok {
		t.Fatal("app not connected")
	}
	cl.Stop(errors.New("dropped by test"))

	for want := int64(2); want <= 5; want++ {
		if e := receive(t, events, "event after reconnect"); e.ID != want {
			t.Fatalf("event %d, want %d", e.ID, want)
		}
	}
	cancel()
	if err := <-listened; err != nil {
		t.Errorf("Listen = %v", err)
	}
}
