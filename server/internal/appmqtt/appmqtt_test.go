package appmqtt

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gapidobri/meshub/proto/appproto"
	"github.com/gapidobri/meshub/server/internal/store"
)

const pk = "ABCD000000000000000000000000000000000000000000000000000000000000"

type message struct {
	topic   string
	qos     byte
	retain  bool
	payload string
}

type fakePub struct {
	mu        sync.Mutex
	connected bool
	failAfter int // fail publishes once this many more succeeded; -1 never
	msgs      []message
}

func (p *fakePub) PublishMessage(topic string, qos byte, retain bool, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failAfter == 0 {
		return errors.New("broker gone")
	}
	p.failAfter--
	p.msgs = append(p.msgs, message{topic, qos, retain, string(payload)})
	return nil
}

func (p *fakePub) Connected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.connected
}

func (p *fakePub) take() []message {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := p.msgs
	p.msgs = nil
	return m
}

func setup(t *testing.T) (*App, *fakePub, *store.Store) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if _, err := st.Join(context.Background(), pk, "weather/v2", "1", "", store.DevicePaired, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	pub := &fakePub{connected: true, failAfter: -1}
	return New(st, pub, slog.New(slog.NewTextHandler(io.Discard, nil))), pub, st
}

func addEvents(t *testing.T, st *store.Store, seqs ...uint32) {
	t.Helper()
	for _, seq := range seqs {
		if err := st.AddEvent(context.Background(), pk, store.Event{Seq: seq, Kind: "open", Data: `{"flow":true}`, ReceivedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEvents(t *testing.T) {
	a, pub, st := setup(t)
	ctx := context.Background()
	addEvents(t, st, 1, 2) // stored before the app started: backfilled through the API

	cursor, err := a.loadCursor(ctx)
	if err != nil || cursor != 2 {
		t.Fatalf("first cursor = %d, %v", cursor, err)
	}
	addEvents(t, st, 3, 4, 5)
	pub.failAfter = 2
	cursor, _, err = a.publishEvents(ctx, cursor)
	if err == nil || cursor != 4 {
		t.Fatalf("publish with broker failing = %d, %v; want cursor 4 and an error", cursor, err)
	}
	msgs := pub.take()
	if len(msgs) != 2 || msgs[0].topic != "meshub/app/v1/weather_v2/devices/"+pk+"/events" || msgs[0].qos != 1 || msgs[0].retain {
		t.Fatalf("messages = %+v", msgs)
	}
	var e appproto.Event
	if err := json.Unmarshal([]byte(msgs[0].payload), &e); err != nil || e.ID != 3 || e.Seq != 3 || string(e.Data) != `{"flow":true}` || e.Program != "weather/v2" {
		t.Errorf("event = %+v, %v", e, err)
	}

	// After a restart, publishing resumes after the last acknowledged event.
	pub.failAfter = -1
	b := New(st, pub, a.log)
	cursor, err = b.loadCursor(ctx)
	if err != nil || cursor != 4 {
		t.Fatalf("cursor after restart = %d, %v", cursor, err)
	}
	if cursor, more, err := b.publishEvents(ctx, cursor); err != nil || more || cursor != 5 {
		t.Errorf("publish rest = %d %v %v", cursor, more, err)
	}
	if msgs := pub.take(); len(msgs) != 1 || msgs[0].payload == "" {
		t.Errorf("messages = %+v", msgs)
	}
}

func TestRun(t *testing.T) {
	a, pub, st := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	time.Sleep(20 * time.Millisecond) // let it load its cursor
	addEvents(t, st, 1)
	a.EventsAdded()
	a.DeviceChanged(pk)
	deadline := time.Now().Add(2 * time.Second)
	var got []message
	for len(got) < 2 && time.Now().Before(deadline) {
		got = append(got, pub.take()...)
		time.Sleep(5 * time.Millisecond)
	}
	if len(got) != 2 {
		t.Fatalf("messages = %+v", got)
	}
}

func TestStatus(t *testing.T) {
	a, pub, st := setup(t)
	ctx := context.Background()
	topic := "meshub/app/v1/weather_v2/devices/" + pk + "/status"

	if err := a.apply(ctx, update{kind: updateStatus, device: pk}); err != nil {
		t.Fatal(err)
	}
	msgs := pub.take()
	if len(msgs) != 1 || msgs[0].topic != topic || !msgs[0].retain {
		t.Fatalf("status = %+v", msgs)
	}
	var d appproto.Device
	if err := json.Unmarshal([]byte(msgs[0].payload), &d); err != nil || d.PubKey != pk || d.Status != appproto.DevicePaired {
		t.Errorf("status = %+v, %v", d, err)
	}

	// A device deleted while the broker is unreachable is cleared once it
	// is back, before the remaining statuses are republished.
	dev, _, _ := st.Device(ctx, pk)
	if err := st.DeleteDevice(ctx, pk); err != nil {
		t.Fatal(err)
	}
	pub.failAfter = 0
	a.DeviceDeleted(dev)
	if err := a.apply(ctx, <-a.updates); err == nil {
		t.Fatal("clear succeeded with the broker failing")
	}
	other := "EEEE000000000000000000000000000000000000000000000000000000000000"
	st.Join(ctx, other, "meter", "1", "", store.DevicePending, 0, time.Now())
	pub.failAfter = -1
	a.Resync()
	if err := a.apply(ctx, <-a.updates); err != nil {
		t.Fatal(err)
	}
	msgs = pub.take()
	if len(msgs) != 2 || msgs[0].topic != topic || msgs[0].payload != "" || !msgs[0].retain ||
		msgs[1].topic != "meshub/app/v1/meter/devices/"+other+"/status" {
		t.Errorf("resync = %+v", msgs)
	}
	if len(a.clears) != 0 {
		t.Errorf("clears left = %v", a.clears)
	}
}

func TestCommand(t *testing.T) {
	a, pub, st := setup(t)
	ctx := context.Background()
	id, err := st.AddCommand(ctx, pk, "status", "null", []byte{0xF6}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	st.FinishCommand(ctx, pk, id, store.CommandOK, `{"battery_mv":3700}`, time.Now())
	if err := a.apply(ctx, update{kind: updateCommand, device: pk, command: id}); err != nil {
		t.Fatal(err)
	}
	msgs := pub.take()
	var c appproto.Command
	if len(msgs) != 1 || msgs[0].topic != "meshub/app/v1/weather_v2/devices/"+pk+"/commands" ||
		json.Unmarshal([]byte(msgs[0].payload), &c) != nil || c.Status != appproto.CommandOK || string(c.Result) != `{"battery_mv":3700}` {
		t.Errorf("command = %+v", msgs)
	}
}
