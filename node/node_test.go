package node

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gapidobri/meshub/proto/hubproto"
)

var ctx = context.Background()

func TestSeqIncreasesAcrossReboots(t *testing.T) {
	hub := newFakeHub(t)
	storage := &countingStorage{}
	n := testNode(t, hub, storage)
	for i := 0; i < 20; i++ {
		if err := n.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// A reboot skips the rest of the reserved block, never reusing one.
	n = testNode(t, hub, storage)
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	var last int64 = -1
	for i, r := range hub.reqs {
		if int64(r.seq) <= last {
			t.Fatalf("request %d has seq %d after %d", i, r.seq, last)
		}
		last = int64(r.seq)
	}
	// The counter is saved once per reserved block, not per request.
	if saves, reqs := storage.saves[keyState], len(hub.reqs); saves > reqs/seqReserve+4 {
		t.Errorf("state saved %d times for %d requests", saves, reqs)
	}
}

func TestPairing(t *testing.T) {
	hub := newFakeHub(t)
	hub.joinStatus = hubproto.JoinPending
	n := testNode(t, hub, &MemStorage{})
	if err := n.Sync(ctx); !errors.Is(err, ErrPending) {
		t.Fatalf("Sync = %v, want ErrPending", err)
	}
	hub.joinStatus = hubproto.JoinAccepted
	if err := n.Sync(ctx); err != nil || !n.Paired() {
		t.Fatalf("Sync = %v, paired %v", err, n.Paired())
	}
	if !hub.reqs[0].anon || hub.reqs[0].msg.Type() != hubproto.TypeJoin {
		t.Errorf("first request = %+v, want an anonymous join", hub.reqs[0])
	}

	// A hub that has forgotten the node makes it pair again.
	hub.override = func(m hubproto.Message) hubproto.Message {
		if _, ok := m.(*hubproto.Hello); ok {
			return &hubproto.Error{Code: hubproto.ErrNotPaired}
		}
		return nil
	}
	if err := n.Sync(ctx); !errors.Is(err, ErrNoReply) || n.Paired() {
		t.Errorf("Sync = %v, paired %v; want unpaired", err, n.Paired())
	}
	hub.override = nil
	joins := hub.count(hubproto.TypeJoin)
	if err := n.Sync(ctx); err != nil || hub.count(hubproto.TypeJoin) != joins+1 {
		t.Errorf("Sync = %v after %d joins, want one more join", err, hub.count(hubproto.TypeJoin)-joins)
	}

	hub.joinStatus = hubproto.JoinRejected
	n = testNode(t, hub, &MemStorage{})
	if err := n.Sync(ctx); !errors.Is(err, ErrRejected) {
		t.Errorf("Sync = %v, want ErrRejected", err)
	}
}

func TestRetries(t *testing.T) {
	hub := newFakeHub(t)
	n := testNode(t, hub, &MemStorage{}, func(c *Config) { c.Retries = 2 })
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// The node knows its route now. When a direct request goes unanswered
	// it floods, which is not a retry; two lost floods are retried.
	lost := 0
	hub.drop = func(hubproto.Message) bool { lost++; return lost <= 3 }
	before := len(hub.reqs)
	if err := n.Sync(ctx); err != nil {
		t.Fatalf("Sync with three lost requests: %v", err)
	}
	reqs := hub.reqs[before:]
	if len(reqs) != 4 || !reqs[0].direct || reqs[1].direct || reqs[2].direct || reqs[3].direct {
		t.Errorf("requests: %+v; want one direct, then three floods", reqs)
	}
	for _, r := range reqs {
		if r.msg.Type() != hubproto.TypeHello {
			t.Errorf("resent %T, want the hello", r.msg)
		}
	}
	if !n.st.hasPath {
		t.Error("route not learnt again from the reply")
	}

	// More losses than retries fail the request.
	lost = 0
	hub.drop = func(hubproto.Message) bool { return true }
	before = len(hub.reqs)
	if err := n.Sync(ctx); !errors.Is(err, ErrNoReply) {
		t.Errorf("Sync = %v, want ErrNoReply", err)
	}
	if got := len(hub.reqs) - before; got != 4 {
		t.Errorf("%d requests sent, want 4 (direct, flood and two retries)", got)
	}
}

func TestBackoffAndStartDelay(t *testing.T) {
	hub := newFakeHub(t)
	hub.drop = func(hubproto.Message) bool { return true }
	n := testNode(t, hub, &MemStorage{}, func(c *Config) {
		c.StartDelay = 10 * time.Second
		c.RNG = fixedRNG(0xFF) // a delay of 3.7s
	})
	// The start delay is cancellable, and nothing is sent during it.
	c, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := n.Sync(c); !errors.Is(err, context.DeadlineExceeded) || len(hub.reqs) != 0 {
		t.Errorf("Sync = %v with %d requests sent", err, len(hub.reqs))
	}

	// Backoffs wait between retries, but stay within RetryBackoff doubled
	// per retry.
	n = testNode(t, hub, &MemStorage{}, func(c *Config) {
		c.Retries, c.RetryBackoff = 2, 100*time.Millisecond
		c.ReplyTimeout = 20 * time.Millisecond
	})
	start := time.Now()
	if err := n.Sync(ctx); !errors.Is(err, ErrNoReply) {
		t.Fatalf("Sync = %v", err)
	}
	// Three timeouts of 20ms and backoffs below 200ms and 400ms.
	if took := time.Since(start); took > 60*time.Millisecond+600*time.Millisecond+200*time.Millisecond {
		t.Errorf("three attempts took %v", took)
	}
}

// fixedRNG fills every random byte with itself.
type fixedRNG byte

func (r fixedRNG) Random(dst []byte) {
	for i := range dst {
		dst[i] = byte(r)
	}
}

func TestAdaptiveReplyTimeout(t *testing.T) {
	hub := newFakeHub(t)
	n := testNode(t, hub, &MemStorage{}, func(c *Config) {
		c.ReplyTimeout, c.MinReplyTimeout = 2*time.Second, 50*time.Millisecond
		c.Retries = -1
	})
	if n.floodRTT.timeout(n.cfg.MinReplyTimeout, n.cfg.ReplyTimeout) != 2*time.Second {
		t.Error("timeout before any reply is not the maximum")
	}
	for i := 0; i < 3; i++ {
		if err := n.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// The fake hub answers at once, so the node waits the minimum.
	hub.drop = func(hubproto.Message) bool { return true }
	start := time.Now()
	if err := n.Sync(ctx); !errors.Is(err, ErrNoReply) {
		t.Fatalf("Sync = %v", err)
	}
	// A direct attempt and a flood; the flood estimate has samples from
	// the join and first hello.
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("two lost requests took %v, want about 100ms", took)
	}

	var r rttEstimator
	for i := 0; i < 20; i++ {
		r.sample(time.Second)
	}
	if got := r.timeout(0, time.Minute); got < time.Second || got > 1100*time.Millisecond {
		t.Errorf("steady 1s replies give a timeout of %v", got)
	}
	r.sample(3 * time.Second)
	if got := r.timeout(0, time.Minute); got < 2*time.Second {
		t.Errorf("after a slow reply the timeout is only %v", got)
	}
	if got := r.timeout(0, 1500*time.Millisecond); got != 1500*time.Millisecond {
		t.Errorf("timeout %v above the maximum", got)
	}
}

func TestConfigSync(t *testing.T) {
	hub := newFakeHub(t)
	storage := &MemStorage{}
	validate := func(v hubproto.Value) error {
		if v.Get("interval_s").Int(1) <= 0 {
			return errors.New("interval_s must be positive")
		}
		return nil
	}
	n := testNode(t, hub, storage, func(c *Config) { c.Validate = validate })
	setConfig := func(json string) {
		cfg := hubproto.Map()
		cfg.Set("interval_s", hubproto.Int(0))
		if json == "good" {
			cfg.Set("interval_s", hubproto.Int(600))
			cfg.Set("notes", hubproto.String(strings.Repeat("x", 300)))
		}
		hub.version++
		hub.config = cfg.AppendCBOR(nil)
	}

	setConfig("good")
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if n.ConfigVersion() != 1 || n.Config().Get("interval_s").Int(0) != 600 {
		t.Fatalf("config v%d %v", n.ConfigVersion(), n.Config().Any())
	}
	if len(hub.statuses) != 1 || hub.statuses[0].Status != hubproto.ConfigApplied {
		t.Errorf("statuses = %+v", hub.statuses)
	}

	setConfig("bad")
	gets := hub.count(hubproto.TypeConfigGet)
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if n.ConfigVersion() != 1 {
		t.Errorf("rejected config applied: v%d", n.ConfigVersion())
	}
	if s := hub.statuses[len(hub.statuses)-1]; s.ConfigVersion != 2 || s.Status != hubproto.ConfigRejected || s.Reason != "interval_s must be positive" {
		t.Errorf("status = %+v", s)
	}

	// After a reboot the node neither fetches the rejected config again
	// nor reports it twice.
	gets = hub.count(hubproto.TypeConfigGet) - gets
	n = testNode(t, hub, storage, func(c *Config) { c.Validate = validate })
	before, statuses := hub.count(hubproto.TypeConfigGet), len(hub.statuses)
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if hub.count(hubproto.TypeConfigGet) != before || len(hub.statuses) != statuses {
		t.Errorf("rejected config fetched or reported again")
	}
	if n.ConfigVersion() != 1 || n.Config().Get("interval_s").Int(0) != 600 {
		t.Errorf("after reboot: config v%d %v", n.ConfigVersion(), n.Config().Any())
	}

	// A lost status report is sent at the next check-in.
	setConfig("good")
	hub.drop = func(m hubproto.Message) bool { return m.Type() == hubproto.TypeConfigStatus }
	if err := n.Sync(ctx); !errors.Is(err, ErrNoReply) {
		t.Fatalf("Sync with the status lost = %v", err)
	}
	hub.drop = nil
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if s := hub.statuses[len(hub.statuses)-1]; s.ConfigVersion != 3 || s.Status != hubproto.ConfigApplied {
		t.Errorf("status = %+v", s)
	}
}

func TestEventQueue(t *testing.T) {
	hub := newFakeHub(t)
	storage := &MemStorage{}
	n := testNode(t, hub, storage, func(c *Config) { c.MaxEvents = 5 })
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	hub.drop = func(hubproto.Message) bool { return true }
	for i := 0; i < 8; i++ {
		d := hubproto.Map()
		d.Set("i", hubproto.Int(int64(i)))
		if err := n.Report("tick", d); err != nil {
			t.Fatal(err)
		}
	}
	if n.Pending() != 5 {
		t.Errorf("%d pending, want the newest 5", n.Pending())
	}
	if !strings.Contains(n.Logs(), "dropped") {
		t.Errorf("drop not logged: %q", n.Logs())
	}

	// The queue survives a reboot, and is delivered in one message.
	n = testNode(t, hub, storage, func(c *Config) { c.MaxEvents = 5 })
	hub.drop = nil
	hub.events = nil
	before := hub.count(hubproto.TypeEvents)
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got := hub.count(hubproto.TypeEvents) - before; got != 1 {
		t.Errorf("%d event messages for 5 small events, want 1", got)
	}
	if len(hub.events) != 5 || n.Pending() != 0 {
		t.Fatalf("hub got %d events, %d still pending", len(hub.events), n.Pending())
	}
	for i, e := range hub.events {
		if e.Seq != uint32(3+i) || e.Data.Get("i").Int(-1) != int64(3+i) || e.Time != hub.time {
			t.Errorf("event %d = seq %d %v at %d", i, e.Seq, e.Data.Any(), e.Time)
		}
	}

	// MaxEventBytes bounds the stored queue.
	n = testNode(t, hub, &MemStorage{}, func(c *Config) { c.MaxEventBytes = 200 })
	big := hubproto.Map()
	big.Set("s", hubproto.String(strings.Repeat("x", 80)))
	for i := 0; i < 4; i++ {
		n.Report("big", big)
	}
	if n.Pending() != 2 {
		t.Errorf("%d pending, want 2 within 200 bytes", n.Pending())
	}
	huge := hubproto.Map()
	huge.Set("s", hubproto.String(strings.Repeat("x", 300)))
	if err := n.Report("huge", huge); !errors.Is(err, ErrTooLarge) {
		t.Errorf("Report beyond MaxEventBytes = %v", err)
	}
}

func TestEventTimes(t *testing.T) {
	hub := newFakeHub(t)
	storage := &MemStorage{}
	n := testNode(t, hub, storage)

	// Before the first check-in the clock is unknown: the event is stamped
	// once it is set, back-dated by how long ago it happened.
	n.Report("early", hubproto.Null())
	time.Sleep(1100 * time.Millisecond)
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if e := hub.events[0]; e.Time != hub.time-1 {
		t.Errorf("early event at %d, want %d", e.Time, hub.time-1)
	}
	n.Report("later", hubproto.Null())
	if err := n.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if e := hub.events[1]; e.Time < hub.time || e.Time > hub.time+2 {
		t.Errorf("later event at %d, want about %d", e.Time, hub.time)
	}

	// An event queued before a reboot, and never stamped, stays unknown.
	hub.drop = func(hubproto.Message) bool { return true }
	n = testNode(t, hub, storage)
	n.Report("lost clock", hubproto.Null())
	n = testNode(t, hub, storage)
	hub.drop = nil
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if e := hub.events[len(hub.events)-1]; e.Kind != "lost clock" || e.Time != 0 {
		t.Errorf("event = %s at %d, want time 0", e.Kind, e.Time)
	}
}

func TestLargeEventFragments(t *testing.T) {
	hub := newFakeHub(t)
	n := testNode(t, hub, &MemStorage{}, func(c *Config) { c.Retries = 1 })
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	big := hubproto.Map()
	big.Set("samples", hubproto.String(strings.Repeat("0123456789", 60)))
	n.Report("small", hubproto.Null())
	n.Report("big", big)
	n.Report("small", hubproto.Null())

	// Lose one fragment's reply: it is resent and the message still
	// arrives whole.
	frags := 0
	hub.drop = func(m hubproto.Message) bool {
		if m.Type() == hubproto.TypeFragment {
			frags++
			return frags == 2
		}
		return false
	}
	if err := n.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(hub.events) != 3 || hub.events[1].Kind != "big" || !hub.events[1].Data.Equal(big) {
		t.Fatalf("hub got %d events", len(hub.events))
	}
	// One fragment was sent twice.
	body, _ := hubproto.AppendMessage(nil, &hubproto.Events{Events: []hubproto.Event{{Kind: "big", Data: big}}})
	if want := (len(body)+hubproto.FragmentSize-1)/hubproto.FragmentSize + 1; frags != want {
		t.Errorf("%d fragments sent, want %d", frags, want)
	}
	if hub.count(hubproto.TypeEvents) != 2 {
		t.Errorf("%d unfragmented event messages, want 2 (the small events either side)", hub.count(hubproto.TypeEvents))
	}
}

func TestCommands(t *testing.T) {
	hub := newFakeHub(t)
	storage := &MemStorage{}
	runs := 0
	opts := func(c *Config) {
		c.Command = func(name string, args hubproto.Value) (hubproto.Value, error) {
			switch name {
			case "add":
				runs++
				return hubproto.Int(args.Get("a").Int(0) + args.Get("b").Int(0)), nil
			case "fail":
				return hubproto.Null(), errors.New("no sensor")
			}
			return hubproto.Null(), ErrUnknownCommand
		}
	}
	n := testNode(t, hub, storage, opts)
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	args := hubproto.Map()
	args.Set("a", hubproto.Int(2))
	args.Set("b", hubproto.Int(3))
	hub.commands = []hubproto.Command{
		{ID: 1, Name: "add", Args: args},
		{ID: 2, Name: "fail", Args: hubproto.Null()},
		{ID: 3, Name: "what", Args: hubproto.Null()},
	}
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	want := []struct {
		status uint8
		result hubproto.Value
	}{
		{hubproto.CommandOK, hubproto.Int(5)},
		{hubproto.CommandFailed, hubproto.String("no sensor")},
		{hubproto.CommandUnknown, hubproto.Null()},
	}
	if len(hub.results) != len(want) {
		t.Fatalf("results = %+v", hub.results)
	}
	for i, w := range want {
		if r := hub.results[i]; r.ID != uint32(i+1) || r.Status != w.status || !r.Result.Equal(w.result) {
			t.Errorf("result %d = %+v", i, r)
		}
	}

	// A command whose result is lost is not run again: the stored result
	// is resent at the next check-in, even after a reboot.
	hub.commands = []hubproto.Command{{ID: 4, Name: "add", Args: args}}
	hub.drop = func(m hubproto.Message) bool { return m.Type() == hubproto.TypeCommandResult }
	if err := n.Sync(ctx); !errors.Is(err, ErrNoReply) {
		t.Fatalf("Sync with the result lost = %v", err)
	}
	hub.drop = nil
	n = testNode(t, hub, storage, opts)
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if runs != 2 {
		t.Errorf("add ran %d times, want 2", runs)
	}
	if r := hub.results[len(hub.results)-1]; r.ID != 4 || !r.Result.Equal(hubproto.Int(5)) {
		t.Errorf("resent result = %+v", r)
	}

	// Built in: logs, and reset.
	n.Log("hello from the node")
	n.Report("queued", hubproto.Null())
	hub.drop = func(m hubproto.Message) bool { return m.Type() == hubproto.TypeEvents }
	n.Flush(ctx)
	hub.drop = nil
	hub.commands = []hubproto.Command{{ID: 5, Name: "logs", Args: hubproto.Null()}, {ID: 6, Name: "reset", Args: hubproto.Null()}}
	events := len(hub.events)
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if r := hub.results[len(hub.results)-2]; r.ID != 5 || !strings.Contains(r.Result.String(""), "hello from the node") {
		t.Errorf("logs result = %+v", r)
	}
	if len(hub.events) != events || n.Pending() != 0 || n.ConfigVersion() != 0 || n.st.hasPath {
		t.Errorf("after reset: %d events sent, %d pending, config v%d, path %v",
			len(hub.events)-events, n.Pending(), n.ConfigVersion(), n.st.hasPath)
	}
	if !n.Paired() {
		t.Error("reset unpaired the node")
	}
}

func TestCorruptStorage(t *testing.T) {
	hub := newFakeHub(t)
	storage := &MemStorage{}
	n := testNode(t, hub, storage)
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	key := n.PublicKey()

	for _, k := range []string{keyState, keyConfig, keyEvents, keyConfigStatus, keyResult} {
		storage.Save(k, []byte{0xFF, 0x01, 0x02})
	}
	n = testNode(t, hub, storage)
	if n.PublicKey() != key {
		t.Error("identity lost")
	}
	if n.Paired() || n.Pending() != 0 || n.ConfigVersion() != 0 || n.result != nil {
		t.Errorf("corrupt state kept: paired %v, %d events, config v%d", n.Paired(), n.Pending(), n.ConfigVersion())
	}
	if c := strings.Count(n.Logs(), "corrupt"); c != 5 {
		t.Errorf("%d corrupt values logged: %q", c, n.Logs())
	}
	// It pairs again and carries on.
	if err := n.Sync(ctx); err != nil || !n.Paired() {
		t.Errorf("Sync = %v, paired %v", err, n.Paired())
	}

	// Malformed but decodable values are rejected too.
	for _, b := range [][]byte{
		encodeState(&state{hasPath: true, pathLen: 2, path: []byte{1}}), // path shorter than its length
		append(encodeState(&state{}), 0)[:9],                            // truncated
	} {
		if _, err := decodeState(b); err == nil {
			t.Errorf("decodeState(%x) accepted", b)
		}
	}
	for _, v := range []hubproto.Value{
		hubproto.Int(3),
		hubproto.List(hubproto.Int(1)),
		hubproto.List(hubproto.List(hubproto.String("x"), hubproto.String("k"), hubproto.Null())),
	} {
		if _, err := decodeEvents(v.AppendCBOR(nil)); err == nil {
			t.Errorf("decodeEvents(%v) accepted", v.Any())
		}
	}
}

func TestOldEventFormat(t *testing.T) {
	// Version 1 firmware stored events as [seq, kind, data].
	old := hubproto.List(hubproto.List(hubproto.Int(7), hubproto.String("flow"), hubproto.Int(1))).AppendCBOR(nil)
	events, err := decodeEvents(old)
	if err != nil || len(events) != 1 || events[0].seq != 7 || events[0].kind != "flow" || events[0].time != 0 {
		t.Errorf("decodeEvents = %+v, %v", events, err)
	}
}

func TestLogBuffer(t *testing.T) {
	l := logBuffer{size: 20}
	l.add("one")
	l.add("two")
	if l.String() != "one\ntwo\n" {
		t.Errorf("log = %q", l.String())
	}
	l.add("three four five") // 16 bytes: the oldest line goes
	if l.String() != "two\nthree four five\n" {
		t.Errorf("log = %q", l.String())
	}
	l.add(strings.Repeat("x", 50))
	if len(l.String()) > 20 || !strings.HasPrefix(l.String(), "xxx") {
		t.Errorf("long line: %q", l.String())
	}
	l.clear()
	l.add("a\n")
	if l.String() != "a\n" {
		t.Errorf("after clear: %q", l.String())
	}

	hub := newFakeHub(t)
	n := testNode(t, hub, &MemStorage{})
	n.Log("before")
	if !strings.HasPrefix(n.Logs(), "+0s before") {
		t.Errorf("unstamped line = %q", n.Logs())
	}
	if err := n.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	n.Log("after")
	stamp := time.Unix(int64(hub.time), 0).UTC().Format("01-02 15:04")
	if !strings.Contains(n.Logs(), stamp) {
		t.Errorf("log %q lacks the hub's time %s", n.Logs(), stamp)
	}
}
