package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestMigrateEventIDs(t *testing.T) {
	// A database at version 2, with events stored before they had IDs.
	path := filepath.Join(t.TempDir(), "v2.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{schema, migrations[0], migrations[1], `PRAGMA user_version = 2`,
		`INSERT INTO devices (pubkey, status, program, firmware, claim, joined_at) VALUES ('AA', 'paired', 'weather', '1', '', 1)`,
		`INSERT INTO events (pubkey, event_seq, kind, data, received_at) VALUES ('AA', 2, 'b', '2', 20), ('AA', 1, 'a', '1', 10)`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()

	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	events, err := st.EventsByID(ctx, EventQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].ID != 1 || events[0].Kind != "a" || events[1].ID != 2 || events[1].Program != "weather" {
		t.Fatalf("events after migration = %+v", events)
	}
	// Resends are still ignored, and new events get new IDs.
	if err := st.AddEvent(ctx, "AA", Event{Seq: 2, Kind: "b", Data: "2", ReceivedAt: time.UnixMilli(30)}); err != nil {
		t.Fatal(err)
	}
	if err := st.AddEvent(ctx, "AA", Event{Seq: 3, Kind: "c", Data: "3", ReceivedAt: time.UnixMilli(30)}); err != nil {
		t.Fatal(err)
	}
	// IDs increase, but an ignored resend may leave a gap.
	events, err = st.EventsByID(ctx, EventQuery{After: 2, Limit: 10})
	if err != nil || len(events) != 1 || events[0].Kind != "c" {
		t.Fatalf("events after 2 = %+v, %v", events, err)
	}
	if id, err := st.LastEventID(ctx); err != nil || id != events[0].ID {
		t.Errorf("last event ID = %d, %v; want %d", id, err, events[0].ID)
	}
}

func TestEventsByID(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	for _, d := range []struct{ pk, program string }{{"AA", "weather"}, {"BB", "meter"}} {
		if _, err := st.Join(ctx, d.pk, d.program, "1", "", DevicePaired, 0, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 6 {
		pk := []string{"AA", "BB"}[i%2]
		if err := st.AddEvent(ctx, pk, Event{Seq: uint32(i), Kind: "k", Data: "null", ReceivedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	ids := func(q EventQuery) []int64 {
		t.Helper()
		events, err := st.EventsByID(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		var out []int64
		for _, e := range events {
			out = append(out, e.ID)
		}
		return out
	}
	for _, tc := range []struct {
		q    EventQuery
		want []int64
	}{
		{EventQuery{Limit: 2}, []int64{1, 2}},
		{EventQuery{After: 2, Limit: 2}, []int64{3, 4}},
		{EventQuery{Desc: true, Limit: 2}, []int64{6, 5}},
		{EventQuery{Before: 5, Desc: true, Limit: 2}, []int64{4, 3}},
		{EventQuery{PubKey: "BB", Limit: 10}, []int64{2, 4, 6}},
		{EventQuery{Program: "weather", After: 1, Limit: 10}, []int64{3, 5}},
	} {
		if got := ids(tc.q); !equal(got, tc.want) {
			t.Errorf("%+v = %v, want %v", tc.q, got, tc.want)
		}
	}
}

func equal(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
