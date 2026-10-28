package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gapidobri/meshub/proto/appproto"
)

func TestEventsAfterPages(t *testing.T) {
	const total = 2500
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/v1/events" || r.URL.Query().Get("program") != "hydrant" || r.Header.Get("Authorization") != "Bearer t" {
			http.Error(w, "unexpected request "+r.URL.String(), http.StatusBadRequest)
			return
		}
		after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		out := struct {
			Events []appproto.Event `json:"events"`
		}{Events: []appproto.Event{}}
		for id := after + 1; id <= total && len(out.Events) < limit; id++ {
			out.Events = append(out.Events, appproto.Event{ID: id})
		}
		json.NewEncoder(w).Encode(out)
	}))
	defer srv.Close()

	c := New(srv.URL+"/", WithToken("t"))
	var n int
	var last int64
	for e, err := range c.EventsAfter(context.Background(), "hydrant", 10) {
		if err != nil {
			t.Fatal(err)
		}
		if e.ID != last+1 && last != 0 {
			t.Fatalf("event %d after %d", e.ID, last)
		}
		last = e.ID
		n++
	}
	if n != total-10 || last != total || requests != 3 {
		t.Errorf("got %d events up to %d in %d requests", n, last, requests)
	}
}

func TestError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(`{"title":"Conflict","status":409,"detail":"the command is no longer pending"}`))
	}))
	defer srv.Close()
	_, err := New(srv.URL).CancelCommand(context.Background(), "AB", 1)
	var e *Error
	if !errors.As(err, &e) || e.Detail != "the command is no longer pending" || !errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v", err)
	}
}
