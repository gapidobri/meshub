package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/proto/appproto"
	"github.com/gapidobri/meshub/server/internal/hub"
	"github.com/gapidobri/meshub/server/internal/store"
)

const (
	token = "0123456789abcdef"
	pk    = "ABCD000000000000000000000000000000000000000000000000000000000000"
)

type nopPublisher struct{}

func (nopPublisher) Publish(string, []byte) error { return nil }

type client struct {
	t   *testing.T
	srv http.Handler
}

// do sends a request with the API token and decodes a JSON reply into out.
func (c client) do(method, path, body string, out any) int {
	c.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	c.srv.ServeHTTP(rec, req)
	if out != nil && rec.Code < 300 {
		if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
			c.t.Fatalf("%s %s: %v: %s", method, path, err, rec.Body)
		}
	}
	return rec.Code
}

func setup(t *testing.T) (client, *store.Store) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := hub.New(meshcore.LocalIdentityFromSeed(make([]byte, 32)), "test hub", nopPublisher{}, st, log)
	mux := http.NewServeMux()
	Register(mux, st, h, log, Options{Version: "test", Tokens: []string{token}})
	return client{t, mux}, st
}

func TestAuth(t *testing.T) {
	c, _ := setup(t)
	for _, auth := range []string{"", "Bearer wrong", token} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/devices", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		c.srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("Authorization %q = %d", auth, rec.Code)
		}
	}
	if code := c.do("GET", "/api/v1/devices", "", nil); code != http.StatusOK {
		t.Errorf("with token = %d", code)
	}
	// The description is public.
	rec := httptest.NewRecorder()
	c.srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/openapi.json", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "list-device-events") {
		t.Errorf("openapi.json = %d", rec.Code)
	}
}

func TestDevices(t *testing.T) {
	c, st := setup(t)
	ctx := context.Background()
	if _, err := st.Join(ctx, pk, "hydrant", "1.0", "XY12", store.DevicePending, 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	var list struct{ Devices []appproto.Device }
	if c.do("GET", "/api/v1/devices?program=hydrant&status=pending", "", &list); len(list.Devices) != 1 || list.Devices[0].Claim != "XY12" {
		t.Errorf("pending hydrants = %+v", list.Devices)
	}
	if c.do("GET", "/api/v1/devices?program=meter", "", &list); len(list.Devices) != 0 {
		t.Errorf("meters = %+v", list.Devices)
	}
	if code := c.do("GET", "/api/v1/devices/"+strings.Repeat("E", 64), "", nil); code != http.StatusNotFound {
		t.Errorf("unknown device = %d", code)
	}
	if code := c.do("GET", "/api/v1/devices/xyz", "", nil); code != http.StatusUnprocessableEntity {
		t.Errorf("malformed key = %d", code)
	}

	var d appproto.Device
	if code := c.do("POST", "/api/v1/devices/"+strings.ToLower(pk)+"/approve", "", &d); code != http.StatusOK || d.Status != appproto.DevicePaired {
		t.Errorf("approve = %d %+v", code, d)
	}
	if d.LastHelloAt != nil || d.WakeReason != "unknown" {
		t.Errorf("device before its first hello = %+v", d)
	}

	// Config.
	var cfg appproto.Config
	if code := c.do("GET", "/api/v1/devices/"+pk+"/config", "", &cfg); code != http.StatusOK || cfg.Version != 0 || string(cfg.Config) != "{}" {
		t.Errorf("config before any = %+v", cfg)
	}
	if code := c.do("PUT", "/api/v1/devices/"+pk+"/config", `{"interval_s": 60}`, &cfg); code != http.StatusOK || cfg.Version != 1 {
		t.Errorf("set config = %d %+v", code, cfg)
	}
	if code := c.do("PUT", "/api/v1/devices/"+pk+"/config", `[1]`, nil); code != http.StatusUnprocessableEntity {
		t.Errorf("array config = %d", code)
	}
	if c.do("GET", "/api/v1/devices/"+pk+"/config", "", &cfg); cfg.Version != 1 || !strings.Contains(string(cfg.Config), `"interval_s":60`) {
		t.Errorf("config = %s", cfg.Config)
	}

	// Commands.
	var cmd appproto.Command
	if code := c.do("POST", "/api/v1/devices/"+pk+"/commands", `{"name":"status","args":{"n":1}}`, &cmd); code != http.StatusCreated ||
		cmd.Status != appproto.CommandPending || string(cmd.Args) != `{"n":1}` {
		t.Errorf("queue = %d %+v", code, cmd)
	}
	if code := c.do("POST", "/api/v1/devices/"+pk+"/commands", `{"name":""}`, nil); code != http.StatusUnprocessableEntity {
		t.Errorf("unnamed command = %d", code)
	}
	path := "/api/v1/devices/" + pk + "/commands/" + jsonString(cmd.ID)
	if code := c.do("POST", path+"/cancel", "", &cmd); code != http.StatusOK || cmd.Status != appproto.CommandCancelled {
		t.Errorf("cancel = %d %+v", code, cmd)
	}
	if code := c.do("POST", path+"/cancel", "", nil); code != http.StatusConflict {
		t.Errorf("cancel again = %d", code)
	}
	if code := c.do("GET", "/api/v1/devices/"+pk+"/commands/999", "", nil); code != http.StatusNotFound {
		t.Errorf("unknown command = %d", code)
	}

	// Events.
	for i, data := range []string{`{"flow":true}`, `2`, `"x"`} {
		if err := st.AddEvent(ctx, pk, store.Event{Seq: uint32(i), Kind: "open", Data: data, ReceivedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	var events struct{ Events []appproto.Event }
	if c.do("GET", "/api/v1/events?after=1&limit=1", "", &events); len(events.Events) != 1 || string(events.Events[0].Data) != "2" ||
		events.Events[0].Program != "hydrant" {
		t.Errorf("events after 1 = %+v", events.Events)
	}
	if c.do("GET", "/api/v1/devices/"+pk+"/events?order=desc", "", &events); len(events.Events) != 3 || events.Events[0].ID != 3 {
		t.Errorf("device events = %+v", events.Events)
	}

	if code := c.do("DELETE", "/api/v1/devices/"+pk, "", nil); code != http.StatusNoContent {
		t.Errorf("delete = %d", code)
	}
	if code := c.do("DELETE", "/api/v1/devices/"+pk, "", nil); code != http.StatusNotFound {
		t.Errorf("delete again = %d", code)
	}
}

func TestAdvertWithoutGateways(t *testing.T) {
	c, _ := setup(t)
	if code := c.do("POST", "/api/v1/hub/advert", "", nil); code != http.StatusServiceUnavailable {
		t.Errorf("advert = %d", code)
	}
	var h appproto.Hub
	if c.do("GET", "/api/v1/hub", "", &h); h.Name != "test hub" || len(h.PubKey) != 64 {
		t.Errorf("hub = %+v", h)
	}
}

func jsonString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
