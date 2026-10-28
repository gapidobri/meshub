package web

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gapidobri/meshub/server/internal/hub"
	"github.com/gapidobri/meshub/server/internal/store"
)

type fakeHub struct {
	adverts int
	st      *store.Store
}

func (f *fakeHub) Status() hub.Status {
	return hub.Status{Name: "meshub", PublicKey: "ABCDEF", Results: []hub.TxResult{}}
}

func (f *fakeHub) SendAdvert(context.Context) (int, error) { f.adverts++; return 2, nil }

func (f *fakeHub) SetConfig(ctx context.Context, device string, cfg []byte) (uint32, error) {
	return f.st.SetConfig(ctx, device, []byte{0xA0}, string(cfg), time.Now())
}

func (f *fakeHub) QueueCommand(ctx context.Context, device, name string, args []byte) (uint32, error) {
	if len(args) == 0 {
		args = []byte("null")
	}
	return f.st.AddCommand(ctx, device, name, string(args), []byte{0xF6}, time.Now())
}

func (f *fakeHub) CancelCommand(ctx context.Context, device string, id uint32) error {
	_, err := f.st.CancelCommand(ctx, device, id, time.Now())
	return err
}

func (f *fakeHub) SetDeviceStatus(ctx context.Context, device, status string) error {
	return f.st.SetDeviceStatus(ctx, device, status)
}

func (f *fakeHub) DeleteDevice(ctx context.Context, device string) error {
	return f.st.DeleteDevice(ctx, device)
}

func TestPages(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fh := &fakeHub{st: st}
	h := Handler(st, fh, log)

	get := func(path string) string {
		t.Helper()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d: %s", path, rec.Code, rec.Body)
		}
		return rec.Body.String()
	}

	if body := get("/"); !strings.Contains(body, "No gateways yet") || !strings.Contains(body, "No nodes yet") {
		t.Error("empty page lacks empty states")
	}

	ctx := context.Background()
	now := time.Now()
	gw := "AAAA000000000000000000000000000000000000000000000000000000000000"
	if err := st.UpsertGateway(ctx, store.Gateway{PubKey: gw, IATA: "LJU", Name: "roof <gw>", Status: "online",
		Stats: `{"uptime_secs":90000}`, LastStatusAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.AddAdvert(ctx, store.Advert{PubKey: "CCCC", Name: "hydrant", Type: 4, HasLoc: true, Lat: 46.05, Lon: 14.5,
		AdvertTime: 1, Gateway: gw, SNR: 7, RSSI: -90, Hops: 1, HeardAt: now}); err != nil {
		t.Fatal(err)
	}

	body := get("/")
	for _, want := range []string{"roof &lt;gw&gt;", "online", "1d 1h", "hydrant", "sensor", "46.05000, 14.50000"} {
		if !strings.Contains(body, want) {
			t.Errorf("page lacks %q", want)
		}
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/advert", nil))
	if rec.Code != http.StatusSeeOther || fh.adverts != 1 || !strings.Contains(rec.Header().Get("Location"), "2+gateways") {
		t.Errorf("POST /advert = %d %q, adverts %d", rec.Code, rec.Header().Get("Location"), fh.adverts)
	}
	if body := get("/?msg=Advert+queued"); !strings.Contains(body, "Advert queued") || !strings.Contains(body, "Send advert") {
		t.Error("page lacks the advert message or button")
	}
}

func TestDevicePages(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	h := Handler(st, &fakeHub{st: st}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()
	pk := "ABCD000000000000000000000000000000000000000000000000000000000000"
	if _, err := st.Join(ctx, pk, "hydrant", "0.1", "XY12", store.DevicePending, 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	do := func(method, path string, form url.Values) *httptest.ResponseRecorder {
		t.Helper()
		var body io.Reader
		if form != nil {
			body = strings.NewReader(form.Encode())
		}
		req := httptest.NewRequest(method, path, body)
		if form != nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	if body := do("GET", "/", nil).Body.String(); !strings.Contains(body, "Approve") || !strings.Contains(body, "hydrant") {
		t.Error("pending device not listed with an Approve button")
	}
	if rec := do("POST", "/devices/"+pk+"/status", url.Values{"status": {"paired"}}); rec.Code != http.StatusSeeOther {
		t.Fatalf("approve = %d", rec.Code)
	}
	if d, _, _ := st.Device(ctx, pk); d.Status != store.DevicePaired {
		t.Errorf("status after approve = %s", d.Status)
	}
	if rec := do("POST", "/devices/"+pk+"/status", url.Values{"status": {"bogus"}}); rec.Code != http.StatusBadRequest {
		t.Errorf("bogus status = %d", rec.Code)
	}

	rec := do("POST", "/devices/"+strings.ToLower(pk)+"/config", url.Values{"config": {`{"a":1}`}})
	if rec.Code != http.StatusSeeOther || !strings.Contains(rec.Header().Get("Location"), "version+1") {
		t.Errorf("save config = %d %s", rec.Code, rec.Header().Get("Location"))
	}
	page := do("GET", "/devices/"+pk, nil)
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "XY12") ||
		!strings.Contains(page.Body.String(), "v1 waiting for the device") || !strings.Contains(page.Body.String(), "No events yet") {
		t.Errorf("device page = %d", page.Code)
	}

	// Commands: queue two, cancel one, finish the other with a log text.
	if rec := do("POST", "/devices/"+pk+"/commands", url.Values{"name": {"logs"}}); rec.Code != http.StatusSeeOther ||
		!strings.Contains(rec.Header().Get("Location"), "queued") {
		t.Errorf("queue command = %d %s", rec.Code, rec.Header().Get("Location"))
	}
	do("POST", "/devices/"+pk+"/commands", url.Values{"name": {"read"}, "args": {`{"n":3}`}})
	cmds, _ := st.Commands(ctx, pk, 10)
	if len(cmds) != 2 {
		t.Fatalf("commands = %+v", cmds)
	}
	if rec := do("POST", fmt.Sprintf("/devices/%s/commands/%d/cancel", pk, cmds[0].ID), url.Values{}); rec.Code != http.StatusSeeOther {
		t.Errorf("cancel = %d", rec.Code)
	}
	st.FinishCommand(ctx, pk, cmds[1].ID, store.CommandOK, `"+3s booted\n+5s config v1 applied\n"`, time.Now())
	st.RecordConfigStatus(ctx, pk, 1, true, "interval_s must be positive")
	body := do("GET", "/devices/"+pk, nil).Body.String()
	for _, want := range []string{"cancelled", "5s config v1 applied", "v1 rejected by the device: interval_s must be positive"} {
		if !strings.Contains(body, want) {
			t.Errorf("device page lacks %q", want)
		}
	}

	if rec := do("GET", "/devices/FFFF", nil); rec.Code != http.StatusNotFound {
		t.Errorf("unknown device = %d", rec.Code)
	}
	if rec := do("POST", "/devices/"+pk+"/delete", url.Values{}); rec.Code != http.StatusSeeOther {
		t.Errorf("delete = %d", rec.Code)
	}
	if _, ok, _ := st.Device(ctx, pk); ok {
		t.Error("device still there after delete")
	}
}
