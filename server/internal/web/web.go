// Package web serves the backend's web UI and JSON API.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gapidobri/meshub/proto/gwproto"
	"github.com/gapidobri/meshub/proto/hubproto"
	"github.com/gapidobri/meshub/server/internal/hub"
	"github.com/gapidobri/meshub/server/internal/store"
)

//go:embed layout.html index.html device.html
var files embed.FS

// Hub is the backend's own mesh presence.
type Hub interface {
	Status() hub.Status
	SendAdvert(ctx context.Context) (int, error)
	SetConfig(ctx context.Context, device string, jsonConfig []byte) (uint32, error)
}

// page is what the shared layout needs.
type page struct {
	Title   string
	Refresh string // URL to reload every 10s, if any
	Header  string
	Message string
}

// Handler serves the UI at / and the API under /api/.
func Handler(st *store.Store, h Hub, log *slog.Logger) http.Handler {
	funcs := template.FuncMap{
		"ago":         ago,
		"never":       func(t time.Time) bool { return t.IsZero() },
		"uptime":      uptime,
		"short":       func(pk string) string { return pk[:min(8, len(pk))] },
		"nodeType":    nodeType,
		"configState": configState,
		"wakeReason":  wakeReason,
	}
	parse := func(name string) *template.Template {
		return template.Must(template.New(name).Funcs(funcs).ParseFS(files, "layout.html", name))
	}
	index, device := parse("index.html"), parse("device.html")
	render := func(w http.ResponseWriter, t *template.Template, data any) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := t.Execute(w, data); err != nil {
			log.Error("render", "err", err)
		}
	}
	back := func(w http.ResponseWriter, r *http.Request, to, msg string) {
		if msg != "" {
			to += "?msg=" + url.QueryEscape(msg)
		}
		http.Redirect(w, r, to, http.StatusSeeOther)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		gws, err := st.Gateways(ctx)
		if err != nil {
			serverError(w, log, err)
			return
		}
		nodes, err := st.Nodes(ctx)
		if err != nil {
			serverError(w, log, err)
			return
		}
		devices, err := st.Devices(ctx)
		if err != nil {
			serverError(w, log, err)
			return
		}
		packets, receptions, err := st.PacketCount(ctx)
		if err != nil {
			serverError(w, log, err)
			return
		}
		names := map[string]string{}
		views := make([]gatewayView, len(gws))
		for i, g := range gws {
			names[g.PubKey] = g.Name
			views[i] = newGatewayView(g)
		}
		render(w, index, struct {
			Page         page
			Hub          hub.Status
			Devices      []store.Device
			Gateways     []gatewayView
			Nodes        []store.Node
			GatewayNames map[string]string
		}{
			Page: page{
				Title: "meshub", Refresh: "/", Message: r.URL.Query().Get("msg"),
				Header: fmt.Sprintf("%d packets · %d receptions", packets, receptions),
			},
			Hub: h.Status(), Devices: devices, Gateways: views, Nodes: nodes, GatewayNames: names,
		})
	})
	mux.HandleFunc("POST /advert", func(w http.ResponseWriter, r *http.Request) {
		msg := "Advert queued on 1 gateway."
		if n, err := h.SendAdvert(r.Context()); err != nil {
			msg = "Advert not sent: " + err.Error()
		} else if n != 1 {
			msg = fmt.Sprintf("Advert queued on %d gateways.", n)
		}
		back(w, r, "/", msg)
	})

	// Devices.
	lookup := func(w http.ResponseWriter, r *http.Request) (store.Device, bool) {
		d, ok, err := st.Device(r.Context(), strings.ToUpper(r.PathValue("pubkey")))
		if err != nil {
			serverError(w, log, err)
		} else if !ok {
			http.NotFound(w, r)
		}
		return d, err == nil && ok
	}
	mux.HandleFunc("GET /devices/{pubkey}", func(w http.ResponseWriter, r *http.Request) {
		d, ok := lookup(w, r)
		if !ok {
			return
		}
		cfg, err := st.Config(r.Context(), d.PubKey)
		if err != nil {
			serverError(w, log, err)
			return
		}
		events, err := st.Events(r.Context(), d.PubKey, 50)
		if err != nil {
			serverError(w, log, err)
			return
		}
		configJSON := cfg.JSON
		if configJSON == "" {
			configJSON = "{}"
		}
		// No auto-refresh here: it would discard edits to the config.
		render(w, device, struct {
			Page       page
			Device     store.Device
			ConfigJSON string
			Events     []store.Event
		}{page{Title: d.Program + " " + d.PubKey[:8] + " · meshub", Header: d.Program, Message: r.URL.Query().Get("msg")}, d, configJSON, events})
	})
	mux.HandleFunc("POST /devices/{pubkey}/status", func(w http.ResponseWriter, r *http.Request) {
		d, ok := lookup(w, r)
		if !ok {
			return
		}
		status := r.FormValue("status")
		if status != store.DevicePaired && status != store.DeviceRejected {
			http.Error(w, "bad status", http.StatusBadRequest)
			return
		}
		if err := st.SetDeviceStatus(r.Context(), d.PubKey, status); err != nil {
			serverError(w, log, err)
			return
		}
		log.Info("device status set", "device", d.PubKey[:8], "status", status)
		msg := "Approved. The device is accepted the next time it asks to pair."
		if status == store.DeviceRejected {
			msg = "Rejected."
		}
		back(w, r, referrerPath(r, "/"), msg)
	})
	mux.HandleFunc("POST /devices/{pubkey}/delete", func(w http.ResponseWriter, r *http.Request) {
		d, ok := lookup(w, r)
		if !ok {
			return
		}
		if err := st.DeleteDevice(r.Context(), d.PubKey); err != nil {
			serverError(w, log, err)
			return
		}
		back(w, r, "/", "Device forgotten.")
	})
	mux.HandleFunc("POST /devices/{pubkey}/config", func(w http.ResponseWriter, r *http.Request) {
		d, ok := lookup(w, r)
		if !ok {
			return
		}
		version, err := h.SetConfig(r.Context(), d.PubKey, []byte(r.FormValue("config")))
		msg := fmt.Sprintf("Saved as version %d. The device fetches it at its next check-in.", version)
		if err != nil {
			msg = "Not saved: " + err.Error()
		}
		back(w, r, "/devices/"+d.PubKey, msg)
	})

	// API.
	mux.HandleFunc("GET /api/hub", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, log, h.Status(), nil)
	})
	mux.HandleFunc("POST /api/advert", func(w http.ResponseWriter, r *http.Request) {
		n, err := h.SendAdvert(r.Context())
		if errors.Is(err, hub.ErrNoGateway) {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, log, map[string]int{"gateways": n}, err)
	})
	mux.HandleFunc("GET /api/gateways", func(w http.ResponseWriter, r *http.Request) {
		gws, err := st.Gateways(r.Context())
		writeJSON(w, log, gws, err)
	})
	mux.HandleFunc("GET /api/nodes", func(w http.ResponseWriter, r *http.Request) {
		nodes, err := st.Nodes(r.Context())
		writeJSON(w, log, nodes, err)
	})
	mux.HandleFunc("GET /api/devices", func(w http.ResponseWriter, r *http.Request) {
		devices, err := st.Devices(r.Context())
		writeJSON(w, log, devices, err)
	})
	mux.HandleFunc("GET /api/devices/{pubkey}/events", func(w http.ResponseWriter, r *http.Request) {
		d, ok := lookup(w, r)
		if !ok {
			return
		}
		events, err := st.Events(r.Context(), d.PubKey, 500)
		writeJSON(w, log, events, err)
	})
	mux.HandleFunc("PUT /api/devices/{pubkey}/config", func(w http.ResponseWriter, r *http.Request) {
		d, ok := lookup(w, r)
		if !ok {
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		version, err := h.SetConfig(r.Context(), d.PubKey, body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, log, map[string]uint32{"version": version}, nil)
	})
	return mux
}

// referrerPath returns the path of the page a form was posted from, if it
// is on this site, else def.
func referrerPath(r *http.Request, def string) string {
	u, err := url.Parse(r.Referer())
	if err != nil || u.Host != r.Host || !strings.HasPrefix(u.Path, "/") {
		return def
	}
	return u.Path
}

func configState(d store.Device) string {
	switch {
	case d.WantVersion == 0:
		return "none set"
	case d.ConfigVersion == d.WantVersion:
		return fmt.Sprintf("v%d, in sync", d.WantVersion)
	default:
		return fmt.Sprintf("v%d waiting for the device (has v%d)", d.WantVersion, d.ConfigVersion)
	}
}

func wakeReason(r int) string {
	switch r {
	case hubproto.WakePowerOn:
		return "power on"
	case hubproto.WakeTimer:
		return "timer"
	case hubproto.WakePin:
		return "pin"
	case hubproto.WakeOther:
		return "other"
	}
	return "unknown"
}

type gatewayView struct {
	store.Gateway
	State string // "online", "stale" or "offline"
	Stats gwproto.Stats
}

func newGatewayView(g store.Gateway) gatewayView {
	v := gatewayView{Gateway: g, State: gwproto.StatusOffline}
	if g.Status == gwproto.StatusOnline {
		v.State = gwproto.StatusOnline
		if time.Since(g.LastStatusAt) > store.GatewayStaleAfter {
			v.State = "stale"
		}
	}
	json.Unmarshal([]byte(g.Stats), &v.Stats)
	return v
}

func writeJSON(w http.ResponseWriter, log *slog.Logger, v any, err error) {
	if err != nil {
		serverError(w, log, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func serverError(w http.ResponseWriter, log *slog.Logger, err error) {
	log.Error("http", "err", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}

// ago formats how long ago t was, coarsely.
func ago(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

// uptime formats a number of seconds as e.g. "3d 4h" or "12m".
func uptime(secs uint32) string {
	d := time.Duration(secs) * time.Second
	switch {
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd %dh", int(d.Hours()/24), int(d.Hours())%24)
	}
}

func nodeType(t uint8) string {
	switch t {
	case 1:
		return "chat"
	case 2:
		return "repeater"
	case 3:
		return "room"
	case 4:
		return "sensor"
	default:
		return "unknown"
	}
}
