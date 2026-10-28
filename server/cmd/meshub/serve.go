package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gapidobri/meshub/server/internal/api"
	"github.com/gapidobri/meshub/server/internal/appmqtt"
	"github.com/gapidobri/meshub/server/internal/hub"
	"github.com/gapidobri/meshub/server/internal/ingest"
	"github.com/gapidobri/meshub/server/internal/mqttconn"
	"github.com/gapidobri/meshub/server/internal/store"
	"github.com/gapidobri/meshub/server/internal/web"
)

// shutdownTimeout bounds waiting for HTTP requests in progress on shutdown.
const shutdownTimeout = 10 * time.Second

func serve(ctx context.Context, cfg config, log *slog.Logger) error {
	sig, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(cfg.DB.Path)
	if err != nil {
		return err
	}
	defer st.Close()

	id, err := hub.LoadIdentity(sig, st)
	if err != nil {
		return err
	}

	// work bounds handling the mesh. It outlives a shutdown signal until
	// requests already received from devices are answered.
	work, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()

	// The hub and the app publisher publish through the connection they
	// are subscribed on, so the connection is created after them and handed
	// to them.
	pub := &connRef{}
	h := hub.New(id, cfg.Hub.Name, pub, st, log)
	h.AutoPair = cfg.Hub.AutoPair
	var app *appmqtt.App
	if cfg.Apps.Enabled {
		app = appmqtt.New(st, pub, log)
		h.Notify = app
	}
	in := ingest.New(st, log)
	in.Self = h.PublicKey()
	in.OnSelfAdvert = h.NoteOwnAdvert
	in.Hub = h

	clientID := cfg.MQTT.ClientID
	if clientID == "" {
		host, _ := os.Hostname()
		clientID = "meshub-server-" + host
	}
	mqttCfg := mqttconn.Config{
		Broker:   cfg.MQTT.Broker,
		ClientID: clientID,
		Username: cfg.MQTT.Username,
		Password: cfg.MQTT.Password,
	}
	if app != nil {
		mqttCfg.OnConnect = app.Resync
	}
	conn := mqttconn.Connect(mqttCfg, log, append(in.Subscriptions(work), h.Subscriptions()...)...)
	pub.Store(conn)
	log.Info("hub identity", "name", cfg.Hub.Name, "pubkey", h.PublicKey(), "version", fullVersion())

	var wg sync.WaitGroup
	wg.Go(func() { h.RunAdverts(sig, cfg.Hub.AdvertInterval) })
	wg.Go(func() { prune(sig, log, st, cfg.Retention.Packets, cfg.Retention.Events) })
	if app != nil {
		wg.Go(func() { app.Run(work) })
	}

	srv := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           handler(cfg, st, h, conn, log),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	served := make(chan error, 1)
	go func() { served <- srv.ListenAndServe() }()
	log.Info("serving", "addr", cfg.HTTP.Addr, "db", cfg.DB.Path, "ui", cfg.UI.Enabled, "apps", cfg.Apps.Enabled)

	var serveErr error
	select {
	case <-sig.Done():
		log.Info("shutting down")
	case serveErr = <-served:
	}

	// Stop taking changes, answer the devices already heard while the
	// broker connection is still up, then stop handling the mesh.
	deadline, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(deadline); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	if err := h.Shutdown(deadline); err != nil {
		log.Warn("hub shutdown", "err", err)
	}
	conn.Close()
	cancelWork()
	stop()
	wg.Wait()
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return serveErr
}

// handler serves the API, the health checks and, if enabled, the web UI.
func handler(cfg config, st *store.Store, h *hub.Hub, conn *mqttconn.Conn, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	if len(cfg.API.Tokens) == 0 {
		log.Warn("api.tokens is empty: the API is open to anyone who can reach it")
	}
	api.Register(mux, st, h, log, api.Options{Version: version, Tokens: cfg.API.Tokens})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := st.Ping(r.Context()); err != nil {
			http.Error(w, "database: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		if !conn.Connected() {
			http.Error(w, "mqtt: not connected", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok\n"))
	})

	if cfg.UI.Enabled {
		ui := web.Handler(st, h, log)
		if cfg.UI.Password != "" {
			ui = basicAuth(ui, cfg.UI.Username, cfg.UI.Password)
		} else {
			log.Warn("ui.password is empty: the web UI is open to anyone who can reach it")
		}
		// The UI changes state with form posts, which a browser would send
		// from other sites with the user's credentials.
		mux.Handle("/", http.NewCrossOriginProtection().Handler(ui))
	}
	return mux
}

func basicAuth(next http.Handler, username, password string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || subtle.ConstantTimeCompare([]byte(u), []byte(username)) != 1 ||
			subtle.ConstantTimeCompare([]byte(p), []byte(password)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="meshub", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// errNoConn is returned for publishing before the connection is made.
var errNoConn = errors.New("mqtt: not connected yet")

// connRef publishes through a connection made after it.
type connRef struct{ atomic.Pointer[mqttconn.Conn] }

func (r *connRef) Publish(topic string, payload []byte) error {
	return r.PublishMessage(topic, 0, false, payload)
}

func (r *connRef) PublishMessage(topic string, qos byte, retain bool, payload []byte) error {
	c := r.Load()
	if c == nil {
		return errNoConn
	}
	return c.PublishMessage(topic, qos, retain, payload)
}

func (r *connRef) Connected() bool {
	c := r.Load()
	return c != nil && c.Connected()
}

// prune deletes old packets, and old events if events is set, hourly.
func prune(ctx context.Context, log *slog.Logger, st *store.Store, packets, events time.Duration) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		now := time.Now()
		if err := st.Prune(ctx, now.Add(-packets)); err != nil && ctx.Err() == nil {
			log.Warn("prune", "err", err)
		}
		if events > 0 {
			if err := st.PruneEvents(ctx, now.Add(-events)); err != nil && ctx.Err() == nil {
				log.Warn("prune events", "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
