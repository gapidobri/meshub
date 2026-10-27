// Command meshub is the meshub backend: it ingests gateway traffic from MQTT
// into SQLite and serves the web UI.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gapidobri/meshub/server/internal/hub"
	"github.com/gapidobri/meshub/server/internal/ingest"
	"github.com/gapidobri/meshub/server/internal/mqttconn"
	"github.com/gapidobri/meshub/server/internal/store"
	"github.com/gapidobri/meshub/server/internal/web"
)

func main() {
	var (
		broker    = flag.String("mqtt", "tcp://localhost:1883", "MQTT broker URL")
		mqttUser  = flag.String("mqtt-user", "", "MQTT username")
		mqttPass  = flag.String("mqtt-pass", os.Getenv("MESHUB_MQTT_PASSWORD"), "MQTT password (default $MESHUB_MQTT_PASSWORD)")
		dbPath    = flag.String("db", "meshub.db", "SQLite database path")
		httpAddr  = flag.String("http", ":8080", "HTTP listen address")
		retention = flag.Duration("retention", 7*24*time.Hour, "how long to keep packets and receptions")
		name      = flag.String("name", "meshub", "name the hub advertises on the mesh")
		advertInt = flag.Duration("advert-interval", 12*time.Hour, "how often the hub floods its advert (0 disables)")
		autoPair  = flag.Bool("auto-pair", false, "accept every device that asks to pair, without approval (for development)")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg := config{*broker, *mqttUser, *mqttPass, *dbPath, *httpAddr, *retention, *name, *advertInt, *autoPair}
	if err := run(log, cfg); err != nil {
		log.Error("meshub", "err", err)
		os.Exit(1)
	}
}

type config struct {
	broker, mqttUser, mqttPass string
	dbPath, httpAddr           string
	retention                  time.Duration
	name                       string
	advertInterval             time.Duration
	autoPair                   bool
}

func run(log *slog.Logger, cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(cfg.dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	id, err := hub.LoadIdentity(ctx, st)
	if err != nil {
		return err
	}

	// The hub publishes through the connection it is subscribed on, so the
	// connection is created after the hub and handed to it.
	var conn *mqttconn.Conn
	h := hub.New(id, cfg.name, publisherFunc(func(topic string, payload []byte) error {
		return conn.Publish(topic, payload)
	}), st, log)
	h.AutoPair = cfg.autoPair
	in := ingest.New(st, log)
	in.Self = h.PublicKey()
	in.OnSelfAdvert = h.NoteOwnAdvert
	in.Hub = h

	host, _ := os.Hostname()
	conn = mqttconn.Connect(mqttconn.Config{
		Broker:   cfg.broker,
		ClientID: "meshub-server-" + host,
		Username: cfg.mqttUser,
		Password: cfg.mqttPass,
	}, log, append(in.Subscriptions(ctx), h.Subscriptions()...)...)
	defer conn.Close()
	log.Info("hub identity", "name", cfg.name, "pubkey", h.PublicKey())

	go h.RunAdverts(ctx, cfg.advertInterval)
	go prune(ctx, log, st, cfg.retention)

	srv := &http.Server{Addr: cfg.httpAddr, Handler: web.Handler(st, h, log)}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	log.Info("serving", "addr", cfg.httpAddr, "db", cfg.dbPath)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

type publisherFunc func(topic string, payload []byte) error

func (f publisherFunc) Publish(topic string, payload []byte) error { return f(topic, payload) }

// prune deletes old packets hourly.
func prune(ctx context.Context, log *slog.Logger, st *store.Store, retention time.Duration) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		if err := st.Prune(ctx, time.Now().Add(-retention)); err != nil && ctx.Err() == nil {
			log.Warn("prune", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
