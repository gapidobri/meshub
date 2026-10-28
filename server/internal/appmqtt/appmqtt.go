// Package appmqtt publishes device data to applications on the MQTT topics
// of package appproto.
package appmqtt

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/gapidobri/meshub/proto/appproto"
	"github.com/gapidobri/meshub/server/internal/appconv"
	"github.com/gapidobri/meshub/server/internal/store"
)

const (
	// cursorKey is the setting holding the ID of the last event published.
	cursorKey = "app_events_cursor"
	// batch is how many events are read at a time.
	batch = 100
	// pollInterval is how often unpublished events are looked for without
	// being told of new ones, which also retries after a failure.
	pollInterval = 5 * time.Second
	// queueLen bounds the status and command updates waiting to be sent.
	queueLen = 256
)

// Publisher publishes MQTT messages.
type Publisher interface {
	PublishMessage(topic string, qos byte, retain bool, payload []byte) error
	Connected() bool
}

type updateKind int

const (
	updateStatus  updateKind = iota // republish a device's status
	updateClear                     // clear a deleted device's status
	updateCommand                   // publish a command's state
	updateAll                       // republish every device's status
)

type update struct {
	kind    updateKind
	device  string
	program string // for updateClear
	command uint32
}

// App publishes device events, status and command updates. It implements
// hub.Notifier.
type App struct {
	st  *store.Store
	pub Publisher
	log *slog.Logger

	wake    chan struct{}
	updates chan update

	mu     sync.Mutex
	clears map[string]string // statuses still to clear: program by device
}

// New returns an App publishing through pub.
func New(st *store.Store, pub Publisher, log *slog.Logger) *App {
	return &App{
		st: st, pub: pub, log: log,
		wake:    make(chan struct{}, 1),
		updates: make(chan update, queueLen),
		clears:  map[string]string{},
	}
}

// DeviceChanged republishes the device's status.
func (a *App) DeviceChanged(device string) { a.enqueue(update{kind: updateStatus, device: device}) }

// DeviceDeleted clears the device's retained status.
func (a *App) DeviceDeleted(d store.Device) {
	a.mu.Lock()
	a.clears[d.PubKey] = d.Program
	a.mu.Unlock()
	a.enqueue(update{kind: updateClear, device: d.PubKey, program: d.Program})
}

// CommandChanged publishes the command's state.
func (a *App) CommandChanged(device string, id uint32) {
	a.enqueue(update{kind: updateCommand, device: device, command: id})
}

// EventsAdded publishes the new events.
func (a *App) EventsAdded() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// Resync republishes what a broker that lost its state needs: every
// device's status, the clears not yet sent, and any unsent events. Call it
// on every connect.
func (a *App) Resync() {
	a.enqueue(update{kind: updateAll})
	a.EventsAdded()
}

func (a *App) enqueue(u update) {
	select {
	case a.updates <- u:
	default:
		a.log.Warn("app update dropped: queue full", "device", short(u.device))
	}
}

// Run publishes until ctx is done.
func (a *App) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Go(func() { a.runEvents(ctx) })
	wg.Go(func() { a.runUpdates(ctx) })
	wg.Wait()
}

func (a *App) runEvents(ctx context.Context) {
	cursor, err := a.loadCursor(ctx)
	if err != nil {
		a.log.Error("app events cursor", "err", err)
		return
	}
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		more := false
		if a.pub.Connected() {
			var err error
			cursor, more, err = a.publishEvents(ctx, cursor)
			if err != nil && ctx.Err() == nil {
				a.log.Warn("app events", "err", err)
				more = false
			}
		}
		if more {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-a.wake:
		case <-t.C:
		}
	}
}

// loadCursor returns the last event published. On first use it starts
// after the events already stored: applications backfill history with the
// API rather than having it all published at once.
func (a *App) loadCursor(ctx context.Context) (int64, error) {
	v, err := a.st.Setting(ctx, cursorKey)
	if err != nil {
		return 0, err
	}
	if len(v) == 8 {
		return int64(binary.BigEndian.Uint64(v)), nil
	}
	id, err := a.st.LastEventID(ctx)
	if err != nil {
		return 0, err
	}
	return id, a.saveCursor(ctx, id)
}

func (a *App) saveCursor(ctx context.Context, id int64) error {
	return a.st.SetSetting(ctx, cursorKey, binary.BigEndian.AppendUint64(nil, uint64(id)))
}

// publishEvents publishes a batch of events after cursor, returning the new
// cursor and whether there may be more. The cursor only moves past events
// the broker acknowledged.
func (a *App) publishEvents(ctx context.Context, cursor int64) (int64, bool, error) {
	events, err := a.st.EventsByID(ctx, store.EventQuery{After: cursor, Limit: batch})
	if err != nil {
		return cursor, false, err
	}
	sent := cursor
	for _, e := range events {
		payload, _ := json.Marshal(appconv.Event(e))
		if err = a.pub.PublishMessage(appproto.EventsTopic(e.Program, e.PubKey), 1, false, payload); err != nil {
			break
		}
		sent = e.ID
	}
	if sent != cursor {
		if serr := a.saveCursor(ctx, sent); serr != nil && err == nil {
			err = serr
		}
	}
	return sent, err == nil && len(events) == batch, err
}

func (a *App) runUpdates(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case u := <-a.updates:
			if !a.pub.Connected() {
				continue // statuses and clears are resent on connect
			}
			if err := a.apply(ctx, u); err != nil && ctx.Err() == nil {
				a.log.Warn("app update", "device", short(u.device), "err", err)
			}
		}
	}
}

func (a *App) apply(ctx context.Context, u update) error {
	switch u.kind {
	case updateStatus:
		d, ok, err := a.st.Device(ctx, u.device)
		if err != nil || !ok {
			return err // a device deleted since is cleared instead
		}
		return a.publishStatus(d)

	case updateClear:
		return a.clear(u.device, u.program)

	case updateCommand:
		d, ok, err := a.st.Device(ctx, u.device)
		if err != nil || !ok {
			return err
		}
		c, ok, err := a.st.Command(ctx, u.device, u.command)
		if err != nil || !ok {
			return err
		}
		payload, _ := json.Marshal(appconv.Command(d.PubKey, c))
		return a.pub.PublishMessage(appproto.CommandsTopic(d.Program, d.PubKey), 1, false, payload)

	case updateAll:
		a.mu.Lock()
		clears := make(map[string]string, len(a.clears))
		for dev, prog := range a.clears {
			clears[dev] = prog
		}
		a.mu.Unlock()
		for dev, prog := range clears {
			if err := a.clear(dev, prog); err != nil {
				return err
			}
		}
		devices, err := a.st.Devices(ctx)
		if err != nil {
			return err
		}
		for _, d := range devices {
			if err := a.publishStatus(d); err != nil {
				return err
			}
		}
		a.log.Info("app statuses published", "devices", len(devices))
	}
	return nil
}

func (a *App) publishStatus(d store.Device) error {
	payload, _ := json.Marshal(appconv.Device(d))
	return a.pub.PublishMessage(appproto.StatusTopic(d.Program, d.PubKey), 1, true, payload)
}

// clear removes a device's retained status; until it succeeds, Resync
// retries it.
func (a *App) clear(device, program string) error {
	if err := a.pub.PublishMessage(appproto.StatusTopic(program, device), 1, true, nil); err != nil {
		return err
	}
	a.mu.Lock()
	if a.clears[device] == program {
		delete(a.clears, device)
	}
	a.mu.Unlock()
	return nil
}

func short(pubKey string) string { return pubKey[:min(8, len(pubKey))] }
