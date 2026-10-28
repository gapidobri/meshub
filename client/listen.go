package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/gapidobri/meshub/proto/appproto"
)

// Subscription is what Listen listens to, and the handlers it calls. The
// handlers run one at a time, in the order the backend published.
type Subscription struct {
	// Broker is the MQTT broker the backend publishes to, such as
	// "tcp://broker:1883" or "ssl://broker:8883".
	Broker   string
	ClientID string // empty for a random one
	Username string
	Password string
	TLS      *tls.Config // for ssl:// and wss:// brokers; nil for the system defaults

	// Program limits the subscription to the devices running it; empty for
	// all devices.
	Program string

	// After is the ID of the last event the application has handled, so
	// that Listen resumes after it; 0 starts at the first event the backend
	// has.
	After int64

	// OnEvent handles each event once, in ID order. If it returns an
	// error, Listen stops and returns it; the event is handled again when
	// Listen is next called with After below its ID. Store the ID of each
	// event handled to resume from it.
	OnEvent func(ctx context.Context, e appproto.Event) error
	// OnStatus gets a device's status whenever it changes, and every
	// device's on connecting.
	OnStatus func(d appproto.Device)
	// OnDeleted gets the public key of a device that was deleted.
	OnDeleted func(pubKey string)
	// OnCommand gets a command whenever it is queued, finished or
	// cancelled. These updates are best effort: Commands has them all.
	OnCommand func(c appproto.Command)

	Logger *slog.Logger // nil for slog.Default()
}

// retryInterval is how long Listen waits to retry a failed backfill.
const retryInterval = 5 * time.Second

// Listen subscribes to the device topics of the backend's broker and calls
// s's handlers until ctx is done or OnEvent fails. It reconnects by itself.
//
// Events reach OnEvent exactly once per call, in ID order, without gaps:
// on every connect Listen subscribes, then fetches from the HTTP API the
// events published while it was not subscribed, and only then hands on
// events as they are published.
func (c *Client) Listen(ctx context.Context, s Subscription) error {
	if s.Broker == "" {
		return errors.New("meshub: Subscription.Broker is required")
	}
	log := s.Logger
	if log == nil {
		log = slog.Default()
	}
	msgs := make(chan mqtt.Message, 256)
	connected := make(chan struct{}, 1)
	filter := appproto.DevicesFilter(s.Program)

	opts := mqtt.NewClientOptions().
		AddBroker(s.Broker).
		SetClientID(s.ClientID).
		SetUsername(s.Username).
		SetPassword(s.Password).
		SetCleanSession(true). // gaps are filled from the API instead
		SetOrderMatters(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetMaxReconnectInterval(time.Minute).
		SetOnConnectHandler(func(mc mqtt.Client) {
			t := mc.Subscribe(filter, 1, func(_ mqtt.Client, m mqtt.Message) {
				select {
				case msgs <- m:
				case <-ctx.Done():
				}
			})
			if t.Wait() && t.Error() != nil {
				log.Error("meshub: subscribe", "filter", filter, "err", t.Error())
				return
			}
			select {
			case connected <- struct{}{}:
			default:
			}
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			log.Warn("meshub: mqtt connection lost", "err", err)
		})
	if s.TLS != nil {
		opts.SetTLSConfig(s.TLS)
	}
	mc := mqtt.NewClient(opts)
	mc.Connect()
	defer mc.Disconnect(250)

	l := &listener{c: c, s: s, log: log, last: s.After}
	var retry <-chan time.Time
	// catchUp backfills, returning OnEvent's error, and on any other
	// error retrying later.
	catchUp := func() error {
		retry = nil
		err := l.backfill(ctx)
		if h := (handlerError{}); errors.As(err, &h) {
			return h.err
		}
		if err != nil && ctx.Err() == nil {
			log.Warn("meshub: fetching missed events", "err", err)
			retry = time.After(retryInterval)
		}
		return nil
	}
	for {
		var err error
		select {
		case <-ctx.Done():
			return nil
		case <-connected:
			l.synced = false
			err = catchUp()
		case <-retry:
			err = catchUp()
		case m := <-msgs:
			err = l.handle(ctx, m.Topic(), m.Payload())
		}
		if err != nil {
			return err
		}
	}
}

type listener struct {
	c   *Client
	s   Subscription
	log *slog.Logger

	last   int64 // ID of the last event handled
	synced bool  // events up to the subscription's start are handled
}

// handlerError is an error from OnEvent, as opposed to one fetching events.
type handlerError struct{ err error }

func (e handlerError) Error() string { return e.err.Error() }

// backfill hands on the events after the last one handled.
func (l *listener) backfill(ctx context.Context) error {
	for e, err := range l.c.EventsAfter(ctx, l.s.Program, l.last) {
		if err != nil {
			return err
		}
		if err := l.event(ctx, e); err != nil {
			return handlerError{err}
		}
	}
	l.synced = true
	return nil
}

func (l *listener) event(ctx context.Context, e appproto.Event) error {
	if e.ID <= l.last {
		return nil // already handled
	}
	if l.s.OnEvent != nil {
		if err := l.s.OnEvent(ctx, e); err != nil {
			return err
		}
	}
	l.last = e.ID
	return nil
}

func (l *listener) handle(ctx context.Context, topic string, payload []byte) error {
	_, pubKey, kind, ok := appproto.ParseTopic(topic)
	if !ok {
		return nil
	}
	switch kind {
	case appproto.KindEvents:
		if !l.synced {
			return nil // the backfill still to come includes it
		}
		var e appproto.Event
		if err := json.Unmarshal(payload, &e); err != nil {
			l.log.Warn("meshub: malformed event", "topic", topic, "err", err)
			return nil
		}
		return l.event(ctx, e)

	case appproto.KindStatus:
		if len(payload) == 0 {
			if l.s.OnDeleted != nil {
				l.s.OnDeleted(pubKey)
			}
			return nil
		}
		var d appproto.Device
		if err := json.Unmarshal(payload, &d); err != nil {
			l.log.Warn("meshub: malformed status", "topic", topic, "err", err)
			return nil
		}
		if l.s.OnStatus != nil {
			l.s.OnStatus(d)
		}

	case appproto.KindCommands:
		var cmd appproto.Command
		if err := json.Unmarshal(payload, &cmd); err != nil {
			l.log.Warn("meshub: malformed command", "topic", topic, "err", err)
			return nil
		}
		if l.s.OnCommand != nil {
			l.s.OnCommand(cmd)
		}
	}
	return nil
}
