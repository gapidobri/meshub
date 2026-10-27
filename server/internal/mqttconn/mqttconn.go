// Package mqttconn holds the backend's broker connection, shared by the
// packages that subscribe and publish.
package mqttconn

import (
	"crypto/tls"
	"errors"
	"log/slog"
	"net/url"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Config is how to reach the broker.
type Config struct {
	Broker   string // e.g. "tcp://localhost:1883"
	ClientID string
	Username string
	Password string
}

// Subscription routes messages matching Filter to Handle.
type Subscription struct {
	Filter string
	Handle func(topic string, payload []byte)
}

// Conn is a broker connection that reconnects and resubscribes by itself.
type Conn struct {
	client mqtt.Client
}

// publishTimeout bounds waiting for a publish to be written.
const publishTimeout = 5 * time.Second

var errPublishTimeout = errors.New("mqttconn: publish timed out")

// Connect starts connecting to the broker in the background and returns
// immediately. Subscriptions are (re)made on every connect.
func Connect(cfg Config, log *slog.Logger, subs ...Subscription) *Conn {
	filters := make(map[string]byte, len(subs))
	for _, s := range subs {
		filters[s.Filter] = 0
	}
	route := func(_ mqtt.Client, m mqtt.Message) {
		for _, s := range subs {
			if topicMatches(s.Filter, m.Topic()) {
				s.Handle(m.Topic(), m.Payload())
			}
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.Broker).
		SetClientID(cfg.ClientID).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password).
		SetOrderMatters(true). // a gateway's status updates must apply in order
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetMaxReconnectInterval(time.Minute).
		SetOnConnectHandler(func(c mqtt.Client) {
			// Subscribing on every connect restores the subscriptions after a
			// reconnect and redelivers retained gateway statuses.
			if len(filters) > 0 {
				t := c.SubscribeMultiple(filters, route)
				if t.Wait() && t.Error() != nil {
					log.Error("mqtt subscribe", "err", t.Error())
					return
				}
			}
			log.Info("mqtt connected", "broker", cfg.Broker)
		}).
		SetConnectionAttemptHandler(func(broker *url.URL, tc *tls.Config) *tls.Config {
			log.Info("mqtt connecting", "broker", broker.Redacted())
			return tc
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			log.Warn("mqtt connection lost", "err", err)
		})

	c := &Conn{client: mqtt.NewClient(opts)}
	c.client.Connect() // with ConnectRetry it keeps trying in the background
	return c
}

// Publish sends a QoS 0 message.
func (c *Conn) Publish(topic string, payload []byte) error {
	t := c.client.Publish(topic, 0, false, payload)
	if !t.WaitTimeout(publishTimeout) {
		return errPublishTimeout
	}
	return t.Error()
}

// Close disconnects from the broker.
func (c *Conn) Close() { c.client.Disconnect(250) }

// topicMatches reports whether topic matches an MQTT filter with + and #
// wildcards.
func topicMatches(filter, topic string) bool {
	for {
		f, frest, fmore := cut(filter)
		t, trest, tmore := cut(topic)
		switch {
		case f == "#":
			return true
		case f != "+" && f != t:
			return false
		case !fmore || !tmore:
			return !fmore && !tmore
		}
		filter, topic = frest, trest
	}
}

func cut(s string) (level, rest string, more bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}
