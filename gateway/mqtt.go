package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/gapidobri/meshub/proto/gwproto"
	mqtt "github.com/soypat/natiu-mqtt"
)

// connectTimeout bounds connecting and subscribing; a variable for tests.
var connectTimeout = 10 * time.Second

const (
	keepAlive  = 60 * time.Second
	minBackoff = time.Second
	maxBackoff = time.Minute
)

var errPingTimeout = errors.New("gateway: mqtt ping timeout")

// RunMQTT keeps a broker session open, reconnecting with backoff, until ctx
// is done. dial opens a new transport to the broker.
func (g *Gateway) RunMQTT(ctx context.Context, dial func() (net.Conn, error)) {
	backoff := minBackoff
	offlineSince := time.Now()
	for ctx.Err() == nil {
		began := time.Now()
		err := g.session(ctx, dial)
		if g.connected.Swap(false) {
			offlineSince = time.Now()
		}
		println("gateway: mqtt:", err.Error())
		g.reconnects.Add(1)

		if time.Since(began) > maxBackoff {
			backoff = minBackoff // the session was healthy for a while
		}
		if g.cfg.OnOffline != nil {
			g.cfg.OnOffline(time.Since(offlineSince))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// session runs one broker connection until it fails.
func (g *Gateway) session(ctx context.Context, dial func() (net.Conn, error)) error {
	conn, err := dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	client := mqtt.NewClient(mqtt.ClientConfig{
		Decoder: mqtt.DecoderNoAlloc{UserBuffer: g.mqttRx[:]},
		OnPub:   g.onPublish,
	})

	var vc mqtt.VariablesConnect
	vc.SetDefaultMQTT([]byte(g.clientID))
	vc.KeepAlive = uint16(keepAlive / time.Second)
	if g.cfg.Username != "" {
		vc.Username = []byte(g.cfg.Username)
		vc.Password = []byte(g.cfg.Password)
	}
	offline := gwproto.Status{Status: gwproto.StatusOffline}
	g.fillStatus(&offline)
	vc.WillTopic = g.topicStatusB
	vc.WillMessage = offline.AppendJSON(nil, time.Now())
	vc.WillRetain = true

	// Connect and Subscribe only check their context between reads, so bound
	// the reads themselves.
	conn.SetReadDeadline(time.Now().Add(connectTimeout))
	cctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	if err := client.Connect(cctx, conn, &vc); err != nil {
		return err
	}
	err = client.Subscribe(cctx, mqtt.VariablesSubscribe{
		PacketIdentifier: 1,
		TopicFilters:     []mqtt.SubscribeRequest{{TopicFilter: []byte(g.topicTx), QoS: mqtt.QoS0}},
	})
	if err != nil {
		return err
	}

	if err := g.publishStatus(client); err != nil {
		return err
	}
	g.connected.Store(true)
	println("gateway: mqtt connected as", g.clientID)

	readErr := make(chan error, 1)
	go func() {
		for {
			// A rolling deadline rather than none: lneto ignores a zero
			// deadline, so the connect deadline could never be cleared.
			// Pings keep reads well inside it on a healthy connection.
			conn.SetReadDeadline(time.Now().Add(2 * keepAlive))
			if err := client.HandleNext(); err != nil {
				readErr <- err
				return
			}
			if !client.IsConnected() {
				readErr <- client.Err()
				return
			}
		}
	}()

	ping := time.NewTicker(keepAlive / 2)
	defer ping.Stop()
	status := time.NewTicker(g.cfg.StatusInterval)
	defer status.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readErr:
			return err
		case ev := <-g.uplink:
			if err := g.publishPacket(client, &ev); err != nil {
				return err
			}
		case o := <-g.outcomes:
			if err := g.publishOutcome(client, &o); err != nil {
				return err
			}
		case <-ping.C:
			if client.AwaitingPingresp() {
				return errPingTimeout
			}
			if err := client.StartPing(); err != nil {
				return err
			}
		case <-status.C:
			if err := g.publishStatus(client); err != nil {
				return err
			}
		}
	}
}

// onPublish handles a message on the transmit topic. It runs on the reader
// goroutine and must not call client methods.
func (g *Gateway) onPublish(_ mqtt.Header, vp mqtt.VariablesPublish, r io.Reader) error {
	n, err := io.ReadFull(r, g.pubBuf[:])
	if err == nil {
		io.Copy(io.Discard, r) // filled the buffer: longer than any valid request
	} else if err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil // the decoder reports transport errors itself
	}
	if string(vp.TopicName) != g.topicTx {
		return nil
	}

	var req gwproto.TxRequest
	if perr := gwproto.ParseTxRequest(&req, g.pubBuf[:n]); perr != nil {
		var id uint32
		if n >= 8 {
			id = uint32(g.pubBuf[4]) | uint32(g.pubBuf[5])<<8 | uint32(g.pubBuf[6])<<16 | uint32(g.pubBuf[7])<<24
		}
		g.queueOutcome(txOutcome{id: id, status: gwproto.TxInvalid})
		return nil
	}
	r2 := txRequest{id: req.ID, priority: req.Priority, delayMs: req.DelayMs}
	r2.n = uint8(copy(r2.raw[:], req.Raw))
	select {
	case g.txReqs <- r2:
	default:
		g.queueOutcome(txOutcome{id: req.ID, status: gwproto.TxFull})
	}
	return nil
}

func (g *Gateway) fillStatus(s *gwproto.Status) {
	s.Origin = g.cfg.Name
	s.OriginID = g.pubKey
	s.Radio = g.cfg.Radio
	s.Model = g.cfg.Model
	s.FirmwareVersion = g.cfg.FirmwareVersion
	s.ClientVersion = clientVersion
}

func (g *Gateway) publishStatus(c *mqtt.Client) error {
	st := g.Stats()
	s := gwproto.Status{Status: gwproto.StatusOnline, Stats: &st}
	g.fillStatus(&s)
	g.txBuf = s.AppendJSON(g.txBuf[:0], time.Now())
	return publish(c, g.topicStatusB, g.txBuf, true)
}

func (g *Gateway) publishPacket(c *mqtt.Client, ev *uplinkEvent) error {
	e := &g.evOut
	*e = gwproto.PacketEvent{
		Origin:      g.cfg.Name,
		OriginID:    g.pubKey,
		Time:        ev.t,
		TX:          ev.tx,
		Raw:         ev.raw[:ev.n],
		PayloadType: ev.payloadType,
		Direct:      ev.direct,
		PayloadLen:  int(ev.payloadLen),
		Hash:        ev.hash[:],
		SNR:         int(ev.snr),
		RSSI:        int(ev.rssi),
		Score:       int(ev.score),
		AirtimeMs:   ev.airtimeMs,
	}
	g.txBuf = e.AppendJSON(g.txBuf[:0])
	return publish(c, g.topicPacketsB, g.txBuf, false)
}

func (g *Gateway) publishOutcome(c *mqtt.Client, o *txOutcome) error {
	out := gwproto.TxOutcome{ID: o.id, Status: o.status}
	if o.hasHash {
		out.Hash = o.hash[:]
	}
	g.txBuf = out.AppendJSON(g.txBuf[:0], time.Now())
	return publish(c, g.topicTxResultB, g.txBuf, false)
}

func publish(c *mqtt.Client, topic, payload []byte, retain bool) error {
	flags, _ := mqtt.NewPublishFlags(mqtt.QoS0, false, retain)
	// QoS 0 publishes carry no packet identifier on the wire, but natiu
	// rejects a zero one.
	return c.PublishPayload(flags, mqtt.VariablesPublish{TopicName: topic, PacketIdentifier: 1}, payload)
}
