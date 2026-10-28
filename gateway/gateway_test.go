package gateway

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/proto/gwproto"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
)

// fakeRadio delivers injected frames and records transmissions.
type fakeRadio struct {
	meshcore.RadioBase
	mu   sync.Mutex
	rx   [][]byte
	sent [][]byte
}

func (r *fakeRadio) inject(raw []byte) {
	r.mu.Lock()
	r.rx = append(r.rx, append([]byte(nil), raw...))
	r.mu.Unlock()
}

func (r *fakeRadio) sentFrames() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]byte(nil), r.sent...)
}

func (r *fakeRadio) RecvRaw(buf []byte) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.rx) == 0 {
		return 0
	}
	n := copy(buf, r.rx[0])
	r.rx = r.rx[1:]
	return n
}

func (r *fakeRadio) StartSendRaw(b []byte) bool {
	r.mu.Lock()
	r.sent = append(r.sent, append([]byte(nil), b...))
	r.mu.Unlock()
	return true
}

func (r *fakeRadio) EstAirtimeFor(n int) uint32             { return uint32(n) }
func (r *fakeRadio) PacketScore(snr float32, n int) float32 { return meshcore.PacketScore(snr, n) }
func (r *fakeRadio) IsSendComplete() bool                   { return true }
func (r *fakeRadio) OnSendFinished()                        {}
func (r *fakeRadio) IsInRecvMode() bool                     { return true }
func (r *fakeRadio) LastSNR() float32                       { return 6.25 }
func (r *fakeRadio) LastRSSI() float32                      { return -97 }

// lnetoConn mimics lneto's sockets, which ignore a zero (clearing) deadline,
// so a deadline once set can only be replaced.
type lnetoConn struct{ net.Conn }

func (c lnetoConn) SetReadDeadline(t time.Time) error {
	if t.IsZero() {
		return nil
	}
	return c.Conn.SetReadDeadline(t)
}

type seqRNG struct{ n byte }

func (s *seqRNG) Random(dst []byte) {
	for i := range dst {
		s.n++
		dst[i] = s.n
	}
}

type message struct {
	topic   string
	payload []byte
	retain  bool
}

// inbox collects every message the broker routes, so tests can wait for one
// regardless of the order they arrive in.
type inbox struct {
	mu   sync.Mutex
	msgs []message
}

func (b *inbox) add(m message) {
	b.mu.Lock()
	b.msgs = append(b.msgs, m)
	b.mu.Unlock()
}

// await returns the first message on topic accepted by match, waiting for
// one to arrive. A matched message is consumed.
func (b *inbox) await(t *testing.T, topic string, match func(message) bool) message {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		for i, m := range b.msgs {
			if m.topic == topic && (match == nil || match(m)) {
				b.msgs = append(b.msgs[:i], b.msgs[i+1:]...)
				b.mu.Unlock()
				return m
			}
		}
		b.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no matching message on %s", topic)
	return message{}
}

// startBroker runs an in-process broker and returns its address and an
// inbox of every meshcore/ and meshub/ message published to it.
func startBroker(t *testing.T) (string, *mochi.Server, *inbox) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()

	srv := mochi.New(&mochi.Options{
		InlineClient: true,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatal(err)
	}
	if err := srv.AddListener(listeners.NewTCP(listeners.Config{ID: "t", Address: addr})); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })

	box := &inbox{}
	for i, filter := range []string{"meshcore/#", "meshub/#"} {
		err := srv.Subscribe(filter, i+1, func(_ *mochi.Client, _ packets.Subscription, pk packets.Packet) {
			box.add(message{pk.TopicName, append([]byte(nil), pk.Payload...), pk.FixedHeader.Retain})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return addr, srv, box
}

func TestGatewayEndToEnd(t *testing.T) {
	addr, srv, box := startBroker(t)
	defer func(d time.Duration) { connectTimeout = d }(connectTimeout)
	connectTimeout = 200 * time.Millisecond

	radio := &fakeRadio{}
	ms := meshcore.NewSystemMillis()
	id := meshcore.LocalIdentityFromSeed(bytes.Repeat([]byte{7}, 32))
	const pool = 8
	gw := New(Config{
		Name: "test gw", IATA: "LJU", Identity: id,
		Model: "fake", FirmwareVersion: "test", Radio: "869.618,62.5,8,5",
	}, meshcore.Config{
		Radio:   radio,
		Millis:  ms,
		RNG:     &seqRNG{},
		RTC:     meshcore.NewVolatileRTCClock(ms),
		Packets: meshcore.NewStaticPoolPacketManager(pool),
		Tables:  &meshcore.SimpleMeshTables{},
	}, pool)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() { cancel(); <-done })
	gw.Begin()
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			gw.Loop()
			time.Sleep(time.Millisecond)
		}
	}()
	go gw.RunMQTT(ctx, func() (net.Conn, error) {
		c, err := net.Dial("tcp", addr)
		return lnetoConn{c}, err
	})

	pk := gw.PublicKey()
	topicPackets := "meshcore/LJU/" + pk + "/packets"

	st := box.await(t, "meshcore/LJU/"+pk+"/status", nil)
	var status gwproto.Status
	if err := json.Unmarshal(st.payload, &status); err != nil {
		t.Fatal(err)
	}
	if !st.retain || status.Status != gwproto.StatusOnline || status.OriginID != pk || status.Stats == nil {
		t.Fatalf("status = %+v (retain %v)", status, st.retain)
	}

	// Received packets are published.
	rx := []byte{meshcore.PayloadTypeRawCustom<<meshcore.PHTypeShift | meshcore.RouteTypeFlood, 0x00, 0xCA, 0xFE}
	radio.inject(rx)
	var up gwproto.Packet
	box.await(t, topicPackets, func(m message) bool {
		return json.Unmarshal(m.payload, &up) == nil && up.Direction == gwproto.DirectionRx
	})
	if up.Raw != strings.ToUpper(hex.EncodeToString(rx)) || up.PacketType != "15" || up.Route != "F" ||
		up.SNR != "6" || up.RSSI != "-97" || up.Hash == "" || up.Origin != "test gw" {
		t.Errorf("rx uplink = %+v", up)
	}

	// Transmit requests go out on air and are reported, also once the
	// connect deadline has passed.
	time.Sleep(2 * connectTimeout)
	tx := []byte{meshcore.PayloadTypeRawCustom<<meshcore.PHTypeShift | meshcore.RouteTypeDirect, 0x00, 0x01, 0x02, 0x03}
	req := gwproto.TxRequest{ID: 42, Raw: tx}
	if err := srv.Publish("meshub/gw/"+pk+"/tx", req.AppendBinary(nil), false, 0); err != nil {
		t.Fatal(err)
	}
	var res gwproto.TxResult
	box.await(t, "meshub/gw/"+pk+"/tx/result", func(m message) bool {
		return json.Unmarshal(m.payload, &res) == nil && res.ID == 42
	})
	if res.Status != gwproto.TxSent || res.Hash == "" {
		t.Errorf("tx result = %+v", res)
	}
	if sent := radio.sentFrames(); len(sent) != 1 || !bytes.Equal(sent[0], tx) {
		t.Errorf("radio sent %x, want [%x]", sent, tx)
	}
	box.await(t, topicPackets, func(m message) bool {
		return json.Unmarshal(m.payload, &up) == nil && up.Direction == gwproto.DirectionTx
	})
	if up.Raw != strings.ToUpper(hex.EncodeToString(tx)) || up.Route != "D" {
		t.Errorf("tx uplink = %+v", up)
	}
	if a := gw.Activity(); a.RxPackets != 1 || a.TxPackets != 1 || a.LastRx.IsZero() || a.LastRSSI != -97 {
		t.Errorf("activity = %+v", a)
	}

	// Malformed requests are rejected.
	bad := (&gwproto.TxRequest{ID: 43, Raw: []byte{0x3C, 0xFF}}).AppendBinary(nil) // path length too long
	if err := srv.Publish("meshub/gw/"+pk+"/tx", bad, false, 0); err != nil {
		t.Fatal(err)
	}
	box.await(t, "meshub/gw/"+pk+"/tx/result", func(m message) bool {
		return json.Unmarshal(m.payload, &res) == nil && res.ID == 43
	})
	if res.Status != gwproto.TxInvalid {
		t.Errorf("bad tx result = %+v", res)
	}
}
