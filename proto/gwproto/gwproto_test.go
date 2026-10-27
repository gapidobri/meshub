package gwproto

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var testTime = time.Date(2026, 3, 7, 9, 4, 5, 123456000, time.UTC)

func TestPacketEventRx(t *testing.T) {
	e := PacketEvent{
		Origin:      `gw "one"\`,
		OriginID:    "ABCD",
		Time:        testTime,
		Raw:         []byte{0x11, 0x00, 0xDE, 0xAD},
		PayloadType: 4,
		PayloadLen:  2,
		Hash:        []byte{0x01, 0xAB},
		SNR:         -7,
		RSSI:        -110,
		Score:       812,
		AirtimeMs:   330,
	}
	var p Packet
	if err := json.Unmarshal(e.AppendJSON(nil), &p); err != nil {
		t.Fatal(err)
	}
	want := Packet{
		Origin: `gw "one"\`, OriginID: "ABCD", Timestamp: "2026-03-07T09:04:05.123456Z",
		Type: "PACKET", Direction: DirectionRx, Time: "09:04:05", Date: "7/3/2026",
		Len: "4", PacketType: "4", Route: "F", PayloadLen: "2", Raw: "1100DEAD",
		SNR: "-7", RSSI: "-110", Score: "812", Duration: "330", Hash: "01AB",
	}
	if p != want {
		t.Errorf("got  %+v\nwant %+v", p, want)
	}
	if Int(p.RSSI) != -110 {
		t.Errorf("Int(RSSI) = %d", Int(p.RSSI))
	}
}

func TestPacketEventTx(t *testing.T) {
	e := PacketEvent{Time: testTime, TX: true, Direct: true, Raw: []byte{0x02, 0x00}, SNR: 5, Hash: []byte{1}}
	out := e.AppendJSON(nil)
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["direction"] != DirectionTx || m["route"] != "D" {
		t.Errorf("direction/route = %v/%v", m["direction"], m["route"])
	}
	for _, k := range []string{"SNR", "RSSI", "score", "duration", "hash"} {
		if _, ok := m[k]; ok {
			t.Errorf("tx packet has receive field %q", k)
		}
	}
}

func TestStatus(t *testing.T) {
	s := Status{
		Status: StatusOnline, Origin: "gw\n", OriginID: "AB", Radio: "869.618,62.5,8,5",
		Model: "LilyGo T3S3", FirmwareVersion: "1", ClientVersion: "2",
		Stats: &Stats{UptimeSecs: 9, QueueLen: 1, NoiseFloor: -120, FreePackets: 30, MQTTReconnects: 2},
	}
	var got Status
	if err := json.Unmarshal(s.AppendJSON(nil, testTime), &got); err != nil {
		t.Fatal(err)
	}
	if got.Timestamp != "2026-03-07T09:04:05.123456Z" {
		t.Errorf("timestamp = %q", got.Timestamp)
	}
	got.Timestamp = ""
	if *got.Stats != *s.Stats {
		t.Errorf("stats = %+v, want %+v", *got.Stats, *s.Stats)
	}
	got.Stats, s.Stats = nil, nil
	if got != s {
		t.Errorf("got %+v, want %+v", got, s)
	}

	off := Status{Status: StatusOffline}
	if bytes.Contains(off.AppendJSON(nil, testTime), []byte("stats")) {
		t.Error("status without stats encodes a stats object")
	}
}

func TestTxRequestRoundTrip(t *testing.T) {
	in := TxRequest{ID: 0xDEADBEEF, Priority: 2, DelayMs: 1500, Raw: []byte{0x11, 0x00, 0x01}}
	var out TxRequest
	if err := ParseTxRequest(&out, in.AppendBinary(nil)); err != nil {
		t.Fatal(err)
	}
	if out.ID != in.ID || out.Priority != in.Priority || out.DelayMs != in.DelayMs || !bytes.Equal(out.Raw, in.Raw) {
		t.Errorf("got %+v, want %+v", out, in)
	}
}

func TestParseTxRequestErrors(t *testing.T) {
	ok := (&TxRequest{Raw: []byte{0, 0}}).AppendBinary(nil)
	badVer := append([]byte{}, ok...)
	badVer[0] = 9
	long := (&TxRequest{Raw: make([]byte, 256)}).AppendBinary(nil)
	for name, src := range map[string][]byte{"short": ok[:len(ok)-1], "version": badVer, "long": long} {
		var r TxRequest
		if err := ParseTxRequest(&r, src); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestTxOutcome(t *testing.T) {
	o := TxOutcome{ID: 7, Status: TxSent, Hash: []byte{0xFF, 0x01}}
	var r TxResult
	if err := json.Unmarshal(o.AppendJSON(nil, testTime), &r); err != nil {
		t.Fatal(err)
	}
	want := TxResult{ID: 7, Status: TxSent, Timestamp: "2026-03-07T09:04:05.123456Z", Hash: "FF01"}
	if r != want {
		t.Errorf("got %+v, want %+v", r, want)
	}
}

func TestEncodersDoNotAllocate(t *testing.T) {
	buf := make([]byte, 0, 1024)
	e := PacketEvent{Origin: "gw", OriginID: "AB", Time: testTime, Raw: make([]byte, 255), Hash: make([]byte, 8)}
	s := Status{Status: StatusOnline, Stats: &Stats{}}
	o := TxOutcome{ID: 1, Status: TxSent, Hash: make([]byte, 8)}
	n := testing.AllocsPerRun(100, func() {
		buf = e.AppendJSON(buf[:0])
		buf = s.AppendJSON(buf[:0], testTime)
		buf = o.AppendJSON(buf[:0], testTime)
	})
	if n != 0 {
		t.Errorf("encoders allocate %v times per run", n)
	}
}
