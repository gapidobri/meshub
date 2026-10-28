package hubproto

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func sampleConfig() Value {
	audio := Map()
	audio.Set("threshold", Int(40))
	audio.Set("gain", Float(1.5))
	audio.Set("bands", List(Int(200), Int(2000)))
	cfg := Map()
	cfg.Set("name", String("hydrant 7"))
	cfg.Set("audio", audio)
	cfg.Set("enabled", Bool(true))
	cfg.Set("note", Null())
	cfg.Set("offset", Int(-300))
	cfg.Set("big", Int(1<<40))
	cfg.Set("pi", Float(math.Pi))
	return cfg
}

func TestValueAccess(t *testing.T) {
	cfg := sampleConfig()
	if got := cfg.Get("audio.threshold").Int(0); got != 40 {
		t.Errorf("audio.threshold = %d", got)
	}
	if got := cfg.Get("audio.gain").Float(0); got != 1.5 {
		t.Errorf("audio.gain = %v", got)
	}
	if got := cfg.Get("audio.missing").Int(7); got != 7 {
		t.Errorf("missing default = %d", got)
	}
	if got := cfg.Get("name.deeper").String("x"); got != "x" {
		t.Errorf("path through a string = %q", got)
	}
	if got := cfg.Get("audio.bands").Index(1).Int(0); got != 2000 {
		t.Errorf("bands[1] = %d", got)
	}
	if !cfg.Get("enabled").Bool(false) || !cfg.Get("note").IsNull() {
		t.Error("bool or null wrong")
	}
	cfg.Set("enabled", Bool(false))
	if cfg.Get("enabled").Bool(true) {
		t.Error("Set did not replace")
	}
}

func TestCBORRoundTrip(t *testing.T) {
	cfg := sampleConfig()
	enc := cfg.AppendCBOR(nil)
	got, n, err := DecodeCBOR(enc)
	if err != nil || n != len(enc) {
		t.Fatalf("decode: n=%d err=%v", n, err)
	}
	if !got.Equal(cfg) {
		t.Errorf("round trip changed the value")
	}

	// Encoding is independent of insertion order.
	a, b := Map(), Map()
	a.Set("x", Int(1))
	a.Set("y", Int(2))
	b.Set("y", Int(2))
	b.Set("x", Int(1))
	if !bytes.Equal(a.AppendCBOR(nil), b.AppendCBOR(nil)) {
		t.Error("map encoding depends on insertion order")
	}
}

// TestCBORVectors checks against examples from RFC 8949 appendix A.
func TestCBORVectors(t *testing.T) {
	for _, tc := range []struct {
		hex string
		v   Value
	}{
		{"00", Int(0)},
		{"17", Int(23)},
		{"1818", Int(24)},
		{"1903e8", Int(1000)},
		{"1b000000e8d4a51000", Int(1000000000000)},
		{"20", Int(-1)},
		{"3903e7", Int(-1000)},
		{"fa47c35000", Float(100000.0)},
		{"fb3ff199999999999a", Float(1.1)},
		{"f4", Bool(false)},
		{"f5", Bool(true)},
		{"f6", Null()},
		{"6449455446", String("IETF")},
		{"83010203", List(Int(1), Int(2), Int(3))},
	} {
		enc := hex.EncodeToString(tc.v.AppendCBOR(nil))
		if enc != tc.hex {
			t.Errorf("encode %v = %s, want %s", tc.v.Any(), enc, tc.hex)
		}
		raw, _ := hex.DecodeString(tc.hex)
		got, _, err := DecodeCBOR(raw)
		if err != nil || !got.Equal(tc.v) {
			t.Errorf("decode %s = %v, %v", tc.hex, got.Any(), err)
		}
	}
	// Half precision is decoded though never encoded.
	for h, want := range map[string]float64{"f93c00": 1, "f9c400": -4, "f90001": 5.960464477539063e-8, "f93e00": 1.5} {
		raw, _ := hex.DecodeString(h)
		got, _, err := DecodeCBOR(raw)
		if err != nil || got.Float(0) != want {
			t.Errorf("decode %s = %v, %v; want %v", h, got.Any(), err, want)
		}
	}
}

func TestCBORRejectsBadInput(t *testing.T) {
	for _, h := range []string{
		"",                   // empty
		"19",                 // truncated argument
		"62ff",               // truncated string
		"9bffffffffffffffff", // huge array count
		"a10102",             // non-text map key
		"9f",                 // indefinite length
		"1bffffffffffffffff", // uint beyond int64
		"c0",                 // tag
	} {
		raw, _ := hex.DecodeString(h)
		if _, _, err := DecodeCBOR(raw); err == nil {
			t.Errorf("decode %q: no error", h)
		}
	}
	deep := bytes.Repeat([]byte{0x81}, maxDepth+2)
	if _, _, err := DecodeCBOR(append(deep, 0x00)); err == nil {
		t.Error("deep nesting accepted")
	}
}

func TestJSONConversion(t *testing.T) {
	var x any
	if err := json.Unmarshal([]byte(`{"a":1,"b":[true,null,"s"],"c":{"d":2.5}}`), &x); err != nil {
		t.Fatal(err)
	}
	v, ok := FromAny(x)
	if !ok {
		t.Fatal("FromAny failed")
	}
	if v.Get("a").Kind() != KindInt || v.Get("c.d").Float(0) != 2.5 || v.Get("b").Len() != 3 {
		t.Errorf("converted = %v", v.Any())
	}
	back, _ := json.Marshal(v.Any())
	if string(back) != `{"a":1,"b":[true,null,"s"],"c":{"d":2.5}}` {
		t.Errorf("back to JSON = %s", back)
	}
}

func TestMessagesRoundTrip(t *testing.T) {
	data := Map()
	data.Set("duration_s", Int(93))
	msgs := []Message{
		&Join{Program: "hydrant", Firmware: "1.0", Claim: "AB12"},
		&Hello{ConfigVersion: 5, WakeReason: WakePin, BatteryMV: 3700, UptimeS: 12, Firmware: "1.0"},
		&ConfigGet{ConfigVersion: 5, Offset: 128},
		&Events{Events: []Event{{Seq: 17, Time: 1_800_000_000, Kind: "flow", Data: data}, {Seq: 18, Kind: "pin", Data: Null()}}},
		&ConfigStatus{ConfigVersion: 5, Status: ConfigRejected, Reason: "interval_s must be positive"},
		&CommandGet{},
		&CommandResult{ID: 7, Status: CommandOK, Result: data},
		&Fragment{Xfer: 0xBEEF, Total: 400, Offset: 154, Data: bytes.Repeat([]byte{3}, FragmentSize)},
		&JoinReply{Status: JoinPending},
		&HelloReply{Time: 1_800_000_000, PathLen: 0x42, Path: []byte{1, 2, 3, 4}, ConfigVersion: 5, ConfigLen: 300, ConfigCRC: 0xDEADBEEF, Commands: 2},
		&ConfigChunk{ConfigVersion: 5, Offset: 256, Data: bytes.Repeat([]byte{7}, 44)},
		&Ack{},
		&Command{ID: 7, Name: "read", Args: data},
		&Error{Code: ErrConfigChanged},
	}
	for _, m := range msgs {
		enc, err := Encode(nil, 99, m, MaxPlaintext)
		if err != nil {
			t.Fatalf("%T: %v", m, err)
		}
		// Datagrams arrive zero padded to the AES block size.
		padded := append(enc, make([]byte, 16-len(enc)%16)...)
		seq, got, err := Decode(padded)
		if err != nil {
			t.Fatalf("%T: decode: %v", m, err)
		}
		if seq != 99 || got.Type() != m.Type() {
			t.Errorf("%T: seq %d type %x", m, seq, got.Type())
		}
		if !messageEqual(got, m) {
			t.Errorf("%T: got %+v, want %+v", m, got, m)
		}
	}
}

// messageEqual compares messages, with Values compared by Equal.
func messageEqual(a, b Message) bool {
	switch a := a.(type) {
	case *Events:
		b, ok := b.(*Events)
		if !ok || len(a.Events) != len(b.Events) {
			return false
		}
		for i, e := range a.Events {
			f := b.Events[i]
			if e.Seq != f.Seq || e.Time != f.Time || e.Kind != f.Kind || !e.Data.Equal(f.Data) {
				return false
			}
		}
		return true
	case *CommandResult:
		b, ok := b.(*CommandResult)
		return ok && a.ID == b.ID && a.Status == b.Status && a.Result.Equal(b.Result)
	case *Command:
		b, ok := b.(*Command)
		return ok && a.ID == b.ID && a.Name == b.Name && a.Args.Equal(b.Args)
	}
	return reflect.DeepEqual(a, b)
}

func TestFragmentReassembly(t *testing.T) {
	big := Map()
	big.Set("blob", String(string(bytes.Repeat([]byte{'x'}, 500))))
	m := &Events{Events: []Event{{Seq: 3, Kind: "big", Data: big}}}
	if _, err := Encode(nil, 1, m, MaxPlaintext); err != ErrTooLarge {
		t.Fatalf("Encode = %v, want ErrTooLarge", err)
	}
	body, err := AppendMessage(nil, m)
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	for off := 0; off < len(body); off += FragmentSize {
		f := &Fragment{Xfer: 1, Total: uint16(len(body)), Offset: uint16(off), Data: body[off:min(off+FragmentSize, len(body))]}
		enc, err := Encode(nil, uint32(off), f, MaxPlaintext)
		if err != nil {
			t.Fatalf("fragment at %d: %v", off, err)
		}
		_, dec, err := Decode(enc)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, dec.(*Fragment).Data...)
	}
	back, err := DecodeMessage(got)
	if err != nil || !messageEqual(back, m) {
		t.Errorf("reassembled = %+v, %v", back, err)
	}
}

func TestEncodeLimits(t *testing.T) {
	long := string(bytes.Repeat([]byte{'x'}, 256))
	if _, err := Encode(nil, 1, &Join{Program: long}, MaxAnonPlaintext); err == nil {
		t.Error("256-byte string accepted")
	}
	big := Map()
	big.Set("blob", String(string(bytes.Repeat([]byte{'x'}, 200))))
	if _, err := Encode(nil, 1, &Events{Events: []Event{{Kind: "k", Data: big}}}, MaxPlaintext); err != ErrTooLarge {
		t.Error("oversized event accepted")
	}
	if _, err := Encode(nil, 1, &HelloReply{PathLen: 2, Path: []byte{1}}, MaxPlaintext); err == nil {
		t.Error("path not matching its length accepted")
	}
	if _, err := Encode(nil, 1, &Events{Events: make([]Event, 256)}, 1<<16); err == nil {
		t.Error("256 events accepted")
	}
	// The largest chunk and fragment fit a datagram.
	if _, err := Encode(nil, 1, &ConfigChunk{Data: make([]byte, ChunkSize)}, MaxPlaintext); err != nil {
		t.Errorf("full chunk: %v", err)
	}
	if _, err := Encode(nil, 1, &Fragment{Data: make([]byte, FragmentSize)}, MaxPlaintext); err != nil {
		t.Errorf("full fragment: %v", err)
	}
	if _, err := Encode(nil, 1, &Fragment{Data: make([]byte, FragmentSize+1)}, 1<<16); err == nil {
		t.Error("oversized fragment accepted")
	}
}

func TestDecodeErrors(t *testing.T) {
	ok, _ := Encode(nil, 1, &Hello{Firmware: "x"}, MaxPlaintext)
	for name, src := range map[string][]byte{
		"short":     ok[:4],
		"version":   append([]byte{1, 0, 0, 0, 1}, ok[5:]...),
		"type":      append(append([]byte{}, ok[:5]...), 0x55),
		"truncated": ok[:len(ok)-2],
	} {
		if _, _, err := Decode(src); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
