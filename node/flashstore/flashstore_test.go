package flashstore

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

var errPowerLoss = errors.New("power lost")

// fakeFlash behaves like NOR flash: erase sets bytes to 0xFF, writes must be
// aligned and may only touch erased bytes. budget, when not negative, is how
// many more bytes may be written before the power fails.
type fakeFlash struct {
	mem    []byte
	wbs    int64
	ebs    int64
	budget int
}

func newFlash(size, wbs, ebs int64) *fakeFlash {
	f := &fakeFlash{mem: make([]byte, size), wbs: wbs, ebs: ebs, budget: -1}
	for i := range f.mem {
		f.mem[i] = 0xA5 // not erased: the store must erase before use
	}
	return f
}

func (f *fakeFlash) ReadAt(p []byte, off int64) (int, error) { return copy(p, f.mem[off:]), nil }
func (f *fakeFlash) Size() int64                             { return int64(len(f.mem)) }
func (f *fakeFlash) WriteBlockSize() int64                   { return f.wbs }
func (f *fakeFlash) EraseBlockSize() int64                   { return f.ebs }

func (f *fakeFlash) WriteAt(p []byte, off int64) (int, error) {
	if off%f.wbs != 0 || int64(len(p))%f.wbs != 0 {
		return 0, fmt.Errorf("unaligned write at %d len %d", off, len(p))
	}
	for i := range p {
		if f.mem[off+int64(i)] != 0xFF {
			return i, fmt.Errorf("write to unerased byte at %d", off+int64(i))
		}
	}
	for i, b := range p {
		if f.budget == 0 {
			return i, errPowerLoss
		}
		if f.budget > 0 {
			f.budget--
		}
		f.mem[off+int64(i)] = b
	}
	return len(p), nil
}

func (f *fakeFlash) EraseBlocks(start, length int64) error {
	for i := start * f.ebs; i < (start+length)*f.ebs; i++ {
		if f.budget == 0 {
			return errPowerLoss // erase interrupted midway
		}
		f.mem[i] = 0xFF
	}
	return nil
}

func mustOpen(t *testing.T, f *fakeFlash) *Store {
	t.Helper()
	s, err := Open(f, 1)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func get(t *testing.T, s *Store, key string) []byte {
	t.Helper()
	v, err := s.Load(key)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestSaveLoad(t *testing.T) {
	f := newFlash(64*1024+100, 4, 4096) // size not a multiple of the erase block
	s := mustOpen(t, f)
	if v := get(t, s, "missing"); v != nil {
		t.Errorf("missing key = %q", v)
	}
	s.Save("a", []byte("one"))
	s.Save("b", []byte{})
	s.Save("a", []byte("two"))
	s.Save("c", []byte("gone"))
	s.Delete("c")

	for _, s := range []*Store{s, mustOpen(t, f)} { // also after a reboot
		if v := get(t, s, "a"); string(v) != "two" {
			t.Errorf("a = %q", v)
		}
		if v := get(t, s, "b"); v == nil || len(v) != 0 {
			t.Errorf("b = %v, want empty and present", v)
		}
		if v := get(t, s, "c"); v != nil {
			t.Errorf("deleted c = %q", v)
		}
	}
	if s.base != 64*1024-2*4096 {
		t.Errorf("store at %d, want the end of the device", s.base)
	}
}

func TestCompaction(t *testing.T) {
	f := newFlash(16*1024, 4, 4096)
	s := mustOpen(t, f)
	big := bytes.Repeat([]byte{'x'}, 900)
	for i := 0; i < 200; i++ { // many times the region size
		if err := s.Save("state", []byte(fmt.Sprint(i))); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
		if i%10 == 0 {
			if err := s.Save("config", append(big, byte(i))); err != nil {
				t.Fatalf("save config %d: %v", i, err)
			}
		}
	}
	s.Save("id", []byte("identity"))
	r := mustOpen(t, f)
	if v := get(t, r, "state"); string(v) != "199" {
		t.Errorf("state = %q", v)
	}
	if v := get(t, r, "config"); len(v) != 901 || v[900] != 190 {
		t.Errorf("config len %d", len(v))
	}
	if v := get(t, r, "id"); string(v) != "identity" {
		t.Errorf("id = %q", v)
	}
	if err := r.Save("huge", make([]byte, 5000)); !errors.Is(err, ErrFull) {
		t.Errorf("oversized save: %v, want ErrFull", err)
	}
	if v := get(t, r, "state"); string(v) != "199" {
		t.Errorf("state after a failed save = %q", v)
	}
}

// TestPowerLoss cuts the power at every byte of a sequence of saves that
// includes compactions, and checks that every key then holds its value from
// before or after the interrupted save, and that the store keeps working.
func TestPowerLoss(t *testing.T) {
	ops := func(i int) (string, []byte) {
		if i%7 == 0 {
			return "config", bytes.Repeat([]byte{byte(i)}, 700)
		}
		return fmt.Sprintf("k%d", i%3), []byte(fmt.Sprintf("v%d", i))
	}
	const nOps = 40

	// Find how many bytes the whole sequence writes.
	f := newFlash(16*1024, 4, 4096)
	s := mustOpen(t, f)
	f.budget = 1 << 30
	for i := 0; i < nOps; i++ {
		k, v := ops(i)
		s.Save(k, v)
	}
	total := 1<<30 - f.budget

	t.Logf("sequence writes %d bytes", total)
	for cut := 0; cut < total; cut++ {
		f := newFlash(16*1024, 4, 4096)
		s := mustOpen(t, f)
		want := map[string][]byte{} // values of completed saves
		f.budget = cut
		var pendingKey string
		var pendingVal []byte
		for i := 0; i < nOps; i++ {
			k, v := ops(i)
			if err := s.Save(k, v); err != nil {
				pendingKey, pendingVal = k, v
				break
			}
			want[k] = v
		}
		f.budget = -1

		r, err := Open(f, 1)
		if err != nil {
			t.Fatalf("cut %d: reopen: %v", cut, err)
		}
		for k, v := range want {
			got := get(t, r, k)
			if k == pendingKey && bytes.Equal(got, pendingVal) {
				continue // the interrupted save happened to complete
			}
			if !bytes.Equal(got, v) {
				t.Fatalf("cut %d: %s = %q, want %q", cut, k, short(got), short(v))
			}
		}
		if pendingKey != "" {
			if _, had := want[pendingKey]; !had {
				if got := get(t, r, pendingKey); got != nil && !bytes.Equal(got, pendingVal) {
					t.Fatalf("cut %d: new key %s holds %q", cut, pendingKey, short(got))
				}
			}
		}
		if err := r.Save("after", []byte("ok")); err != nil {
			t.Fatalf("cut %d: save after reboot: %v", cut, err)
		}
		if v := get(t, mustOpen(t, f), "after"); string(v) != "ok" {
			t.Fatalf("cut %d: store broken after reboot", cut)
		}
	}
}

func short(b []byte) []byte {
	if len(b) > 12 {
		return append(append([]byte{}, b[:12]...), "..."...)
	}
	return b
}

func TestKeyLimits(t *testing.T) {
	s := mustOpen(t, newFlash(16*1024, 4, 4096))
	if err := s.Save("", []byte("x")); !errors.Is(err, ErrKey) {
		t.Errorf("empty key: %v", err)
	}
	if err := s.Save(string(bytes.Repeat([]byte{'k'}, 256)), nil); !errors.Is(err, ErrKey) {
		t.Errorf("long key: %v", err)
	}
	if _, err := Open(newFlash(4096, 4, 4096), 1); !errors.Is(err, ErrTooSmall) {
		t.Errorf("tiny device: %v", err)
	}
}
