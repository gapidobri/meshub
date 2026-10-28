// Package flashstore is a node.Storage on raw flash, such as TinyGo's
// machine.Flash. It is a small log-structured key-value store that survives
// power loss at any point.
//
// The store uses two equal regions at the end of the device. Saves append a
// checksummed record to the active region. When it is full, the latest value
// of every key is copied to the other region, whose header (with a higher
// generation) is written last, so an interrupted copy leaves the old region
// in charge. A record torn by power loss fails its checksum and is ignored.
//
// Placing the regions at the end matters with machine.Flash, whose area
// starts after the program: a larger firmware moves the start, not the end.
package flashstore

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// BlockDevice is flash memory, as implemented by machine.Flash.
type BlockDevice interface {
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64) (int, error)
	Size() int64
	WriteBlockSize() int64
	EraseBlockSize() int64
	EraseBlocks(start, length int64) error
}

const (
	headerMagic = 0x564B484D // "MHKV"
	recordMagic = 0xA55A
	headerLen   = 12 // magic, generation, CRC
	recordHdr   = 12 // magic u16, key length u8, flags u8, value length u32, CRC u32
	maxKeyLen   = 255
)

var (
	ErrFull     = errors.New("flashstore: data does not fit")
	ErrTooSmall = errors.New("flashstore: device too small")
	ErrKey      = errors.New("flashstore: key empty or too long")
)

// Store is a node.Storage on flash. It is not safe for concurrent use.
type Store struct {
	dev        BlockDevice
	base       int64 // device offset of region 0
	regionSize int64
	align      int64

	active int    // 0 or 1
	gen    uint32 // generation of the active region
	head   int64  // next free offset in the active region
	dirty  bool   // the active region holds a torn record: compact before writing
	index  map[string]entry
}

type entry struct {
	off int64 // of the value, within the active region
	n   int
}

// Open mounts a store on the last 2×regionBlocks erase blocks of dev,
// formatting it if it holds none.
func Open(dev BlockDevice, regionBlocks int) (*Store, error) {
	eb := dev.EraseBlockSize()
	s := &Store{
		dev:        dev,
		regionSize: eb * int64(regionBlocks),
		align:      max(dev.WriteBlockSize(), 4),
	}
	s.base = dev.Size()/eb*eb - 2*s.regionSize
	if regionBlocks < 1 || s.base < 0 {
		return nil, ErrTooSmall
	}

	gens := [2]uint32{}
	valid := [2]bool{}
	for r := 0; r < 2; r++ {
		gens[r], valid[r] = s.readHeader(r)
	}
	switch {
	case valid[0] && valid[1]:
		s.active = 0
		if int32(gens[1]-gens[0]) > 0 {
			s.active = 1
		}
	case valid[0]:
		s.active = 0
	case valid[1]:
		s.active = 1
	default:
		if err := s.format(0, 1); err != nil {
			return nil, err
		}
		s.active = 0
	}
	s.gen = gens[s.active]
	if !valid[0] && !valid[1] {
		s.gen = 1
	}
	return s, s.scan()
}

func (s *Store) regionOff(r int) int64 { return s.base + int64(r)*s.regionSize }

func (s *Store) pad(n int64) int64 { return (n + s.align - 1) / s.align * s.align }

func (s *Store) readHeader(r int) (uint32, bool) {
	var h [headerLen]byte
	if _, err := s.dev.ReadAt(h[:], s.regionOff(r)); err != nil {
		return 0, false
	}
	if binary.LittleEndian.Uint32(h[0:]) != headerMagic || crc32.ChecksumIEEE(h[:8]) != binary.LittleEndian.Uint32(h[8:]) {
		return 0, false
	}
	return binary.LittleEndian.Uint32(h[4:]), true
}

func (s *Store) writeHeader(r int, gen uint32) error {
	h := make([]byte, s.pad(headerLen))
	for i := range h {
		h[i] = 0xFF
	}
	binary.LittleEndian.PutUint32(h[0:], headerMagic)
	binary.LittleEndian.PutUint32(h[4:], gen)
	binary.LittleEndian.PutUint32(h[8:], crc32.ChecksumIEEE(h[:8]))
	_, err := s.dev.WriteAt(h, s.regionOff(r))
	return err
}

// format erases region r and writes its header with generation gen.
func (s *Store) format(r int, gen uint32) error {
	if err := s.erase(r); err != nil {
		return err
	}
	return s.writeHeader(r, gen)
}

func (s *Store) erase(r int) error {
	eb := s.dev.EraseBlockSize()
	return s.dev.EraseBlocks(s.regionOff(r)/eb, s.regionSize/eb)
}

// scan rebuilds the index from the active region.
func (s *Store) scan() error {
	s.index = map[string]entry{}
	s.dirty = false
	off := s.pad(headerLen)
	var hdr [recordHdr]byte
	for off+recordHdr <= s.regionSize {
		if _, err := s.dev.ReadAt(hdr[:], s.regionOff(s.active)+off); err != nil {
			return err
		}
		magic := binary.LittleEndian.Uint16(hdr[0:])
		if magic == 0xFFFF {
			break // erased: end of the log
		}
		klen, flags := int64(hdr[2]), hdr[3]
		vlen := int64(binary.LittleEndian.Uint32(hdr[4:]))
		total := s.pad(recordHdr + klen + vlen)
		if magic != recordMagic || off+total > s.regionSize {
			s.dirty = true
			break
		}
		body := make([]byte, klen+vlen)
		if _, err := s.dev.ReadAt(body, s.regionOff(s.active)+off+recordHdr); err != nil {
			return err
		}
		if recordCRC(hdr[:8], body) != binary.LittleEndian.Uint32(hdr[8:]) {
			s.dirty = true // torn by power loss
			break
		}
		key := string(body[:klen])
		if flags&1 != 0 {
			delete(s.index, key)
		} else {
			s.index[key] = entry{off: off + recordHdr + klen, n: int(vlen)}
		}
		off += total
	}
	s.head = off
	return nil
}

func recordCRC(hdr, body []byte) uint32 {
	return crc32.Update(crc32.ChecksumIEEE(hdr), crc32.IEEETable, body)
}

func (s *Store) encode(key string, data []byte, del bool) []byte {
	rec := make([]byte, s.pad(int64(recordHdr+len(key)+len(data))))
	for i := range rec {
		rec[i] = 0xFF
	}
	binary.LittleEndian.PutUint16(rec[0:], recordMagic)
	rec[2] = byte(len(key))
	if del {
		rec[3] = 1
	} else {
		rec[3] = 0
	}
	binary.LittleEndian.PutUint32(rec[4:], uint32(len(data)))
	copy(rec[recordHdr:], key)
	copy(rec[recordHdr+len(key):], data)
	binary.LittleEndian.PutUint32(rec[8:], recordCRC(rec[:8], rec[recordHdr:recordHdr+len(key)+len(data)]))
	return rec
}

// Load returns the data stored under key, or nil if there is none.
func (s *Store) Load(key string) ([]byte, error) {
	e, ok := s.index[key]
	if !ok {
		return nil, nil
	}
	b := make([]byte, e.n)
	_, err := s.dev.ReadAt(b, s.regionOff(s.active)+e.off)
	return b, err
}

// Save stores data under key.
func (s *Store) Save(key string, data []byte) error { return s.put(key, data, false) }

// Delete removes key.
func (s *Store) Delete(key string) error {
	if _, ok := s.index[key]; !ok {
		return nil
	}
	return s.put(key, nil, true)
}

func (s *Store) put(key string, data []byte, del bool) error {
	if key == "" || len(key) > maxKeyLen {
		return ErrKey
	}
	rec := s.encode(key, data, del)
	if s.dirty || s.head+int64(len(rec)) > s.regionSize {
		return s.compact(key, rec)
	}
	if _, err := s.dev.WriteAt(rec, s.regionOff(s.active)+s.head); err != nil {
		s.dirty = true // the write may have half happened
		return err
	}
	s.apply(key, s.head, len(data), del)
	s.head += int64(len(rec))
	return nil
}

func (s *Store) apply(key string, recOff int64, n int, del bool) {
	if del {
		delete(s.index, key)
		return
	}
	s.index[key] = entry{off: recOff + recordHdr + int64(len(key)), n: n}
}

// compact copies the live records, with rec for key replacing its old
// value, into the other region and makes it active.
func (s *Store) compact(key string, rec []byte) error {
	other := 1 - s.active
	if err := s.erase(other); err != nil {
		return err
	}
	off := s.pad(headerLen)
	newIndex := map[string]entry{}
	write := func(k string, r []byte, n int, del bool) error {
		if off+int64(len(r)) > s.regionSize {
			return ErrFull
		}
		if _, err := s.dev.WriteAt(r, s.regionOff(other)+off); err != nil {
			return err
		}
		if !del {
			newIndex[k] = entry{off: off + recordHdr + int64(len(k)), n: n}
		}
		off += int64(len(r))
		return nil
	}
	for k, e := range s.index {
		if k == key {
			continue
		}
		v := make([]byte, e.n)
		if _, err := s.dev.ReadAt(v, s.regionOff(s.active)+e.off); err != nil {
			return err
		}
		if err := write(k, s.encode(k, v, false), len(v), false); err != nil {
			return err
		}
	}
	del := rec[3]&1 != 0
	if !del {
		if err := write(key, rec, int(binary.LittleEndian.Uint32(rec[4:])), false); err != nil {
			return err
		}
	}
	// The header goes last: until it is written, the old region stays active.
	if err := s.writeHeader(other, s.gen+1); err != nil {
		return err
	}
	s.active, s.gen, s.head, s.index, s.dirty = other, s.gen+1, off, newIndex, false
	return nil
}
