package flashstore

import "errors"

// MemDevice is a BlockDevice in RAM with flash semantics (erase to 0xFF,
// aligned writes), for running nodes on a host in tests and simulations.
type MemDevice struct {
	mem        []byte
	writeBlock int64
	eraseBlock int64
}

// NewMemDevice returns an erased device of size bytes.
func NewMemDevice(size, writeBlock, eraseBlock int64) *MemDevice {
	d := &MemDevice{mem: make([]byte, size), writeBlock: writeBlock, eraseBlock: eraseBlock}
	for i := range d.mem {
		d.mem[i] = 0xFF
	}
	return d
}

var errUnaligned = errors.New("flashstore: unaligned write")

func (d *MemDevice) ReadAt(p []byte, off int64) (int, error) { return copy(p, d.mem[off:]), nil }

func (d *MemDevice) WriteAt(p []byte, off int64) (int, error) {
	if off%d.writeBlock != 0 || int64(len(p))%d.writeBlock != 0 {
		return 0, errUnaligned
	}
	for i, b := range p {
		d.mem[off+int64(i)] &= b // flash can only clear bits
	}
	return len(p), nil
}

func (d *MemDevice) Size() int64           { return int64(len(d.mem)) }
func (d *MemDevice) WriteBlockSize() int64 { return d.writeBlock }
func (d *MemDevice) EraseBlockSize() int64 { return d.eraseBlock }

func (d *MemDevice) EraseBlocks(start, length int64) error {
	for i := start * d.eraseBlock; i < (start+length)*d.eraseBlock; i++ {
		d.mem[i] = 0xFF
	}
	return nil
}
