package hubproto

import (
	"encoding/binary"
	"errors"
	"math"
)

// CBOR major types.
const (
	majorUint   = 0
	majorNegInt = 1
	majorText   = 3
	majorArray  = 4
	majorMap    = 5
	majorSimple = 7
)

const (
	cborFalse   = 0xF4
	cborTrue    = 0xF5
	cborNull    = 0xF6
	cborFloat16 = 0xF9
	cborFloat32 = 0xFA
	cborFloat64 = 0xFB
)

// maxDepth bounds nesting when decoding untrusted input.
const maxDepth = 16

var (
	errCBORShort    = errors.New("hubproto: truncated CBOR")
	errCBORDepth    = errors.New("hubproto: CBOR nested too deeply")
	errCBORType     = errors.New("hubproto: unsupported CBOR item")
	errCBORMapKey   = errors.New("hubproto: CBOR map key is not text")
	errCBOROverflow = errors.New("hubproto: CBOR integer out of range")
)

func appendHead(dst []byte, major byte, n uint64) []byte {
	m := major << 5
	switch {
	case n < 24:
		return append(dst, m|byte(n))
	case n <= math.MaxUint8:
		return append(dst, m|24, byte(n))
	case n <= math.MaxUint16:
		return binary.BigEndian.AppendUint16(append(dst, m|25), uint16(n))
	case n <= math.MaxUint32:
		return binary.BigEndian.AppendUint32(append(dst, m|26), uint32(n))
	default:
		return binary.BigEndian.AppendUint64(append(dst, m|27), n)
	}
}

// AppendCBOR appends the CBOR encoding of v. Map keys are written sorted, so
// equal values encode to equal bytes.
func (v Value) AppendCBOR(dst []byte) []byte {
	switch v.kind {
	case KindNull:
		return append(dst, cborNull)
	case KindBool:
		if v.b {
			return append(dst, cborTrue)
		}
		return append(dst, cborFalse)
	case KindInt:
		if v.i >= 0 {
			return appendHead(dst, majorUint, uint64(v.i))
		}
		return appendHead(dst, majorNegInt, uint64(^v.i))
	case KindFloat:
		if f32 := float32(v.f); float64(f32) == v.f {
			return binary.BigEndian.AppendUint32(append(dst, cborFloat32), math.Float32bits(f32))
		}
		return binary.BigEndian.AppendUint64(append(dst, cborFloat64), math.Float64bits(v.f))
	case KindString:
		return append(appendHead(dst, majorText, uint64(len(v.s))), v.s...)
	case KindList:
		dst = appendHead(dst, majorArray, uint64(len(v.items)))
		for _, e := range v.items {
			dst = e.AppendCBOR(dst)
		}
		return dst
	case KindMap:
		dst = appendHead(dst, majorMap, uint64(len(v.items)))
		order := make([]int, len(v.keys))
		for i := range order {
			order[i] = i
		}
		for i := 1; i < len(order); i++ {
			for j := i; j > 0 && v.keys[order[j]] < v.keys[order[j-1]]; j-- {
				order[j], order[j-1] = order[j-1], order[j]
			}
		}
		for _, i := range order {
			dst = append(appendHead(dst, majorText, uint64(len(v.keys[i]))), v.keys[i]...)
			dst = v.items[i].AppendCBOR(dst)
		}
		return dst
	}
	return dst
}

// DecodeCBOR decodes one CBOR item from src, returning it and the number of
// bytes it used.
func DecodeCBOR(src []byte) (Value, int, error) {
	d := cborDecoder{src: src}
	v, err := d.value(0)
	return v, d.pos, err
}

type cborDecoder struct {
	src []byte
	pos int
}

func (d *cborDecoder) byte() (byte, error) {
	if d.pos >= len(d.src) {
		return 0, errCBORShort
	}
	b := d.src[d.pos]
	d.pos++
	return b, nil
}

func (d *cborDecoder) take(n uint64) ([]byte, error) {
	if n > uint64(len(d.src)-d.pos) {
		return nil, errCBORShort
	}
	b := d.src[d.pos : d.pos+int(n)]
	d.pos += int(n)
	return b, nil
}

// head reads an item head, returning its major type, additional info and
// argument.
func (d *cborDecoder) head() (major, info byte, arg uint64, err error) {
	b, err := d.byte()
	if err != nil {
		return 0, 0, 0, err
	}
	major, info = b>>5, b&0x1F
	switch {
	case info < 24:
		return major, info, uint64(info), nil
	case info <= 27:
		n := uint64(1) << (info - 24)
		raw, err := d.take(n)
		if err != nil {
			return 0, 0, 0, err
		}
		for _, c := range raw {
			arg = arg<<8 | uint64(c)
		}
		return major, info, arg, nil
	}
	return 0, 0, 0, errCBORType // reserved, or indefinite length
}

func (d *cborDecoder) text() (string, error) {
	major, _, n, err := d.head()
	if err != nil {
		return "", err
	}
	if major != majorText {
		return "", errCBORMapKey
	}
	b, err := d.take(n)
	return string(b), err
}

func (d *cborDecoder) value(depth int) (Value, error) {
	if depth > maxDepth {
		return Value{}, errCBORDepth
	}
	major, info, arg, err := d.head()
	if err != nil {
		return Value{}, err
	}
	switch major {
	case majorUint:
		if arg > math.MaxInt64 {
			return Value{}, errCBOROverflow
		}
		return Int(int64(arg)), nil
	case majorNegInt:
		if arg > math.MaxInt64 {
			return Value{}, errCBOROverflow
		}
		return Int(^int64(arg)), nil
	case majorText:
		b, err := d.take(arg)
		return String(string(b)), err
	case majorArray:
		if arg > uint64(len(d.src)-d.pos) { // every item takes at least a byte
			return Value{}, errCBORShort
		}
		items := make([]Value, arg)
		for i := range items {
			if items[i], err = d.value(depth + 1); err != nil {
				return Value{}, err
			}
		}
		return List(items...), nil
	case majorMap:
		if arg > uint64(len(d.src)-d.pos)/2 {
			return Value{}, errCBORShort
		}
		m := Value{kind: KindMap, keys: make([]string, 0, arg), items: make([]Value, 0, arg)}
		for i := uint64(0); i < arg; i++ {
			k, err := d.text()
			if err != nil {
				return Value{}, err
			}
			e, err := d.value(depth + 1)
			if err != nil {
				return Value{}, err
			}
			m.Set(k, e)
		}
		return m, nil
	case majorSimple:
		switch info {
		case 20:
			return Bool(false), nil
		case 21:
			return Bool(true), nil
		case 22:
			return Null(), nil
		case 25:
			return Float(float64(halfToFloat32(uint16(arg)))), nil
		case 26:
			return Float(float64(math.Float32frombits(uint32(arg)))), nil
		case 27:
			return Float(math.Float64frombits(arg)), nil
		}
	}
	return Value{}, errCBORType
}

// halfToFloat32 converts an IEEE 754 half-precision float.
func halfToFloat32(h uint16) float32 {
	sign := uint32(h>>15) << 31
	exp := uint32(h>>10) & 0x1F
	frac := uint32(h) & 0x3FF
	switch exp {
	case 0:
		if frac == 0 {
			return math.Float32frombits(sign)
		}
		// subnormal: normalize
		e := uint32(127 - 15 + 1)
		for frac&0x400 == 0 {
			frac <<= 1
			e--
		}
		return math.Float32frombits(sign | e<<23 | (frac&0x3FF)<<13)
	case 0x1F:
		return math.Float32frombits(sign | 0xFF<<23 | frac<<13)
	}
	return math.Float32frombits(sign | (exp+127-15)<<23 | frac<<13)
}
