package gwproto

import (
	"encoding/binary"
	"errors"
	"time"
)

// TxRequestVersion is the current TxRequest encoding version.
const TxRequestVersion = 1

// txHeaderLen is the size of the TxRequest header before the raw packet.
const txHeaderLen = 8

// MaxTxRequestLen is the largest encoded TxRequest, for a packet of
// MaxTransUnit (255) bytes.
const MaxTxRequestLen = txHeaderLen + 255

// TxRequest asks a gateway to transmit a raw packet. It is encoded as:
//
//	[0]    version (TxRequestVersion)
//	[1]    priority (lower is more important)
//	[2:4]  delay before transmitting, milliseconds, little-endian
//	[4:8]  request ID echoed in the TxResult, little-endian
//	[8:]   the packet in wire format
type TxRequest struct {
	ID       uint32
	Priority uint8
	DelayMs  uint16
	Raw      []byte
}

// AppendBinary appends the encoding of r.
func (r *TxRequest) AppendBinary(dst []byte) []byte {
	dst = append(dst, TxRequestVersion, r.Priority)
	dst = binary.LittleEndian.AppendUint16(dst, r.DelayMs)
	dst = binary.LittleEndian.AppendUint32(dst, r.ID)
	return append(dst, r.Raw...)
}

var (
	errTxShort   = errors.New("gwproto: tx request too short")
	errTxVersion = errors.New("gwproto: unsupported tx request version")
	errTxLong    = errors.New("gwproto: tx request too long")
)

// ParseTxRequest decodes a TxRequest. r.Raw aliases src.
func ParseTxRequest(r *TxRequest, src []byte) error {
	if len(src) < txHeaderLen+2 { // the smallest packet is a header and path length byte
		return errTxShort
	}
	if src[0] != TxRequestVersion {
		return errTxVersion
	}
	if len(src) > MaxTxRequestLen {
		return errTxLong
	}
	r.Priority = src[1]
	r.DelayMs = binary.LittleEndian.Uint16(src[2:])
	r.ID = binary.LittleEndian.Uint32(src[4:])
	r.Raw = src[txHeaderLen:]
	return nil
}

// Transmit outcomes reported in TxResult.Status.
const (
	TxSent    = "sent"    // the packet went out on air
	TxFailed  = "failed"  // the radio failed or timed out sending it
	TxInvalid = "invalid" // the request or packet could not be parsed
	TxFull    = "full"    // the gateway had no room to queue it
	TxExpired = "expired" // it was queued but never sent
)

// TxResult reports what happened to a TxRequest.
type TxResult struct {
	ID        uint32 `json:"id"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
	Hash      string `json:"hash,omitempty"` // packet hash, when the packet parsed
}

// TxOutcome is a TxResult as the gateway knows it.
type TxOutcome struct {
	ID     uint32
	Status string
	Hash   []byte
}

// AppendJSON appends the TxResult JSON encoding of o, with ts as its
// timestamp.
func (o *TxOutcome) AppendJSON(dst []byte, ts time.Time) []byte {
	dst = append(dst, '{')
	dst = appendUintField(dst, "id", uint64(o.ID))
	dst = appendStrField(dst, "status", o.Status)
	dst = appendTimestamp(dst, "timestamp", ts)
	if len(o.Hash) > 0 {
		dst = append(appendKey(dst, "hash"), '"')
		dst = appendHex(dst, o.Hash)
		dst = append(dst, '"')
	}
	return append(dst, '}')
}
