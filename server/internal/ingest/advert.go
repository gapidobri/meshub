package ingest

import (
	"encoding/binary"
	"errors"

	meshcore "github.com/gapidobri/meshcore-go"
)

// advert is a decoded, signature-verified advertisement.
type advert struct {
	id        meshcore.Identity
	timestamp uint32
	data      meshcore.ParsedAdvert
}

var (
	errMalformedPacket = errors.New("malformed packet")
	errAdvertShort     = errors.New("advert too short")
	errAdvertSignature = errors.New("advert signature invalid")
)

// parseAdvert decodes an advert payload and verifies its signature, the same
// way meshcore.Mesh does on receive.
func parseAdvert(payload []byte) (advert, error) {
	var a advert
	const hdr = meshcore.PubKeySize + 4 + meshcore.SignatureSize
	if len(payload) < hdr {
		return a, errAdvertShort
	}
	copy(a.id.PubKey[:], payload)
	a.timestamp = binary.LittleEndian.Uint32(payload[meshcore.PubKeySize:])
	sig := payload[meshcore.PubKeySize+4 : hdr]
	appData := payload[hdr:]
	if len(appData) > meshcore.MaxAdvertDataSize {
		appData = appData[:meshcore.MaxAdvertDataSize]
	}

	msg := make([]byte, 0, meshcore.PubKeySize+4+len(appData))
	msg = append(msg, a.id.PubKey[:]...)
	msg = binary.LittleEndian.AppendUint32(msg, a.timestamp)
	msg = append(msg, appData...)
	if !a.id.Verify(sig, msg) {
		return a, errAdvertSignature
	}
	a.data = meshcore.ParseAdvertData(appData)
	return a, nil
}
