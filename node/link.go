package node

import (
	"math"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/proto/hubproto"
)

// errorCounter is implemented by radios that count packets they dropped
// for CRC or header errors, like the sx1262 driver.
type errorCounter interface {
	Stats() (recv, recvErrs, sent uint32)
}

// linkTracker gathers the LinkStats a Hello reports: how the hub's replies
// arrive, and how many go missing. It lives in RAM, so a reboot starts it
// over.
type linkTracker struct {
	rssi    int8   // last hub reply, dBm; 0 if none since boot
	snrx4   int8   // its SNR in quarter dB
	missed  uint16 // requests without a reply, not yet reported to the hub
	errBase uint32 // the radio's error count up to which errors were reported

	// What the Hello being sent reports, taken off the counts once the hub
	// answers it. A Hello is built once and resent unchanged, so misses of
	// its own resends stay counted for the next one.
	sentMissed uint16
	sentErrs   uint32
}

// heard records the signal of a hub reply the node accepted.
func (l *linkTracker) heard(rssi float32, snrx4 int8) {
	l.rssi, l.snrx4 = clampInt8(math.Round(float64(rssi))), snrx4
}

// miss counts a request that got no reply.
func (l *linkTracker) miss() {
	if l.missed < math.MaxUint16 {
		l.missed++
	}
}

// stats returns what the next Hello reports.
func (l *linkTracker) stats(r meshcore.Radio) *hubproto.LinkStats {
	s := &hubproto.LinkStats{
		ReplyRSSI:  l.rssi,
		ReplySNRx4: l.snrx4,
		NoiseFloor: clampInt8(float64(r.NoiseFloor())),
		Missed:     l.missed,
	}
	l.sentMissed, l.sentErrs = l.missed, l.errBase
	if c, ok := r.(errorCounter); ok {
		_, errs, _ := c.Stats()
		s.RxErrors = uint16(min(errs-l.errBase, math.MaxUint16))
		l.sentErrs = errs
	}
	return s
}

// reported takes what the answered Hello carried off the counts.
func (l *linkTracker) reported() {
	l.missed -= min(l.sentMissed, l.missed)
	l.errBase = l.sentErrs
	l.sentMissed = 0
}

func clampInt8(v float64) int8 {
	return int8(max(math.MinInt8, min(math.MaxInt8, v)))
}
