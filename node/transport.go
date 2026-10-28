package node

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshub/proto/hubproto"
)

// rttEstimator learns how long the hub takes to answer over one kind of
// route, like TCP's retransmission timer (RFC 6298).
type rttEstimator struct {
	srtt, rttvar time.Duration
	ok           bool
}

func (r *rttEstimator) sample(d time.Duration) {
	if !r.ok {
		r.srtt, r.rttvar, r.ok = d, d/2, true
		return
	}
	diff := r.srtt - d
	if diff < 0 {
		diff = -diff
	}
	r.rttvar = (3*r.rttvar + diff) / 4
	r.srtt = (7*r.srtt + d) / 8
}

// timeout returns how long to wait for a reply: the full max until there
// is a sample, then the smoothed time plus four deviations, within
// [min, max].
func (r *rttEstimator) timeout(min, max time.Duration) time.Duration {
	if !r.ok {
		return max
	}
	return clamp(r.srtt+4*r.rttvar, min, max)
}

func clamp(d, lo, hi time.Duration) time.Duration {
	if lo > hi {
		lo = hi
	}
	return max(lo, min(d, hi))
}

// randDuration returns a random duration in [0, d).
func (n *Node) randDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	var b [8]byte
	n.cfg.RNG.Random(b[:])
	return time.Duration(binary.LittleEndian.Uint64(b[:]) % uint64(d))
}

// wait sleeps for d, or until ctx is done.
func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// request sends m to the hub and waits for the reply. It goes direct when
// the node knows its path to the hub, falling back to a flood if that gets
// no reply, and resends after a random backoff up to cfg.Retries times. A
// message too large for a datagram is sent in fragments.
func (n *Node) request(ctx context.Context, m hubproto.Message, anon bool) (hubproto.Message, error) {
	_, err := hubproto.Encode(nil, 0, m, n.maxPlaintext(anon))
	if errors.Is(err, hubproto.ErrTooLarge) && !anon {
		return n.requestFragmented(ctx, m)
	}
	if err != nil {
		return nil, err
	}
	return n.requestDatagram(ctx, m, anon)
}

func (n *Node) maxPlaintext(anon bool) int {
	if anon {
		return hubproto.MaxAnonPlaintext
	}
	return hubproto.MaxPlaintext
}

func (n *Node) requestDatagram(ctx context.Context, m hubproto.Message, anon bool) (hubproto.Message, error) {
	for tries := 0; ; {
		direct := !anon && n.st.hasPath
		est := &n.floodRTT
		if direct {
			est = &n.directRTT
		}
		// Each retry over the same kind of route waits twice as long.
		timeout := clamp(est.timeout(n.cfg.MinReplyTimeout, n.cfg.ReplyTimeout)<<tries, 0, n.cfg.ReplyTimeout)
		start := time.Now()
		reply, err := n.send(ctx, m, anon, direct, timeout)
		if err == nil {
			est.sample(time.Since(start))
			return reply, nil
		}
		if !errors.Is(err, ErrNoReply) {
			return nil, err
		}
		if direct {
			// The route to the hub is stale: forget it and flood. This
			// does not count as a retry.
			n.st.hasPath = false
			n.directRTT = rttEstimator{}
			if err := n.saveState(); err != nil {
				return nil, err
			}
		} else if tries == n.cfg.Retries {
			return nil, ErrNoReply
		} else {
			tries++
		}
		// Back off randomly, so nodes that lost the same packet do not
		// collide again.
		if err := wait(ctx, n.randDuration(n.cfg.RetryBackoff<<tries)); err != nil {
			return nil, err
		}
	}
}

// requestFragmented sends a message larger than a datagram as Fragments.
// The hub acknowledges each one but the last, whose reply answers m.
func (n *Node) requestFragmented(ctx context.Context, m hubproto.Message) (hubproto.Message, error) {
	body, err := hubproto.AppendMessage(nil, m)
	if err != nil {
		return nil, err
	}
	if len(body) > hubproto.MaxMessage {
		return nil, ErrTooLarge
	}
	var x [2]byte
	n.cfg.RNG.Random(x[:])
	xfer := binary.LittleEndian.Uint16(x[:])
	for off := 0; ; off += hubproto.FragmentSize {
		end := min(off+hubproto.FragmentSize, len(body))
		reply, err := n.requestDatagram(ctx, &hubproto.Fragment{
			Xfer: xfer, Total: uint16(len(body)), Offset: uint16(off), Data: body[off:end],
		}, false)
		if err != nil || end == len(body) {
			return reply, err
		}
		if _, ok := reply.(*hubproto.Ack); !ok {
			return reply, nil // an error: the caller handles it
		}
	}
}

func (n *Node) send(ctx context.Context, m hubproto.Message, anon, direct bool, timeout time.Duration) (hubproto.Message, error) {
	seq, err := n.nextSeq()
	if err != nil {
		return nil, err
	}
	pt, err := hubproto.Encode(nil, seq, m, n.maxPlaintext(anon))
	if err != nil {
		return nil, err
	}

	var pkt *meshcore.Packet
	if anon {
		pkt = n.mesh.CreateAnonDatagram(meshcore.PayloadTypeAnonReq, &n.mesh.SelfID, &n.cfg.Hub, n.secret[:], pt)
	} else {
		pkt = n.mesh.CreateDatagram(meshcore.PayloadTypeReq, &n.cfg.Hub, n.secret[:], pt)
	}
	if pkt == nil {
		return nil, errors.New("node: no packet buffer")
	}
	if direct {
		n.mesh.SendDirect(pkt, n.st.path, n.st.pathLen, 0)
	} else {
		n.mesh.SendFlood(pkt, 0, 1)
	}

	n.h.await(seq)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n.mesh.Loop()
		n.cfg.RTC.Tick()
		if reply := n.h.take(); reply != nil {
			return reply, nil
		}
		time.Sleep(time.Millisecond)
	}
	return nil, ErrNoReply
}

// nextSeq returns a fresh request number, reserving a block in storage when
// the current one runs out.
func (n *Node) nextSeq() (uint32, error) {
	if n.seq >= n.st.seqLimit {
		n.st.seqLimit = n.seq + seqReserve
		if err := n.saveState(); err != nil {
			return 0, err
		}
	}
	s := n.seq
	n.seq++
	return s, nil
}

// handler receives the hub's responses from the mesh.
type handler struct {
	meshcore.BaseHandler
	n       *Node
	waitSeq uint32
	waiting bool
	reply   hubproto.Message
}

func (h *handler) await(seq uint32) { h.waitSeq, h.waiting, h.reply = seq, true, nil }

func (h *handler) take() hubproto.Message {
	r := h.reply
	if r != nil {
		h.waiting, h.reply = false, nil
	}
	return r
}

// CalcRxDelay is zero: the node does not repeat, so it has no reason to
// hold packets back.
func (h *handler) CalcRxDelay(float32, uint32) int { return 0 }

func (h *handler) SearchPeersByHash(hash []byte) int {
	if h.n.cfg.Hub.IsHashMatch(hash) {
		return 1
	}
	return 0
}

func (h *handler) PeerSharedSecret(dst *[meshcore.PubKeySize]byte, _ int) { *dst = h.n.secret }

func (h *handler) OnPeerDataRecv(p *meshcore.Packet, typ uint8, _ int, _ *[meshcore.PubKeySize]byte, data []byte) {
	if typ != meshcore.PayloadTypeResponse || !h.waiting {
		return
	}
	seq, m, err := hubproto.Decode(data)
	if err != nil || seq != h.waitSeq {
		return // not the reply we are waiting for
	}
	h.reply = m
}
