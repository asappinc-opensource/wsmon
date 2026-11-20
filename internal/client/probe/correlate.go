package probe

import (
	"sync"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// windowSize is the number of completed or swept seqs remembered per
// connection. Memory stays constant (TC-CORR-07).
const windowSize = 1024

type windowState uint8

const (
	stateCompleted windowState = iota + 1
	stateSwept
)

type remembered struct {
	state   windowState
	payload Payload
}

// burstGroup tracks one burst. It is resolved under the correlator lock when
// its last entry resolves; wsmon_client_burst_completion_seconds is observed only if
// every frame was delivered.
type burstGroup struct {
	payload   Payload
	remaining int
	allOK     bool
	firstT1   int64 // t1 of the burst's first frame
	maxT4     int64 // latest t4 among the burst's echoes
	done      chan struct{}
}

func newBurstGroup(p Payload, n int) *burstGroup {
	return &burstGroup{payload: p, remaining: n, allOK: true, done: make(chan struct{})}
}

// entry is one in-flight probe.
type entry struct {
	seq        int64
	payload    Payload
	traceID    string
	spanID     string
	sampled    bool
	idleBucket string
	burst      *burstGroup
	burstFirst bool

	// Set by the writer, under the lock, immediately before the write.
	sent     bool
	t1       int64
	t1Time   time.Time // carries the monotonic reading of t1
	deadline time.Time

	done chan Outcome // buffered 1; receives exactly one outcome
}

// correlator matches echoes to probes on one connection. The in-flight
// map holds only unresolved entries; resolved seqs move to a fixed-size
// window. Recorder and Tracer calls are made under the lock so their order
// is the order of events; both are required not to block.
type correlator struct {
	p      *Probe
	connID string

	mu            sync.Mutex
	inflight      map[int64]*entry
	window        map[int64]remembered
	ring          [windowSize]int64
	ringLen       int
	ringPos       int
	highestEchoed int64
	server        protocol.ServerInfo // identity from hello_ack, for lost spans
	lastLoss      int64               // unix ns of the last sweep that counted a loss
}

func newCorrelator(p *Probe, connID string) *correlator {
	return &correlator{
		p:        p,
		connID:   connID,
		inflight: make(map[int64]*entry),
		window:   make(map[int64]remembered, windowSize),
	}
}

// register adds an entry before it is queued for writing, so an echo can
// never race ahead of its entry.
func (c *correlator) register(e *entry) {
	c.mu.Lock()
	c.inflight[e.seq] = e
	c.mu.Unlock()
}

// markSent records the t1 that is about to go on the wire. The writer calls
// it after StampT1 and immediately before Write (TC-TIME-07 end to end).
func (c *correlator) markSent(seq, t1 int64, t1Time time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.inflight[seq]
	if !ok {
		return
	}
	e.sent, e.t1, e.t1Time = true, t1, t1Time
	e.deadline = t1Time.Add(c.p.cfg.MessageTimeout)
	if e.burst != nil && e.burstFirst {
		e.burst.firstT1 = t1
	}
	c.p.rec.Sent(e.payload)
}

func (c *correlator) remember(seq int64, st windowState, p Payload) {
	if c.ringLen == windowSize {
		delete(c.window, c.ring[c.ringPos])
	} else {
		c.ringLen++
	}
	c.ring[c.ringPos] = seq
	c.ringPos = (c.ringPos + 1) % windowSize
	c.window[seq] = remembered{state: st, payload: p}
}

// resolve delivers the outcome to the waiting mode loop and settles the
// burst group. Called with the lock held.
func (c *correlator) resolve(e *entry, o Outcome, t4 int64) {
	select {
	case e.done <- o:
	default:
	}
	g := e.burst
	if g == nil {
		return
	}
	if o != OutcomeOK {
		g.allOK = false
	}
	if t4 > g.maxT4 {
		g.maxT4 = t4
	}
	g.remaining--
	if g.remaining == 0 {
		if g.allOK && g.maxT4 >= g.firstT1 {
			c.p.rec.BurstCompletion(g.payload, time.Duration(g.maxT4-g.firstT1))
		}
		close(g.done)
	}
}

// onEcho processes one echo in a fixed order. t4 was taken immediately
// after Read returned, before decoding.
func (c *correlator) onEcho(f *protocol.Echo, t4 time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rec := c.p.rec

	e, ok := c.inflight[f.Seq]
	if !ok || !e.sent {
		switch w, known := c.window[f.Seq]; {
		case !known:
			// 1. Never sent on this connection (or forgotten beyond the
			// window, which is indistinguishable).
			rec.Unexpected()
		case w.state == stateCompleted:
			// 2. Already completed: no second RTT.
			rec.Duplicate()
		default:
			// 3. Already swept as lost: late, not echoed.
			rec.Late(w.payload)
		}
		return
	}
	delete(c.inflight, f.Seq)
	c.remember(f.Seq, stateCompleted, e.payload)

	t4ns := t4.UnixNano()
	info := RoundTripInfo{
		ConnID: c.connID, Seq: e.seq, Payload: e.payload, IdleBucket: e.idleBucket,
		TraceID: e.traceID, SpanID: e.spanID, Sampled: e.sampled,
		Start: e.t1Time, End: t4, Server: f.Server,
	}

	// 4. Verbatim fields compared by value; one count per differing field.
	var mism []Field
	if f.TraceID != e.traceID {
		mism = append(mism, FieldTraceID)
	}
	if f.ConnID != c.connID {
		mism = append(mism, FieldConnID)
	}
	if f.ClientSendEpochNs != e.t1 {
		mism = append(mism, FieldT1)
	}
	if len(mism) > 0 {
		for _, m := range mism {
			rec.Mismatched(m)
		}
		info.Outcome, info.Mismatched = OutcomeMismatched, mism
		c.p.tr.RoundTrip(info)
		c.p.log.Warn("echo fields differ from the probe sent",
			"conn_id", c.connID, "seq", f.Seq, "fields", mism)
		c.resolve(e, OutcomeMismatched, t4ns)
		return
	}

	// 5. Delivered. RTT uses our own recorded t1, never the echoed one.
	lat := ComputeLatency(e.t1, f.ServerRecvEpochNs, f.ServerSendEpochNs, t4ns)
	for _, k := range lat.Anomalies {
		rec.TimeAnomaly(k)
	}
	info.Outcome = OutcomeOK
	if !lat.Discard {
		info.HaveRTT, info.RTT, info.RTTNet, info.ServerTime = true, lat.RTT, lat.RTTNet, lat.ServerTime
	}
	ex := c.p.tr.RoundTrip(info)
	if !lat.Discard {
		rec.RTT(e.payload, lat.RTT, lat.RTTNet, lat.ServerTime, f.Server.Name, ex)
		c.p.clock.set(sample{e.t1, f.ServerRecvEpochNs, f.ServerSendEpochNs, t4ns})
	}
	rec.Echoed(e.payload)
	if f.Seq < c.highestEchoed {
		rec.OutOfOrder()
	} else {
		c.highestEchoed = f.Seq
	}
	c.resolve(e, OutcomeOK, t4ns)
}

// sweep counts every sent entry past its deadline as lost. It returns
// the number swept.
func (c *correlator) sweep(now time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for seq, e := range c.inflight {
		if !e.sent || now.Before(e.deadline) {
			continue
		}
		delete(c.inflight, seq)
		c.remember(seq, stateSwept, e.payload)
		c.lost(e, e.deadline, 0)
		n++
	}
	if n > 0 {
		c.lastLoss = now.UnixNano()
	}
	return n
}

// lastLossNs is the unix ns time of the last sweep that counted a probe lost,
// or 0.
func (c *correlator) lastLossNs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastLoss
}

// failAll resolves every remaining entry when the connection is gone
// (in-flight probes are counted lost, then discarded; TC-CORR-08).
// Entries that were queued but never written are discarded uncounted.
// closeCode is the connection's close code; it is carried on the lost spans
// (wsmon.close_code) so a 1006/1009/1011 can be traced to its round trips.
func (c *correlator) failAll(now time.Time, closeCode int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for seq, e := range c.inflight {
		delete(c.inflight, seq)
		if e.sent {
			c.lost(e, now, closeCode)
		} else {
			c.resolve(e, OutcomeLost, 0)
		}
	}
}

func (c *correlator) lost(e *entry, end time.Time, closeCode int) {
	c.p.rec.Lost(e.payload)
	c.p.tr.RoundTrip(RoundTripInfo{
		ConnID: c.connID, Seq: e.seq, Payload: e.payload, IdleBucket: e.idleBucket,
		TraceID: e.traceID, SpanID: e.spanID, Sampled: e.sampled,
		Start: e.t1Time, End: end, Outcome: OutcomeLost, Server: c.server, CloseCode: closeCode,
	})
	c.resolve(e, OutcomeLost, 0)
}

// pending is the number of unresolved probes, including any still queued
// for the writer: shutdown drain must not close the connection under a probe
// that is about to be written.
func (c *correlator) pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.inflight)
}

func (c *correlator) sizes() (inflight, window int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.inflight), len(c.window)
}
