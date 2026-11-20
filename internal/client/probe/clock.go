package probe

import (
	"log/slog"
	"sync"
	"time"
)

// Latency is the result of the timestamp maths for one round trip. All values
// are single-clock differences; one-way latency is never computed.
type Latency struct {
	RTT        time.Duration
	ServerTime time.Duration
	RTTNet     time.Duration
	// Discard is true when t4 < t1: the sample is dropped entirely
	// (rtt_negative). No RTT, rtt_net or server-time observation is made.
	Discard bool
	// Anomalies lists the wsmon_client_time_anomaly_total kinds to increment.
	Anomalies []AnomalyKind
}

// ComputeLatency applies the four-timestamp RTT maths to integer-nanosecond timestamps:
//
//	rtt         = t4 − t1             (t4 < t1 → discard, rtt_negative)
//	server_time = t3 − t2             (< 0 → 0, server_time_negative)
//	rtt_net     = rtt − server_time   (clamped at ≥ 0)
func ComputeLatency(t1, t2, t3, t4 int64) Latency {
	var l Latency
	if t4 < t1 {
		l.Discard = true
		l.Anomalies = append(l.Anomalies, AnomalyRTTNegative)
		return l
	}
	l.RTT = time.Duration(t4 - t1)
	st := t3 - t2
	if st < 0 {
		st = 0
		l.Anomalies = append(l.Anomalies, AnomalyServerTimeNegative)
	}
	l.ServerTime = time.Duration(st)
	l.RTTNet = l.RTT - l.ServerTime
	if l.RTTNet < 0 {
		l.RTTNet = 0
	}
	return l
}

// ClockOffset is the NTP-style estimate ((t2−t1)+(t3−t4))/2. It
// is a diagnostic only and never alters a latency figure. Computed as
// (t2−t1)/2 + (t3−t4)/2 plus the remainder so int64 cannot overflow on
// epoch-scale inputs.
func ClockOffset(t1, t2, t3, t4 int64) time.Duration {
	a, b := t2-t1, t3-t4
	return time.Duration(a/2 + b/2 + (a%2+b%2)/2)
}

// sample is the most recent completed (t1, t2, t3, t4).
type sample struct {
	t1, t2, t3, t4 int64
}

// clockState holds the latest sample across connections (the gauge
// uses the most recent completed round trip, however old).
type clockState struct {
	mu   sync.Mutex
	last sample
	have bool
}

func (c *clockState) set(s sample) {
	c.mu.Lock()
	c.last, c.have = s, true
	c.mu.Unlock()
}

func (c *clockState) get() (sample, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last, c.have
}

// clockLoop emits ClockOffset every interval from the latest sample and
// WARNs above the threshold. It stops when stop is closed.
func (p *Probe) clockLoop(stop <-chan struct{}) {
	t := time.NewTicker(p.cfg.ClockOffsetInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		s, ok := p.clock.get()
		if !ok {
			continue
		}
		off := ClockOffset(s.t1, s.t2, s.t3, s.t4)
		p.rec.ClockOffset(off)
		if off.Abs() > p.cfg.ClockOffsetWarnThreshold {
			p.log.Warn("clock offset to server exceeds threshold; latency metrics are unaffected",
				slog.Duration("offset", off), slog.Duration("threshold", p.cfg.ClockOffsetWarnThreshold))
		}
	}
}
