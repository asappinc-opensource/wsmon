// Package probetest provides an in-memory, concurrency-safe probe.Recorder
// and Tracer for tests. TestReassertViaMetrics re-asserts key cases against
// the Prometheus implementation via /metrics.
package probetest

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// Obs is one histogram-like observation.
type Obs struct {
	At    time.Time
	Value time.Duration
	Label string // e.g. server name, exemplar trace ID
}

// Event is one recorder call, in order.
type Event struct {
	At   time.Time
	Name string // the metric key, e.g. "closes{1006,remote}"
}

// Recorder records every call. Keys:
//
//	connect_attempts, connect_failures{reason}, connect_phase{phase} (obs),
//	connection_up (gauge), closes{code,initiator}, lifetime{code} (obs),
//	reconnects, sent{payload}, echoed{payload}, lost{payload}, late{payload},
//	mismatched{field}, duplicate, out_of_order, unexpected,
//	rtt{payload} / rtt_net{payload} / server_time (obs),
//	burst_completion{payload} (obs), clock_offset (gauge + obs),
//	time_anomaly{kind}, server_errors{code}, push_received, push_gap (obs),
//	push_lost, push_out_of_order, idle_probe{bucket,result},
//	idle_close{bucket,code}
type Recorder struct {
	mu     sync.Mutex
	counts map[string]float64
	gauges map[string]float64
	obs    map[string][]Obs
	events []Event
	notify chan struct{}
}

var _ probe.Recorder = (*Recorder)(nil)

func NewRecorder() *Recorder {
	return &Recorder{
		counts: map[string]float64{},
		gauges: map[string]float64{},
		obs:    map[string][]Obs{},
		notify: make(chan struct{}),
	}
}

func key(name string, labels ...string) string {
	if len(labels) == 0 {
		return name
	}
	return name + "{" + strings.Join(labels, ",") + "}"
}

// Key builds a key in the format documented on Recorder.
func Key(name string, labels ...string) string { return key(name, labels...) }

func (r *Recorder) changed(name string) {
	r.events = append(r.events, Event{At: time.Now(), Name: name})
	close(r.notify)
	r.notify = make(chan struct{})
}

func (r *Recorder) add(k string, v float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[k] += v
	r.changed(k)
}

func (r *Recorder) observe(k string, d time.Duration, label string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.obs[k] = append(r.obs[k], Obs{At: time.Now(), Value: d, Label: label})
	r.changed(k)
}

func (r *Recorder) gauge(k string, v float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[k] = v
	r.changed(k)
}

// Count returns a counter value.
func (r *Recorder) Count(k string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[k]
}

// SumPrefix sums every counter whose key starts with prefix.
func (r *Recorder) SumPrefix(prefix string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var s float64
	for k, v := range r.counts {
		if strings.HasPrefix(k, prefix) {
			s += v
		}
	}
	return s
}

// Gauge returns a gauge value.
func (r *Recorder) Gauge(k string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gauges[k]
}

// Obs returns the observations for k.
func (r *Recorder) Obs(k string) []Obs {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Obs(nil), r.obs[k]...)
}

// ObsPrefix returns observation counts per key with the prefix.
func (r *Recorder) ObsPrefix(prefix string) map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]int{}
	for k, v := range r.obs {
		if strings.HasPrefix(k, prefix) {
			out[k] = len(v)
		}
	}
	return out
}

// Events returns every call in order.
func (r *Recorder) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Event(nil), r.events...)
}

// Dump renders all counters, gauges and observation counts, for failure
// messages.
func (r *Recorder) Dump() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var lines []string
	for k, v := range r.counts {
		lines = append(lines, fmt.Sprintf("count %s = %g", k, v))
	}
	for k, v := range r.gauges {
		lines = append(lines, fmt.Sprintf("gauge %s = %g", k, v))
	}
	for k, v := range r.obs {
		lines = append(lines, fmt.Sprintf("obs   %s n=%d", k, len(v)))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// WaitFor blocks until cond() is true or timeout elapses, re-evaluating on
// every recorder call (and at least every 50 ms). It returns cond().
func (r *Recorder) WaitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		left := time.Until(deadline)
		if left <= 0 {
			return cond()
		}
		r.mu.Lock()
		ch := r.notify
		r.mu.Unlock()
		select {
		case <-ch:
		case <-time.After(min(left, 50*time.Millisecond)):
		}
	}
}

// MustWait fails the test if cond does not become true within timeout.
func (r *Recorder) MustWait(t testing.TB, what string, cond func() bool, timeout time.Duration) {
	t.Helper()
	if !r.WaitFor(cond, timeout) {
		t.Fatalf("timed out after %v waiting for %s\nrecorder:\n%s", timeout, what, r.Dump())
	}
}

func (r *Recorder) ConnectAttempt() { r.add("connect_attempts", 1) }
func (r *Recorder) ConnectFailure(reason probe.Reason) {
	r.add(key("connect_failures", string(reason)), 1)
}
func (r *Recorder) ConnectPhase(ph probe.Phase, d time.Duration) {
	r.observe(key("connect_phase", string(ph)), d, "")
}
func (r *Recorder) ConnectionUp(up bool, since time.Time) {
	v := 0.0
	if up {
		v = 1
	}
	r.gauge("connection_up", v)
}
func (r *Recorder) Closed(code int, init probe.Initiator, lifetime time.Duration) {
	c := fmt.Sprint(code)
	r.add(key("closes", c, string(init)), 1)
	r.observe(key("lifetime", c), lifetime, "")
}
func (r *Recorder) Reconnect()               { r.add("reconnects", 1) }
func (r *Recorder) Sent(p probe.Payload)     { r.add(key("sent", string(p)), 1) }
func (r *Recorder) Echoed(p probe.Payload)   { r.add(key("echoed", string(p)), 1) }
func (r *Recorder) Lost(p probe.Payload)     { r.add(key("lost", string(p)), 1) }
func (r *Recorder) Late(p probe.Payload)     { r.add(key("late", string(p)), 1) }
func (r *Recorder) Mismatched(f probe.Field) { r.add(key("mismatched", string(f)), 1) }
func (r *Recorder) Duplicate()               { r.add("duplicate", 1) }
func (r *Recorder) OutOfOrder()              { r.add("out_of_order", 1) }
func (r *Recorder) Unexpected()              { r.add("unexpected", 1) }
func (r *Recorder) RTT(p probe.Payload, rtt, rttNet, st time.Duration, server string, ex probe.Exemplar) {
	r.observe(key("rtt", string(p)), rtt, server)
	r.observe(key("rtt_net", string(p)), rttNet, ex.TraceID)
	r.observe("server_time", st, "")
}
func (r *Recorder) BurstCompletion(p probe.Payload, d time.Duration) {
	r.observe(key("burst_completion", string(p)), d, "")
}
func (r *Recorder) ClockOffset(d time.Duration) {
	r.gauge("clock_offset", d.Seconds())
	r.observe("clock_offset", d, "")
}
func (r *Recorder) TimeAnomaly(k probe.AnomalyKind) { r.add(key("time_anomaly", string(k)), 1) }
func (r *Recorder) ServerError(code protocol.ErrorCode) {
	r.add(key("server_errors", string(code)), 1)
}
func (r *Recorder) PushReceived()           { r.add("push_received", 1) }
func (r *Recorder) PushGap(d time.Duration) { r.observe("push_gap", d, "") }
func (r *Recorder) PushLost(n int64)        { r.add("push_lost", float64(n)) }
func (r *Recorder) PushOutOfOrder()         { r.add("push_out_of_order", 1) }
func (r *Recorder) IdleProbe(bucket string, res probe.IdleResult) {
	r.add(key("idle_probe", bucket, string(res)), 1)
}
func (r *Recorder) IdleClose(bucket, code string) { r.add(key("idle_close", bucket, code), 1) }

// Tracer records RoundTrip, Connect and PushAck calls and generates real IDs.
// Sample sets the head decision.
type Tracer struct {
	probe.NopTracer
	Sample bool

	mu         sync.Mutex
	roundTrips []probe.RoundTripInfo
	connects   []probe.ConnectInfo
	pushAcks   []probe.PushAckInfo
}

var _ probe.Tracer = (*Tracer)(nil)

func (t *Tracer) HeadDecision(string) bool { return t.Sample }
func (t *Tracer) RoundTrip(i probe.RoundTripInfo) probe.Exemplar {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.roundTrips = append(t.roundTrips, i)
	if i.Sampled {
		return probe.Exemplar{TraceID: i.TraceID}
	}
	return probe.Exemplar{}
}
func (t *Tracer) Connect(i probe.ConnectInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.connects = append(t.connects, i)
}
func (t *Tracer) PushAck(i probe.PushAckInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pushAcks = append(t.pushAcks, i)
}
func (t *Tracer) RoundTrips() []probe.RoundTripInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]probe.RoundTripInfo(nil), t.roundTrips...)
}
func (t *Tracer) Connects() []probe.ConnectInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]probe.ConnectInfo(nil), t.connects...)
}
func (t *Tracer) PushAcks() []probe.PushAckInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]probe.PushAckInfo(nil), t.pushAcks...)
}
