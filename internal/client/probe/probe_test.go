package probe

import (
	"math/rand/v2"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// TC-TIME-01: t1..t4 = 1000, 1020, 1030, 1050 ns.
func TestTCTIME01_Maths(t *testing.T) {
	l := ComputeLatency(1000, 1020, 1030, 1050)
	if l.Discard || l.RTT != 50 || l.ServerTime != 10 || l.RTTNet != 40 || len(l.Anomalies) != 0 {
		t.Fatalf("got %+v", l)
	}
	if off := ClockOffset(1000, 1020, 1030, 1050); off != 0 {
		t.Errorf("offset = %v, want 0", off)
	}
}

// TC-TIME-04: t3 < t2 → server_time 0, rtt_net ≥ 0, one anomaly.
func TestTCTIME04_ServerTimeNegative(t *testing.T) {
	l := ComputeLatency(1000, 1030, 1020, 1050)
	if l.Discard || l.ServerTime != 0 || l.RTT != 50 || l.RTTNet != 50 {
		t.Fatalf("got %+v", l)
	}
	if len(l.Anomalies) != 1 || l.Anomalies[0] != AnomalyServerTimeNegative {
		t.Fatalf("anomalies %v", l.Anomalies)
	}
	// server_time larger than rtt (skewed server): rtt_net clamped at 0.
	l = ComputeLatency(1000, 0, 500, 1050)
	if l.RTTNet != 0 || l.RTT != 50 || l.ServerTime != 500 {
		t.Fatalf("clamp: got %+v", l)
	}
}

// TC-TIME-06: t4 < t1 → discarded entirely, rtt_negative.
func TestTCTIME06_RTTNegative(t *testing.T) {
	l := ComputeLatency(1050, 1020, 1030, 1000)
	if !l.Discard || l.RTT != 0 || l.RTTNet != 0 || l.ServerTime != 0 {
		t.Fatalf("got %+v", l)
	}
	if len(l.Anomalies) != 1 || l.Anomalies[0] != AnomalyRTTNegative {
		t.Fatalf("anomalies %v", l.Anomalies)
	}
}

func TestClockOffset(t *testing.T) {
	const skew = int64(5 * time.Second)
	// Symmetric 10 ns paths, server clock +5 s.
	if got := ClockOffset(1000, 1010+skew, 1020+skew, 1030); got != time.Duration(skew) {
		t.Errorf("+5s: %v", got)
	}
	if got := ClockOffset(1000, 1010-skew, 1020-skew, 1030); got != -time.Duration(skew) {
		t.Errorf("-5s: %v", got)
	}
	// Epoch-scale values do not overflow.
	now := time.Now().UnixNano()
	if got := ClockOffset(now, now+10, now+20, now+30); got != 0 {
		t.Errorf("epoch: %v", got)
	}
}

// TC-TIME-05 (Recorder half): the Recorder seam has no one-way latency
// method. Its method set is exactly the client's metric observables.
func TestTCTIME05_NoOneWayMethod(t *testing.T) {
	want := []string{
		"Closed", "ClockOffset", "ConnectAttempt", "ConnectFailure", "ConnectPhase", "ConnectionUp",
		"Duplicate", "Echoed", "IdleClose", "IdleProbe", "BurstCompletion", "Late", "Lost", "Mismatched",
		"OutOfOrder", "PushGap", "PushLost", "PushOutOfOrder", "PushReceived", "RTT", "Reconnect", "Sent",
		"ServerError", "TimeAnomaly", "Unexpected",
	}
	sort.Strings(want)
	typ := reflect.TypeOf((*Recorder)(nil)).Elem()
	var got []string
	for i := 0; i < typ.NumMethod(); i++ {
		got = append(got, typ.Method(i).Name)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Recorder methods = %v\nwant %v", got, want)
	}
	// RTT carries only single-clock durations: rtt, rtt_net, server_time.
	m, _ := typ.MethodByName("RTT")
	durs := 0
	for i := 0; i < m.Type.NumIn(); i++ {
		if m.Type.In(i) == reflect.TypeOf(time.Duration(0)) {
			durs++
		}
	}
	if durs != 3 {
		t.Errorf("RTT has %d duration args, want 3", durs)
	}
}

func TestBackoffDelay(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	base, cap := 100*time.Millisecond, time.Second
	for attempt := 0; attempt < 70; attempt++ {
		ceil := cap
		if attempt < 4 {
			ceil = base << attempt
		}
		seen := map[time.Duration]bool{}
		for i := 0; i < 200; i++ {
			d := backoffDelay(r, base, cap, attempt)
			if d < 0 || d >= ceil {
				t.Fatalf("attempt %d: %v not in [0, %v)", attempt, d, ceil)
			}
			seen[d] = true
		}
		if len(seen) < 150 {
			t.Fatalf("attempt %d: only %d distinct delays", attempt, len(seen))
		}
	}
}

// fakeRec counts calls for the correlator unit tests.
type fakeRec struct {
	NopRecorder
	sent, echoed, lost, late, dup, ooo, unexpected int
	mism                                           []Field
	rtts                                           []time.Duration
	anomalies                                      []AnomalyKind
	bursts                                         []time.Duration
}

func (f *fakeRec) Sent(Payload)              { f.sent++ }
func (f *fakeRec) Echoed(Payload)            { f.echoed++ }
func (f *fakeRec) Lost(Payload)              { f.lost++ }
func (f *fakeRec) Late(Payload)              { f.late++ }
func (f *fakeRec) Duplicate()                { f.dup++ }
func (f *fakeRec) OutOfOrder()               { f.ooo++ }
func (f *fakeRec) Unexpected()               { f.unexpected++ }
func (f *fakeRec) Mismatched(m Field)        { f.mism = append(f.mism, m) }
func (f *fakeRec) TimeAnomaly(k AnomalyKind) { f.anomalies = append(f.anomalies, k) }
func (f *fakeRec) BurstCompletion(_ Payload, d time.Duration) {
	f.bursts = append(f.bursts, d)
}
func (f *fakeRec) RTT(_ Payload, rtt, _, _ time.Duration, _ string, _ Exemplar) {
	f.rtts = append(f.rtts, rtt)
}

const testConn = "01J9ZQ8K3T4X5Y6Z7A8B9C0D1E"

func newTestCorrelator(rec Recorder) *correlator {
	p := New(config.Client{MessageTimeout: time.Second, Payloads: []string{"small"}}, rec, NopTracer{}, Options{})
	return newCorrelator(p, testConn)
}

// send registers and marks seq sent at t1 (ns since epoch, relative to base).
func send(c *correlator, seq int64, t1 time.Time, g *burstGroup, first bool) *entry {
	e := &entry{seq: seq, payload: PayloadSmall, traceID: "trace" + string(rune('a'+seq)), done: make(chan Outcome, 1), burst: g, burstFirst: first}
	c.register(e)
	c.markSent(seq, t1.UnixNano(), t1)
	return e
}

func echoOf(e *entry, t2, t3 int64) *protocol.Echo {
	return &protocol.Echo{TraceID: e.traceID, ConnID: testConn, Seq: e.seq, ClientSendEpochNs: e.t1,
		ServerRecvEpochNs: t2, ServerSendEpochNs: t3}
}

func TestCorrelatorOrder(t *testing.T) {
	rec := &fakeRec{}
	c := newTestCorrelator(rec)
	t0 := time.Now()
	e1 := send(c, 1, t0, nil, false)
	e2 := send(c, 2, t0, nil, false)
	e3 := send(c, 3, t0, nil, false)
	t4 := t0.Add(5 * time.Millisecond)

	// TC-CORR-01: never sent.
	c.onEcho(&protocol.Echo{Seq: 99, ConnID: testConn}, t4)
	// Delivered, then duplicate (TC-CORR-03).
	c.onEcho(echoOf(e1, t0.UnixNano(), t0.UnixNano()), t4)
	c.onEcho(echoOf(e1, t0.UnixNano(), t0.UnixNano()), t4)
	// TC-CORR-02 plus two more fields: one count per differing field.
	bad := echoOf(e2, 0, 0)
	bad.TraceID, bad.ConnID, bad.ClientSendEpochNs = "x", "y", 1
	c.onEcho(bad, t4)
	if o := <-e2.done; o != OutcomeMismatched {
		t.Errorf("e2 outcome %v", o)
	}
	// TC-CORR-05/06: sweep e3, then its echo is late, not echoed.
	if n := c.sweep(t0.Add(2 * time.Second)); n != 1 {
		t.Fatalf("swept %d", n)
	}
	c.onEcho(echoOf(e3, 0, 0), t4)

	if rec.unexpected != 1 || rec.echoed != 1 || rec.dup != 1 || rec.lost != 1 || rec.late != 1 || len(rec.rtts) != 1 {
		t.Errorf("rec = %+v", rec)
	}
	if !reflect.DeepEqual(rec.mism, []Field{FieldTraceID, FieldConnID, FieldT1}) {
		t.Errorf("mismatched = %v", rec.mism)
	}
	if rec.rtts[0] != 5*time.Millisecond {
		t.Errorf("rtt = %v (must use our own t1)", rec.rtts[0])
	}
	if in, _ := c.sizes(); in != 0 {
		t.Errorf("in-flight %d", in)
	}
}

// TC-CORR-04 / TC-TIME-06 through the correlator: 3, 1, 2 → +2 out of
// order, all delivered; an rtt_negative sample is echoed but not observed.
func TestCorrelatorOutOfOrderAndBurst(t *testing.T) {
	rec := &fakeRec{}
	c := newTestCorrelator(rec)
	t0 := time.Now()
	g := newBurstGroup(PayloadSmall, 3)
	es := []*entry{send(c, 1, t0, g, true), send(c, 2, t0, g, false), send(c, 3, t0, g, false)}
	for i, seq := range []int{3, 1, 2} {
		c.onEcho(echoOf(es[seq-1], 0, 0), t0.Add(time.Duration(i+1)*time.Millisecond))
	}
	if rec.ooo != 2 || rec.echoed != 3 {
		t.Errorf("ooo=%d echoed=%d", rec.ooo, rec.echoed)
	}
	select {
	case <-g.done:
	default:
		t.Fatal("burst not done")
	}
	if len(rec.bursts) != 1 || rec.bursts[0] != 3*time.Millisecond {
		t.Errorf("burst completion %v", rec.bursts)
	}

	// Incomplete burst: no completion.
	g2 := newBurstGroup(PayloadSmall, 2)
	a := send(c, 4, t0, g2, true)
	send(c, 5, t0, g2, false)
	c.onEcho(echoOf(a, 0, 0), t0.Add(time.Millisecond))
	c.failAll(t0, 1006)
	if len(rec.bursts) != 1 || rec.lost != 1 {
		t.Errorf("bursts=%v lost=%d", rec.bursts, rec.lost)
	}

	// rtt_negative: echoed, no RTT observation.
	e := send(c, 6, t0, nil, false)
	before := len(rec.rtts)
	c.onEcho(echoOf(e, 0, 0), t0.Add(-time.Millisecond))
	if len(rec.rtts) != before || rec.echoed != 5 || len(rec.anomalies) != 1 || rec.anomalies[0] != AnomalyRTTNegative {
		t.Errorf("rtts=%d echoed=%d anomalies=%v", len(rec.rtts)-before, rec.echoed, rec.anomalies)
	}
}

// TC-CORR-07 (unit): the window never exceeds 1024 and the map drains.
func TestCorrelatorBounded(t *testing.T) {
	c := newTestCorrelator(&fakeRec{})
	t0 := time.Now()
	for seq := int64(1); seq <= 5000; seq++ {
		e := send(c, seq, t0, nil, false)
		c.onEcho(echoOf(e, 0, 0), t0)
	}
	in, win := c.sizes()
	if in != 0 || win != windowSize {
		t.Fatalf("in=%d window=%d", in, win)
	}
	// An echo for an evicted seq is indistinguishable from never-sent.
	rec := &fakeRec{}
	c.p.rec = rec
	c.onEcho(&protocol.Echo{Seq: 1, ConnID: testConn}, t0)
	if rec.unexpected != 1 {
		t.Errorf("evicted seq: %+v", rec)
	}
}

// Queued-but-unwritten entries are discarded uncounted at close; sent ones
// are lost (TC-CORR-08 unit half).
func TestFailAll(t *testing.T) {
	rec := &fakeRec{}
	c := newTestCorrelator(rec)
	t0 := time.Now()
	for seq := int64(1); seq <= 3; seq++ {
		send(c, seq, t0, nil, false)
	}
	c.register(&entry{seq: 4, done: make(chan Outcome, 1)})
	c.failAll(t0, 1006)
	if rec.lost != 3 || rec.sent != 3 {
		t.Errorf("lost=%d sent=%d", rec.lost, rec.sent)
	}
}
