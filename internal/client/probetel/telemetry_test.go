package probetel

import (
	"context"
	"net/http"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
)

func load(t *testing.T, kv ...string) config.Client {
	t.Helper()
	env := map[string]string{
		"WSMON_URL":          "ws://127.0.0.1:1/__synthetic/ws",
		"WSMON_METRICS_ADDR": "127.0.0.1:0",
		"WSMON_LABELS":       "site=unit",
	}
	for i := 0; i+1 < len(kv); i += 2 {
		env[kv[i]] = kv[i+1]
	}
	cfg, err := config.LoadClient(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func newTel(t *testing.T, cfg config.Client, exp *tracetest.InMemoryExporter) *Telemetry {
	t.Helper()
	opts := Options{Version: "0.0.0-unit"}
	if exp != nil {
		opts.SpanExporter = exp
	}
	tel, err := New(context.Background(), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	return tel
}

func families(t *testing.T, tel *Telemetry) map[string]*dto.MetricFamily {
	t.Helper()
	mfs, err := tel.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*dto.MetricFamily{}
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

// TC-TEL-09: every histogram's classic buckets equal its documented list exactly;
// wsmon_client_rtt_seconds has le=0.25.
func TestTCTEL09_Buckets(t *testing.T) {
	want := map[string][]float64{
		"wsmon_client_rtt_seconds":                 telemetry.RTTBuckets,
		"wsmon_client_rtt_net_seconds":             telemetry.RTTBuckets,
		"wsmon_client_burst_completion_seconds":    telemetry.RTTBuckets,
		"wsmon_client_push_gap_seconds":            telemetry.RTTBuckets,
		"wsmon_client_server_processing_seconds":   {0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01},
		"wsmon_client_connect_duration_seconds":    {0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0},
		"wsmon_client_connection_lifetime_seconds": {10, 30, 60, 120, 300, 600, 1800, 3600, 7200, 21600, 86400},
		"wsmon_client_loop_lag_seconds":            {0.001, 0.005, 0.010, 0.050, 0.100, 0.500, 1.0},
	}
	// The RTT bucket list, written out independently of telemetry.RTTBuckets.
	spec := []float64{0.005, 0.010, 0.025, 0.050, 0.075, 0.100, 0.150, 0.200, 0.250, 0.500, 0.750, 1.0, 2.5, 5.0, 10.0}
	if !slices.Equal(telemetry.RTTBuckets, spec) {
		t.Fatalf("telemetry.RTTBuckets %v", telemetry.RTTBuckets)
	}
	seen := map[string]bool{}
	for _, kv := range [][]string{
		{"WSMON_MODE", "burst"},
		{"WSMON_MODE", "pingpong", "WSMON_PUSH_INTERVAL_MS", "1000"},
	} {
		tel := newTel(t, load(t, kv...), nil)
		r := tel.rec
		r.RTT(probe.PayloadSmall, time.Millisecond, time.Millisecond, 0, "srv", probe.Exemplar{})
		r.Closed(1000, probe.InitiatorClient, time.Second)
		r.PushGap(time.Second)
		r.LoopLag(0)
		for name, mf := range families(t, tel) {
			if mf.GetType() != dto.MetricType_HISTOGRAM {
				continue
			}
			wb, ok := want[name]
			if !ok {
				t.Errorf("unexpected histogram %s", name)
				continue
			}
			seen[name] = true
			for _, m := range mf.GetMetric() {
				var got []float64
				for _, b := range m.GetHistogram().GetBucket() {
					got = append(got, b.GetUpperBound())
				}
				if !slices.Equal(got, wb) {
					t.Errorf("%s buckets %v, want %v", name, got, wb)
				}
				if name == "wsmon_client_rtt_seconds" && !slices.Contains(got, 0.25) {
					t.Error("wsmon_client_rtt_seconds has no le=0.25")
				}
			}
		}
	}
	for name := range want {
		if !seen[name] {
			t.Errorf("histogram %s never exposed", name)
		}
	}
}

func counterValue(t *testing.T, tel *Telemetry, name string, labels map[string]string) float64 {
	t.Helper()
	mf := families(t, tel)[name]
	for _, m := range mf.GetMetric() {
		ok := true
		for k, v := range labels {
			found := false
			for _, lp := range m.GetLabel() {
				found = found || (lp.GetName() == k && lp.GetValue() == v)
			}
			ok = ok && found
		}
		if ok {
			return m.GetCounter().GetValue()
		}
	}
	return -1
}

// Unknown close and error codes never become label values.
func TestLabelMapping(t *testing.T) {
	tel := newTel(t, load(t, "WSMON_MODE", "idle", "WSMON_IDLE_LADDER", "30s,60s"), nil)
	r := tel.rec
	r.Closed(4000, probe.InitiatorRemote, time.Second)
	r.Closed(1006, probe.InitiatorRemote, time.Second)
	r.ServerError("brand_new_code")
	r.ServerError(protocol.ErrRateLimited)
	r.IdleClose("30", "1006")
	r.IdleClose("30", "4321")
	r.IdleClose("60", probe.IdleCloseNone)
	for _, c := range []struct {
		name   string
		labels map[string]string
	}{
		{"wsmon_client_closes_total", map[string]string{"close_code": "other", "initiator": "remote", "mode": "idle", "site": "unit"}},
		{"wsmon_client_closes_total", map[string]string{"close_code": "1006", "initiator": "remote"}},
		{"wsmon_client_server_errors_received_total", map[string]string{"code": "other"}},
		{"wsmon_client_server_errors_received_total", map[string]string{"code": "rate_limited"}},
		{"wsmon_client_idle_probe_close_total", map[string]string{"idle_bucket": "30", "close_code": "1006"}},
		{"wsmon_client_idle_probe_close_total", map[string]string{"idle_bucket": "30", "close_code": "other"}},
		{"wsmon_client_idle_probe_close_total", map[string]string{"idle_bucket": "60", "close_code": "none"}},
	} {
		if v := counterValue(t, tel, c.name, c.labels); v != 1 {
			t.Errorf("%s%v = %v, want 1", c.name, c.labels, v)
		}
	}
	for _, mf := range families(t, tel) {
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetValue() == "4000" || lp.GetValue() == "4321" || lp.GetValue() == "brand_new_code" {
					t.Errorf("%s carries %s=%s", mf.GetName(), lp.GetName(), lp.GetValue())
				}
			}
		}
	}
}

// wsmon_client_config_hash ignores secrets and spelling, and changes with any
// non-secret value.
func TestConfigHash(t *testing.T) {
	base := ConfigHash(load(t))
	if len(base) != 16 {
		t.Fatalf("hash %q", base)
	}
	if h := ConfigHash(load(t, "WSMON_AUTH_TOKEN", "x", "WSMON_REMOTE_WRITE_PASSWORD", "y", "WSMON_REMOTE_WRITE_BEARER_TOKEN", "z")); h != base {
		t.Error("secrets changed the hash")
	}
	if h := ConfigHash(load(t, "WSMON_PINGPONG_INTERVAL", "1000ms")); h != base {
		t.Error("1000ms vs 1s changed the hash")
	}
	if h := ConfigHash(load(t, "WSMON_PINGPONG_INTERVAL", "2s")); h == base {
		t.Error("a non-secret change kept the hash")
	}
}

func spansNamed(exp *tracetest.InMemoryExporter, name string) tracetest.SpanStubs {
	var out tracetest.SpanStubs
	for _, s := range exp.GetSpans() {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

// Sampling: ws.roundtrip uses the wire IDs; exported iff head-sampled
// or upgraded (lost, mismatched, slow, 1006/1009/1011); the exemplar is set
// iff exported.
func TestTracerSampling(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tel := newTel(t, load(t, "WSMON_TRACE_SAMPLE_RATIO", "0", "WSMON_TRACE_SLOW_THRESHOLD", "100ms"), exp)
	tr := tel.tr
	if tid, _ := tr.NewIDs(); tr.HeadDecision(tid) {
		t.Error("ratio 0 head-sampled")
	}
	now := time.Now()
	rt := func(mut func(*probe.RoundTripInfo)) (probe.Exemplar, string, string) {
		tid, sid := tr.NewIDs()
		info := probe.RoundTripInfo{ConnID: "c", Seq: 1, Payload: probe.PayloadSmall, TraceID: tid, SpanID: sid,
			Start: now, End: now.Add(10 * time.Millisecond), Outcome: probe.OutcomeOK,
			HaveRTT: true, RTT: 10 * time.Millisecond}
		mut(&info)
		return tr.RoundTrip(info), tid, sid
	}
	cases := []struct {
		name   string
		mut    func(*probe.RoundTripInfo)
		export bool
	}{
		{"fast unsampled", func(*probe.RoundTripInfo) {}, false},
		{"head sampled", func(i *probe.RoundTripInfo) { i.Sampled = true }, true},
		{"slow", func(i *probe.RoundTripInfo) { i.RTT = 101 * time.Millisecond }, true},
		{"lost", func(i *probe.RoundTripInfo) { i.Outcome, i.HaveRTT = probe.OutcomeLost, false }, true},
		{"mismatched", func(i *probe.RoundTripInfo) {
			i.Outcome, i.HaveRTT, i.Mismatched = probe.OutcomeMismatched, false, []probe.Field{probe.FieldT1}
		}, true},
		{"lost at 1006", func(i *probe.RoundTripInfo) { i.Outcome, i.HaveRTT, i.CloseCode = probe.OutcomeLost, false, 1006 }, true},
	}
	for _, c := range cases {
		before := len(spansNamed(exp, SpanRoundTrip))
		ex, tid, sid := rt(c.mut)
		got := spansNamed(exp, SpanRoundTrip)
		exported := len(got) > before
		if exported != c.export || (ex.TraceID != "") != c.export {
			t.Errorf("%s: exported=%v exemplar=%q, want export=%v", c.name, exported, ex.TraceID, c.export)
			continue
		}
		if exported {
			sc := got[len(got)-1].SpanContext
			if sc.TraceID().String() != tid || sc.SpanID().String() != sid || ex.TraceID != tid {
				t.Errorf("%s: span IDs %s/%s, wire %s/%s", c.name, sc.TraceID(), sc.SpanID(), tid, sid)
			}
			if d := got[len(got)-1].EndTime.Sub(got[len(got)-1].StartTime); d != 10*time.Millisecond {
				t.Errorf("%s: span duration %v", c.name, d)
			}
		}
	}
	last := spansNamed(exp, SpanRoundTrip)
	var hasClose bool
	for _, a := range last[len(last)-1].Attributes {
		hasClose = hasClose || (a.Key == AttrCloseCode && a.Value.AsInt64() == 1006)
	}
	if !hasClose {
		t.Error("lost-at-1006 span lacks wsmon.close_code")
	}

	// Connect: a failure is always exported, with one child per phase and no
	// tls.* for ws://; a success at ratio 0 is not.
	tr.Connect(probe.ConnectInfo{Start: now, End: now.Add(time.Millisecond)})
	if n := len(spansNamed(exp, SpanConnect)); n != 0 {
		t.Errorf("unsampled successful connect exported (%d)", n)
	}
	tr.Connect(probe.ConnectInfo{Start: now, End: now.Add(5 * time.Millisecond), Failure: probe.ReasonHTTPStatus,
		Phases:  []probe.PhaseSpan{{Phase: probe.PhaseTCP, Start: now, End: now.Add(time.Millisecond)}, {Phase: probe.PhaseUpgrade, Start: now.Add(time.Millisecond), End: now.Add(5 * time.Millisecond)}},
		Headers: map[string]string{"cf-ray": "abc-LHR"}, PeerIP: "127.0.0.1", LocalPort: 5555})
	cs := spansNamed(exp, SpanConnect)
	if len(cs) != 1 {
		t.Fatalf("connect spans %d", len(cs))
	}
	attrs := map[string]string{}
	for _, a := range cs[0].Attributes {
		attrs[string(a.Key)] = a.Value.Emit()
	}
	if attrs["wsmon.connect.failure_reason"] != "http_status" || attrs["http.response.header.cf_ray"] != "abc-LHR" ||
		attrs["net.peer.ip"] != "127.0.0.1" || attrs["net.host.port"] != "5555" || attrs["wsmon.site"] != "unit" {
		t.Errorf("connect attrs %v", attrs)
	}
	if _, ok := attrs["tls.protocol.version"]; ok {
		t.Error("tls.* on a ws:// connect")
	}
	for _, ph := range []string{"tcp", "upgrade"} {
		c := spansNamed(exp, ph)
		if len(c) != 1 || c[0].Parent.SpanID() != cs[0].SpanContext.SpanID() {
			t.Errorf("phase %s: %+v", ph, c)
		}
	}

	// Pre-generated IDs (the ones sent in the upgrade traceparent) are the
	// span's own, and the head decision they carry is honoured.
	tr.Connect(probe.ConnectInfo{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "00f067aa0ba902b7", Sampled: false,
		Start: now, End: now.Add(time.Millisecond)})
	tr.Connect(probe.ConnectInfo{TraceID: "4bf92f3577b34da6a3ce929d0e0e4737", SpanID: "00f067aa0ba902b8", Sampled: true,
		Start: now, End: now.Add(time.Millisecond)})
	cs = spansNamed(exp, SpanConnect)
	if len(cs) != 2 || cs[1].SpanContext.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4737" ||
		cs[1].SpanContext.SpanID().String() != "00f067aa0ba902b8" {
		t.Errorf("connect with pre-generated IDs: %+v", cs)
	}

	// push_ack: child of the push traceparent; unsampled parent → no span.
	tr.PushAck(probe.PushAckInfo{Traceparent: protocol.FormatTraceparent("9c1e0000000000000000000000000001", "b7ad6b7169203331", false), Start: now, End: now})
	tr.PushAck(probe.PushAckInfo{Traceparent: protocol.FormatTraceparent("9c1e0000000000000000000000000002", "b7ad6b7169203332", true), PushSeq: 4, Start: now, End: now})
	pa := spansNamed(exp, SpanPushAck)
	if len(pa) != 1 || pa[0].Parent.SpanID().String() != "b7ad6b7169203332" || pa[0].SpanContext.TraceID().String() != "9c1e0000000000000000000000000002" || !pa[0].Parent.IsRemote() {
		t.Errorf("push_ack %+v", pa)
	}
}

// With tracing disabled: no head sampling (traceparent -00), valid random
// IDs, nothing exported, no exemplar.
func TestTracerDisabled(t *testing.T) {
	tel := newTel(t, load(t, "WSMON_TRACE_SAMPLE_RATIO", "1"), nil)
	tid, sid := tel.tr.NewIDs()
	if _, err := trace.TraceIDFromHex(tid); err != nil || len(sid) != 16 {
		t.Fatalf("ids %q %q", tid, sid)
	}
	if tel.tr.HeadDecision(tid) {
		t.Error("head-sampled with tracing disabled")
	}
	if ex := tel.tr.RoundTrip(probe.RoundTripInfo{TraceID: tid, SpanID: sid, Outcome: probe.OutcomeLost}); ex.TraceID != "" {
		t.Error("exemplar with tracing disabled")
	}
}

func get(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// /healthz fails only on a stalled ticker; /readyz only from shutdown on;
// nothing else is served.
func TestHealthEndpoints(t *testing.T) {
	tel := newTel(t, load(t), nil)
	if err := tel.Start(); err != nil {
		t.Fatal(err)
	}
	base := "http://" + tel.Addr().String()
	for path, want := range map[string]int{"/healthz": 200, "/readyz": 200, "/metrics": 200, "/": 404, "/debug/pprof/": 404} {
		if got := get(t, base+path); got != want {
			t.Errorf("%s = %d, want %d", path, got, want)
		}
	}
	tel.lastTick.Store(time.Now().Add(-WedgedAfter - time.Second).UnixNano())
	if got := get(t, base+"/healthz"); got != 503 {
		t.Errorf("stalled ticker: /healthz %d", got)
	}
	time.Sleep(3 * TickInterval)
	if got := get(t, base+"/healthz"); got != 200 {
		t.Errorf("ticker resumed: /healthz %d", got)
	}
	tel.BeginShutdown()
	if got, h := get(t, base+"/readyz"), get(t, base+"/healthz"); got != 503 || h != 200 {
		t.Errorf("shutting down: /readyz %d /healthz %d", got, h)
	}
}

func lagStats(t *testing.T, tel *Telemetry) (count uint64, sum float64) {
	t.Helper()
	h := families(t, tel)["wsmon_client_loop_lag_seconds"].GetMetric()[0].GetHistogram()
	return h.GetSampleCount(), h.GetSampleSum()
}

// TC-TEL-07: CPU starvation shows up in wsmon_client_loop_lag_seconds. Method: in
// process, GOMAXPROCS(1) plus busy goroutines for a window, compared with an
// idle baseline window of the same length. The Go scheduler preempts each
// busy goroutine only every ~10 ms, so the ticker goroutine waits behind
// them, which is exactly a throttled probe.
func TestTCTEL07_LoopLagRises(t *testing.T) {
	tel := newTel(t, load(t), nil)
	if err := tel.Start(); err != nil {
		t.Fatal(err)
	}
	const window = 2 * time.Second
	time.Sleep(300 * time.Millisecond)
	c0, s0 := lagStats(t, tel)
	time.Sleep(window)
	c1, s1 := lagStats(t, tel)

	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			x := 0
			for !stop.Load() {
				x++
			}
			_ = x
		}()
	}
	time.Sleep(window)
	stop.Store(true)
	wg.Wait()
	c2, s2 := lagStats(t, tel)
	runtime.GOMAXPROCS(prev)

	baseMean := (s1 - s0) / float64(max(c1-c0, 1))
	busyMean := (s2 - s1) / float64(max(c2-c1, 1))
	t.Logf("baseline: %d ticks, mean lag %.2f ms; starved: %d ticks, mean lag %.2f ms",
		c1-c0, baseMean*1e3, c2-c1, busyMean*1e3)
	if busyMean < 0.005 || busyMean < 3*baseMean {
		t.Errorf("loop lag did not rise measurably: baseline %.2f ms, starved %.2f ms", baseMean*1e3, busyMean*1e3)
	}
}
