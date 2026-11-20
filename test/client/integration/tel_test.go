package integration

import (
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/asappinc-opensource/wsmon/internal/client/probetel"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
	"github.com/asappinc-opensource/wsmon/test/client/fakeserver"
	"github.com/asappinc-opensource/wsmon/test/client/otlpsink"
	"github.com/asappinc-opensource/wsmon/test/client/rwsink"
)

// specRows lists every client metric and its labels beyond the
// universal WSMON_LABELS pairs. This table is the allow-list.
var specRows = map[string][]string{
	// Connection lifecycle.
	"wsmon_client_connect_attempts_total":      {"mode"},
	"wsmon_client_connect_failures_total":      {"mode", "reason"},
	"wsmon_client_connect_duration_seconds":    {"mode", "phase"},
	"wsmon_client_connection_up":               {"mode"},
	"wsmon_client_connections_active":          {"mode"},
	"wsmon_client_connection_age_seconds":      {"mode"},
	"wsmon_client_connection_lifetime_seconds": {"mode", "close_code"},
	"wsmon_client_closes_total":                {"mode", "close_code", "initiator"},
	"wsmon_client_reconnects_total":            {"mode"},
	// Message plane.
	"wsmon_client_messages_sent_total":          {"mode", "payload"},
	"wsmon_client_messages_echoed_total":        {"mode", "payload"},
	"wsmon_client_messages_lost_total":          {"mode", "payload"},
	"wsmon_client_messages_late_total":          {"mode", "payload"},
	"wsmon_client_messages_mismatched_total":    {"mode", "field"},
	"wsmon_client_messages_duplicate_total":     {"mode"},
	"wsmon_client_messages_out_of_order_total":  {"mode"},
	"wsmon_client_messages_unexpected_total":    {"mode"},
	"wsmon_client_rtt_seconds":                  {"mode", "payload", "server_name"},
	"wsmon_client_rtt_net_seconds":              {"mode", "payload"},
	"wsmon_client_server_processing_seconds":    {"mode"},
	"wsmon_client_burst_completion_seconds":     {"payload"},
	"wsmon_client_clock_offset_seconds":         {},
	"wsmon_client_time_anomaly_total":           {"kind"},
	"wsmon_client_server_errors_received_total": {"code"},
	"wsmon_client_push_received_total":          {},
	"wsmon_client_push_gap_seconds":             {},
	"wsmon_client_push_lost_total":              {},
	"wsmon_client_push_out_of_order_total":      {},
	// Idle probe.
	"wsmon_client_idle_probe_total":       {"idle_bucket", "result"},
	"wsmon_client_idle_probe_close_total": {"idle_bucket", "close_code"},
	// Probe self-health.
	"wsmon_client_loop_lag_seconds":                            {},
	"wsmon_client_build_info":                                  {"version", "go_version", "name"},
	"wsmon_client_config_hash":                                 {"hash"},
	"wsmon_client_start_time_seconds":                          {},
	"wsmon_client_remote_write_failures_total":                 {},
	"wsmon_client_remote_write_last_success_timestamp_seconds": {},
}

var (
	burstOnly = []string{"wsmon_client_burst_completion_seconds"}
	pushOnly  = []string{"wsmon_client_push_received_total", "wsmon_client_push_gap_seconds", "wsmon_client_push_lost_total", "wsmon_client_push_out_of_order_total"}
	idleOnly  = []string{"wsmon_client_idle_probe_total", "wsmon_client_idle_probe_close_total"}
	rwOnly    = []string{"wsmon_client_remote_write_failures_total", "wsmon_client_remote_write_last_success_timestamp_seconds"}
)

// expectedFamilies is the specRows subset applicable to a mode/options.
func expectedFamilies(mode string, push, rw bool) []string {
	var out []string
	for name := range specRows {
		switch {
		case slices.Contains(burstOnly, name) && mode != "burst",
			slices.Contains(pushOnly, name) && !push,
			slices.Contains(idleOnly, name) && mode != "idle",
			slices.Contains(rwOnly, name) && !rw:
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

const telLabels = "site=tel-site,path=direct"

var universal = L{"site": "tel-site", "path": "direct"}

// oneWayPattern catches any family that could be one-way latency.
var oneWayPattern = regexp.MustCompile(`one_way|oneway|upstream|downstream|t2_t1|t4_t3|server_recv|server_send|client_send|client_recv|uplink|downlink`)

// forbiddenLabels may never appear.
var forbiddenLabels = []string{"trace_id", "span_id", "conn_id", "seq", "url", "host", "hostname", "instance", "ip", "pod", "node", "az", "client_ip", "peer_ip"}

// checkSeries asserts, for every series: exactly the row's labels plus the
// universal pairs with the configured values; mode equals the process mode;
// no forbidden label; no value equal to a URL, host or conn_id.
func checkSeries(t *testing.T, s *Scrape, mode string, forbiddenValues []string) {
	t.Helper()
	for name, mf := range s.Fams {
		row, ok := specRows[name]
		if !ok {
			t.Errorf("TC-TEL-01: family %s is not in specRows", name)
			continue
		}
		want := append(slices.Clone(row), slices.Collect(maps.Keys(universal))...)
		sort.Strings(want)
		for _, m := range mf.GetMetric() {
			ls := labelMap(m)
			got := slices.Sorted(maps.Keys(ls))
			if !slices.Equal(got, want) {
				t.Errorf("TC-TEL-01: %s labels %v, want %v", name, got, want)
			}
			for k, v := range universal {
				if ls[k] != v {
					t.Errorf("TC-TEL-01: %s %s=%q, want %q", name, k, ls[k], v)
				}
			}
			if m, has := ls["mode"]; has && m != mode {
				t.Errorf("%s mode=%q, want %q", name, m, mode)
			}
			for k, v := range ls {
				if slices.Contains(forbiddenLabels, k) {
					t.Errorf("TC-TEL-02: %s has forbidden label %s", name, k)
				}
				if slices.Contains(forbiddenValues, v) {
					t.Errorf("TC-TEL-02: %s label %s=%q is a URL/host/conn_id", name, k, v)
				}
			}
		}
		if oneWayPattern.MatchString(name) {
			t.Errorf("TC-TIME-05: family %s looks like one-way latency", name)
		}
	}
}

// TC-TEL-01, TC-TEL-02, TC-TIME-05: per mode (plus pingpong+push), after a
// remote close, a reconnect and traffic, the exposed families equal the
// applicable specRows set exactly; every series carries exactly its row's
// labels plus the universal pairs; no forbidden label or value; no one-way
// family. The union over all runs (plus remote-write, TC-TEL-11) is the
// whole of specRows.
func TestTCTEL01_02_TIME05_MetricSet(t *testing.T) {
	seen := map[string]bool{}
	for _, tc := range []struct {
		name string
		env  func(url string) map[string]string
		mode string
		push bool
		// ready is the mode-specific traffic to wait for.
		ready func(s *Scrape) bool
	}{
		{"pingpong", func(u string) map[string]string { return probeEnv(u) }, "pingpong", false,
			func(s *Scrape) bool { return s.HistCount("wsmon_client_rtt_seconds", L{"payload": "large"}) >= 1 }},
		{"pingpong+push", func(u string) map[string]string { return pushEnv(u) }, "pingpong", true,
			func(s *Scrape) bool { return s.HistCount("wsmon_client_push_gap_seconds", nil) >= 1 }},
		{"burst", func(u string) map[string]string {
			return probeEnv(u, "WSMON_MODE", "burst", "WSMON_BURST_SIZE", "5", "WSMON_BURST_INTERVAL", "100ms")
		}, "burst", false, func(s *Scrape) bool { return s.HistCount("wsmon_client_burst_completion_seconds", nil) >= 1 }},
		{"idle", func(u string) map[string]string { return idleEnv(u) }, "idle", false,
			func(s *Scrape) bool { return s.Counter("wsmon_client_idle_probe_total", L{"result": "survived"}) >= 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The first connection is drained (1001) early, so close,
			// lifetime and reconnect series exist; in idle this drops the
			// first rung, so wsmon_client_idle_probe_close_total appears too.
			srv := fakeserver.New(t, fakeserver.Behaviour{OnHello: func(c *fakeserver.Conn) {
				if c.Index == 0 {
					time.Sleep(30 * time.Millisecond)
					c.Drain()
				}
			}})
			env := tc.env(srv.URL)
			env["WSMON_LABELS"] = telLabels
			h := newTelHarness(t, env, false)
			s := h.Wait("traffic after a reconnect", func(s *Scrape) bool {
				return s.Counter("wsmon_client_closes_total", L{"close_code": "1001", "initiator": "remote"}) == 1 &&
					s.Value("wsmon_client_connection_up", nil) == 1 && s.HistCount("wsmon_client_rtt_seconds", nil) >= 1 && tc.ready(s)
			}, D(30*time.Second, 10*time.Second))

			want := expectedFamilies(tc.mode, tc.push, false)
			if got := s.Families(); !slices.Equal(got, want) {
				t.Errorf("TC-TEL-01 families:\n got  %v\n want %v", got, want)
			}
			forbidden := []string{srv.URL, srv.Addr, "127.0.0.1", strings.TrimPrefix(srv.URL, "ws://")}
			for _, c := range srv.Upgraded() {
				if id := c.ConnID(); id != "" {
					forbidden = append(forbidden, id)
				}
			}
			checkSeries(t, s, tc.mode, forbidden)
			for _, f := range s.Families() {
				seen[f] = true
			}
		})
	}
	for _, f := range rwOnly { // covered by TC-TEL-11
		seen[f] = true
	}
	for name := range specRows {
		if !seen[name] {
			t.Errorf("TC-TEL-01: metric %s never exposed in any mode", name)
		}
	}
	for name := range seen {
		if strings.HasPrefix(name, "wsmon_client_") && specRows[name] == nil {
			t.Errorf("TC-TIME-05: %s is not on the allow-list", name)
		}
	}
}

// exemplarLine matches a classic bucket with an OpenMetrics exemplar.
var exemplarLine = regexp.MustCompile(`(?m)^(wsmon_client_rtt_seconds|wsmon_client_rtt_net_seconds)_bucket\{[^}]*\} \S+ # \{trace_id="([0-9a-f]{32})"\} \S+`)

func exemplars(om string) map[string][]string {
	out := map[string][]string{}
	for _, m := range exemplarLine.FindAllStringSubmatch(om, -1) {
		out[m[1]] = append(out[m[1]], m[2])
	}
	return out
}

func roundTripTraceIDs(h *telHarness) map[string]float64 {
	out := map[string]float64{}
	for _, s := range h.spans.GetSpans() {
		if s.Name != probetel.SpanRoundTrip {
			continue
		}
		rtt := -1.0
		for _, a := range s.Attributes {
			if a.Key == probetel.AttrRTTMs {
				rtt = a.Value.AsFloat64()
			}
		}
		out[s.SpanContext.TraceID().String()] = rtt
	}
	return out
}

// TC-TEL-04: with sampling at 1.0, the OpenMetrics scrape carries exemplars
// on wsmon_client_rtt_seconds and wsmon_client_rtt_net_seconds whose trace_id is an exported
// ws.roundtrip. With sampling at 0, only round trips slower than
// TRACE_SLOW_THRESHOLD are exported and carry exemplars.
func TestTCTEL04_Exemplars(t *testing.T) {
	t.Run("head-sampled", func(t *testing.T) {
		srv := fakeserver.New(t, fakeserver.Behaviour{})
		h := newTelHarness(t, probeEnv(srv.URL, "WSMON_TRACE_SAMPLE_RATIO", "1"), true)
		h.Wait("5 round trips", func(s *Scrape) bool { return s.HistCount("wsmon_client_rtt_seconds", nil) >= 5 }, 10*time.Second)
		ex := exemplars(scrapeOM(t, h.base))
		ids := roundTripTraceIDs(h)
		for _, fam := range []string{"wsmon_client_rtt_seconds", "wsmon_client_rtt_net_seconds"} {
			if len(ex[fam]) == 0 {
				t.Errorf("no exemplar on %s", fam)
			}
			for _, id := range ex[fam] {
				if _, ok := ids[id]; !ok {
					t.Errorf("%s exemplar trace_id %s is not an exported ws.roundtrip", fam, id)
				}
			}
		}
	})
	t.Run("slow-upgrade", func(t *testing.T) {
		const delay = 60 * time.Millisecond
		srv := fakeserver.New(t, fakeserver.Behaviour{OnProbe: func(c *fakeserver.Conn, p fakeserver.Probe) {
			if p.Seq%2 == 0 {
				go func() { time.Sleep(delay); _ = c.Echo(p) }()
				return
			}
			_ = c.Echo(p)
		}})
		h := newTelHarness(t, probeEnv(srv.URL, "WSMON_TRACE_SAMPLE_RATIO", "0",
			"WSMON_TRACE_SLOW_THRESHOLD", "40ms", "WSMON_PAYLOADS", "small"), true)
		h.Wait("6 round trips", func(s *Scrape) bool { return s.HistCount("wsmon_client_rtt_seconds", nil) >= 6 }, 10*time.Second)
		ex := exemplars(scrapeOM(t, h.base))
		ids := roundTripTraceIDs(h)
		if len(ids) == 0 || len(ex["wsmon_client_rtt_seconds"]) == 0 {
			t.Fatalf("no slow round trip exported (%d spans, exemplars %v)", len(ids), ex)
		}
		for id, rtt := range ids {
			if rtt < 40 {
				t.Errorf("fast round trip %s (%.1f ms) exported at ratio 0", id, rtt)
			}
		}
		for _, id := range ex["wsmon_client_rtt_seconds"] {
			if _, ok := ids[id]; !ok {
				t.Errorf("exemplar %s without an exported span", id)
			}
		}
		om := scrapeOM(t, h.base)
		for _, line := range strings.Split(om, "\n") {
			if strings.HasPrefix(line, `wsmon_client_rtt_seconds_bucket{`) && strings.Contains(line, `le="0.025"`) && strings.Contains(line, "# {") {
				t.Errorf("fast bucket carries an exemplar: %s", line)
			}
		}
	})
}

// TC-TEL-06 (scaled stand-in for 2 h): several hundred round trips across
// forced reconnects, plus idle rungs. No exported span outlasts one round
// trip (MESSAGE_TIMEOUT + margin; connect spans: CONNECT_TIMEOUT), and no
// span is named after a connection or a silence.
func TestTCTEL06_NoLongSpans(t *testing.T) {
	parallelLong(t)
	const margin = 50 * time.Millisecond
	allowed := []string{probetel.SpanRoundTrip, probetel.SpanConnect, probetel.SpanPushAck, "dns", "tcp", "tls", "upgrade", "hello"}
	check := func(t *testing.T, h *telHarness) int {
		n := 0
		for _, s := range h.spans.GetSpans() {
			d := s.EndTime.Sub(s.StartTime)
			limit := h.cfg.MessageTimeout + margin
			switch s.Name {
			case probetel.SpanRoundTrip:
				n++
			case probetel.SpanPushAck:
			default:
				limit = h.cfg.ConnectTimeout
			}
			if !slices.Contains(allowed, s.Name) {
				t.Errorf("unexpected span %q", s.Name)
			}
			if d > limit || d < 0 {
				t.Errorf("span %s lasted %v > %v", s.Name, d, limit)
			}
		}
		return n
	}

	t.Run("pingpong+push with reconnects", func(t *testing.T) {
		srv := fakeserver.New(t, fakeserver.Behaviour{OnHello: func(c *fakeserver.Conn) {
			time.Sleep(D(2*time.Second, 500*time.Millisecond))
			c.Kill()
		}})
		env := pushEnv(srv.URL)
		env["WSMON_PINGPONG_INTERVAL"] = "20ms"
		env["WSMON_TRACE_SAMPLE_RATIO"] = "1"
		h := newTelHarness(t, env, true)
		run := D(20*time.Second, 3*time.Second)
		time.Sleep(run)
		s := h.Scrape()
		h.Stop()
		n := check(t, h)
		reconnects := s.Counter("wsmon_client_reconnects_total", nil)
		t.Logf("%v: %d round-trip spans, %d spans total, %v reconnects", run, n, len(h.spans.GetSpans()), reconnects)
		if n < int(D(300, 50)) || reconnects < float64(D(5, 2)) {
			t.Errorf("too little traffic for the test to mean anything: %d round trips, %v reconnects", n, reconnects)
		}
	})
	t.Run("idle", func(t *testing.T) {
		srv := fakeserver.New(t, fakeserver.Behaviour{})
		env := idleEnv(srv.URL, "WSMON_TRACE_SAMPLE_RATIO", "1")
		h := newTelHarness(t, env, true)
		h.Wait("3 rungs", func(s *Scrape) bool {
			return s.Counter("wsmon_client_idle_probe_total", L{"result": "survived"}) >= 3
		}, D(30*time.Second, 10*time.Second))
		h.Stop()
		if n := check(t, h); n < 3 {
			t.Errorf("%d idle round-trip spans", n)
		}
		for _, s := range h.spans.GetSpans() {
			if s.Name != probetel.SpanRoundTrip {
				continue
			}
			var bucket bool
			for _, a := range s.Attributes {
				bucket = bucket || a.Key == probetel.AttrIdleBucket
			}
			if !bucket {
				t.Error("idle ws.roundtrip without wsmon.idle_bucket")
			}
		}
	})
}

// sinkKeys returns the series keys of one remote-write request.
func sinkKeys(r rwsink.Request) []string {
	out := make([]string, 0, len(r.Samples))
	for _, s := range r.Samples {
		out = append(out, s.Key())
	}
	sort.Strings(out)
	return out
}

func diff(a, b []string) (onlyA, onlyB []string) {
	for _, x := range a {
		if !slices.Contains(b, x) {
			onlyA = append(onlyA, x)
		}
	}
	for _, x := range b {
		if !slices.Contains(a, x) {
			onlyB = append(onlyB, x)
		}
	}
	return
}

// TC-TEL-11: remote-write against an in-test receiver. The pushed series
// equal the /metrics series plus job="wsmon-probe", with no instance label.
// A hanging, then refusing, receiver raises the failure counter without
// affecting the probe loop lag or the RTT observation rate. Shutdown does a
// final push.
func TestTCTEL11_RemoteWrite(t *testing.T) {
	parallelLong(t)
	sink := rwsink.Start(t)
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	env := probeEnv(srv.URL,
		"WSMON_LABELS", telLabels,
		"WSMON_PINGPONG_INTERVAL", "20ms",
		"WSMON_REMOTE_WRITE_URL", sink.URL,
		"WSMON_REMOTE_WRITE_INTERVAL", D(time.Second, 200*time.Millisecond).String(),
		"WSMON_REMOTE_WRITE_TIMEOUT", D(time.Second, 300*time.Millisecond).String(),
		"WSMON_REMOTE_WRITE_BEARER_TOKEN", canaryRW)
	h := newTelHarness(t, env, false)
	h.Wait("round trips", func(s *Scrape) bool { return s.HistCount("wsmon_client_rtt_seconds", L{"payload": "large"}) >= 1 }, 10*time.Second)
	time.Sleep(D(60*time.Second, 3*time.Second))

	// Phase 1: healthy. Compare a scrape with the next push after it.
	var matched bool
	for attempt := 0; attempt < 5 && !matched; attempt++ {
		s := h.Scrape()
		at := time.Now()
		var req rwsink.Request
		h.Wait("a push after the scrape", func(*Scrape) bool {
			rs := sink.Requests()
			if len(rs) > 0 && rs[len(rs)-1].At.After(at) {
				req = rs[len(rs)-1]
				return true
			}
			return false
		}, 10*time.Second)
		if req.Err != nil {
			t.Fatalf("decode: %v", req.Err)
		}
		pulled, pushed := s.SeriesKeys(L{"job": probetel.RemoteWriteJob}), sinkKeys(req)
		onlyPulled, onlyPushed := diff(pulled, pushed)
		if len(onlyPulled) == 0 && len(onlyPushed) == 0 {
			matched = true
			hdr := req.Header
			if hdr.Get("Content-Encoding") != "snappy" || hdr.Get("Content-Type") != "application/x-protobuf" ||
				hdr.Get("X-Prometheus-Remote-Write-Version") != "0.1.0" || hdr.Get("User-Agent") != "wsmon-probe/0.0.0-test" ||
				hdr.Get("Authorization") != "Bearer "+canaryRW {
				t.Errorf("headers %v", hdr)
			}
			for _, smp := range req.Samples {
				if _, ok := smp.Labels["instance"]; ok {
					t.Fatalf("instance label on %s", smp.Key())
				}
				if smp.Labels["job"] != probetel.RemoteWriteJob || smp.Labels["site"] != "tel-site" {
					t.Fatalf("labels %s", smp.Key())
				}
				if d := time.Since(time.UnixMilli(smp.TimestampMs)); d < 0 || d > 30*time.Second {
					t.Fatalf("timestamp %d is %v old", smp.TimestampMs, d)
				}
			}
			t.Logf("%d series pushed == pulled", len(pushed))
		} else if attempt == 4 {
			t.Errorf("pushed != pulled\nonly pulled: %v\nonly pushed: %v", onlyPulled, onlyPushed)
		}
	}
	if strings.Contains(h.logs.String(), canaryRW) {
		t.Error("remote-write token leaked into logs")
	}

	// Phase 2: baseline window vs hanging-receiver window.
	window := D(10*time.Second, 2*time.Second)
	lagHigh := func(s *Scrape) float64 {
		return float64(s.HistCount("wsmon_client_loop_lag_seconds", nil) - s.HistBucket("wsmon_client_loop_lag_seconds", 0.1, nil))
	}
	s0 := h.Scrape()
	time.Sleep(window)
	s1 := h.Scrape()
	sink.SetMode(rwsink.Hang)
	time.Sleep(window)
	s2 := h.Scrape()
	rttBase := float64(s1.HistCount("wsmon_client_rtt_seconds", nil) - s0.HistCount("wsmon_client_rtt_seconds", nil))
	rttHang := float64(s2.HistCount("wsmon_client_rtt_seconds", nil) - s1.HistCount("wsmon_client_rtt_seconds", nil))
	failHang := s2.Counter("wsmon_client_remote_write_failures_total", nil) - s1.Counter("wsmon_client_remote_write_failures_total", nil)
	meanBase := (s1.HistSum("wsmon_client_rtt_seconds", nil) - s0.HistSum("wsmon_client_rtt_seconds", nil)) / max(rttBase, 1)
	meanHang := (s2.HistSum("wsmon_client_rtt_seconds", nil) - s1.HistSum("wsmon_client_rtt_seconds", nil)) / max(rttHang, 1)
	t.Logf("baseline: %v RTTs (mean %.2f ms), lag>100ms %v; hanging receiver: %v RTTs (mean %.2f ms), lag>100ms %v, %v failures, %d hung",
		rttBase, meanBase*1e3, lagHigh(s1)-lagHigh(s0), rttHang, meanHang*1e3, lagHigh(s2)-lagHigh(s1), failHang, sink.Hung())
	if failHang < 1 || sink.Hung() < 1 {
		t.Errorf("hanging receiver: failures +%v, hung %d", failHang, sink.Hung())
	}
	if rttHang < 0.7*rttBase {
		t.Errorf("RTT observations fell from %v to %v while the receiver hung", rttBase, rttHang)
	}
	if lagHigh(s2)-lagHigh(s1) > lagHigh(s1)-lagHigh(s0)+1 {
		t.Errorf("loop lag > 100 ms rose while the receiver hung")
	}

	// Phase 3: refusing receiver.
	sink.SetMode(rwsink.Refuse)
	h.Wait("failures while refused", func(s *Scrape) bool {
		return s.Counter("wsmon_client_remote_write_failures_total", nil) >= s2.Counter("wsmon_client_remote_write_failures_total", nil)+2
	}, 10*time.Second)
	if n := len(h.logs.Find("WARN", "remote-write push failed")); n != 1 {
		t.Errorf("%d remote-write WARNs, want 1 (rate-limited to one per minute)", n)
	}

	// Phase 4: healthy again; shutdown does one final push.
	sink.SetMode(rwsink.OK)
	h.Wait("recovered", func(s *Scrape) bool {
		return s.Value("wsmon_client_remote_write_last_success_timestamp_seconds", nil) > float64(time.Now().Add(-5*time.Second).Unix())
	}, 10*time.Second)
	stopAt := time.Now()
	h.Stop()
	rs := sink.Requests()
	if last := rs[len(rs)-1]; !last.At.After(stopAt) {
		t.Errorf("no final push on shutdown (last push %v before stop)", stopAt.Sub(last.At))
	}
}

// TC-TEL-05 / TC-TEL-10, client half. Through the real OTLP gRPC exporter to
// otlpsink: every ws.roundtrip carries the trace_id/span_id the server saw
// in the probe frame (so the server's ws.echo becomes its child), and every
// ws.push_ack is a child of the push frame's traceparent. The contract tests run the
// same assertions against the real server's ws.echo / ws.push spans.
func TestTCTEL05_10_ClientHalf(t *testing.T) {
	sinkSrv := otlpsink.Start(t, "127.0.0.1:0")
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	env := pushEnv(srv.URL)
	env["WSMON_PINGPONG_INTERVAL"] = "50ms"
	env["WSMON_OTLP_ENDPOINT"] = sinkSrv.Addr
	env["WSMON_TRACE_SAMPLE_RATIO"] = "1"
	env["WSMON_LABELS"] = telLabels
	h := newTelHarness(t, env, false)
	h.Wait("round trips and pushes", func(s *Scrape) bool {
		return s.HistCount("wsmon_client_rtt_seconds", nil) >= 3 && s.Counter("wsmon_client_push_received_total", nil) >= 3
	}, 10*time.Second)
	h.Stop() // flushes the batch processor

	c := srv.Upgraded()[0]
	probes := c.Probes()
	for _, p := range probes {
		if !strings.HasSuffix(p.Traceparent, "-01") {
			t.Errorf("probe %d traceparent %s not sampled at ratio 1 (C12)", p.Seq, p.Traceparent)
		}
	}
	echoed := 0
	for _, p := range probes[:len(probes)-1] { // the last may be in flight at shutdown: still exported (lost)
		sp, ok := sinkSrv.ByID(p.TraceID, p.SpanID)
		if !ok || sp.Name != probetel.SpanRoundTrip || sp.ParentSpanID != "" {
			t.Errorf("no root ws.roundtrip with wire IDs %s/%s", p.TraceID, p.SpanID)
			continue
		}
		echoed++
		if sp.Attrs["conn.id"] != p.ConnID || sp.Attrs["msg.seq"] != p.Seq || sp.Attrs["wsmon.site"] != "tel-site" ||
			sp.Attrs["server.name"] != fakeserver.DefaultServer.Name {
			t.Errorf("roundtrip attrs %v", sp.Attrs)
		}
	}
	pushes := c.SentPushes()
	acks := sinkSrv.Named(probetel.SpanPushAck)
	parents := map[string]string{}
	for _, a := range acks {
		parents[a.ParentSpanID] = a.TraceID
	}
	for _, p := range pushes[:min(len(pushes), len(acks))] {
		if parents[p.SpanID] != p.TraceID {
			t.Errorf("push %d (span %s): no ws.push_ack child", p.PushSeq, p.SpanID)
		}
	}
	if echoed < 2 || len(acks) < 2 {
		t.Errorf("%d roundtrip spans, %d push_ack spans", echoed, len(acks))
	}
}

// histogramFamilies is used by TC-TEL-09 style checks over a scrape.
func histogramFamilies(s *Scrape) []string {
	var out []string
	for n, mf := range s.Fams {
		if mf.GetType() == dto.MetricType_HISTOGRAM {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

// TC-TEL-09 over the wire: every histogram on /metrics has the documented bounds,
// and wsmon_client_rtt_seconds has le="0.25" exactly.
func TestTCTEL09_BucketsOnTheWire(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	h := newTelHarness(t, probeEnv(srv.URL), false)
	s := h.Wait("an RTT", func(s *Scrape) bool { return s.HistCount("wsmon_client_rtt_seconds", nil) >= 1 }, 10*time.Second)
	if !strings.Contains(s.Raw, `le="0.25"`) {
		t.Error(`no le="0.25" in /metrics`)
	}
	want := map[string][]float64{
		"wsmon_client_rtt_seconds":               telemetry.RTTBuckets,
		"wsmon_client_rtt_net_seconds":           telemetry.RTTBuckets,
		"wsmon_client_server_processing_seconds": telemetry.HandleBuckets,
		"wsmon_client_connect_duration_seconds":  probetel.ConnectBuckets,
		"wsmon_client_loop_lag_seconds":          probetel.LoopLagBuckets,
	}
	for _, name := range histogramFamilies(s) {
		wb, ok := want[name]
		if !ok {
			continue
		}
		for _, m := range s.Fams[name].GetMetric() {
			var got []float64
			for _, b := range m.GetHistogram().GetBucket() {
				if b.GetUpperBound() < 1e300 {
					got = append(got, b.GetUpperBound())
				}
			}
			if !slices.Equal(got, wb) {
				t.Errorf("%s buckets %v", name, got)
			}
		}
	}
}
