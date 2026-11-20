package contract

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel/attribute"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/test/client/otlpsink"
	"github.com/asappinc-opensource/wsmon/test/client/wirecap"
)

// TestContract runs every TC-CONTRACT-* case plus TC-TEL-05/10
// against a real server — `wsmon server` from this commit by default, or a
// released image (server_ctl_test.go) — in a fixed order and never in
// parallel:
// server counters are cumulative and are asserted as deltas, TC-08 restarts
// the server so it runs after every delta test, and TC-01 decodes the frames
// captured by all the others, so it runs last.
func TestContract(t *testing.T) {
	// Both the probe and the server export here: a local server on
	// loopback, an image server to wsmon-contract-runner:4317 (Makefile).
	sink := otlpsink.Start(t, "0.0.0.0:4317")
	if !setupServer(t) {
		t.Skip("needs -tags wsmontest (TC-CONTRACT-11); run `make test`")
	}
	if !serverReady() {
		t.Fatalf("server %s is not ready\n%s", env.AdminBase, serverLogs(40))
	}
	steps := []struct {
		name string
		fn   func(*testing.T)
	}{
		{"TC-CONTRACT-02", tc02},
		{"TC-CONTRACT-03", tc03},
		{"TC-CONTRACT-04", tc04},
		{"TC-CONTRACT-05", tc05},
		{"TC-CONTRACT-06", tc06},
		{"TC-CONTRACT-07", tc07},
		{"TC-CONTRACT-09", tc09},
		{"TC-CONTRACT-10", tc10},
		{"TC-CONTRACT-11", tc11},
		{"TC-CONTRACT-12", tc12},
		{"TC-CONTRACT-13", tc13},
		{"TC-TEL-05", func(t *testing.T) { tel05(t, sink) }},
		{"TC-TEL-10", func(t *testing.T) { tel10(t, sink) }},
		{"TC-CONTRACT-14", tc14},
		{"TC-CONTRACT-08", tc08},
		{"TC-CONTRACT-01", tc01},
	}
	for _, s := range steps {
		start := time.Now()
		t.Run(s.name, s.fn)
		t.Logf("%s took %v", s.name, time.Since(start).Round(time.Millisecond))
	}
}

const otlpLocal = "127.0.0.1:4317"

func up(s *Scrape) bool { return s.Value("wsmon_client_connection_up", nil) == 1 }

func echoedAtLeast(n float64) func(*Scrape) bool {
	return func(s *Scrape) bool { return s.Value("wsmon_client_messages_echoed_total", nil) >= n }
}

// Final reads the probe's registry directly. It works after Stop, when the
// /metrics listener is already shut.
func (h *harness) Final() *Scrape {
	h.t.Helper()
	mfs, err := h.tel.Gatherer().Gather()
	if err != nil {
		h.t.Fatal(err)
	}
	s := &Scrape{Fams: map[string]*dto.MetricFamily{}}
	for _, mf := range mfs {
		s.Fams[mf.GetName()] = mf
	}
	return s
}

// onlyCap returns the single connection a test expected.
func (h *harness) onlyCap() *wirecap.Conn {
	h.t.Helper()
	caps := h.Caps()
	if len(caps) != 1 {
		h.t.Fatalf("%d connections, want exactly 1\nclient logs:\n%s", len(caps), tail(h.logs.String(), 30))
	}
	return caps[0]
}

// serverErrorDeltas fails on any wsmon_server_errors_total increase.
func serverErrorDeltas(t *testing.T, before, after *Scrape) {
	t.Helper()
	for _, c := range protocol.ErrorCodes {
		l := L{"code": string(c)}
		if d := after.Value("wsmon_server_errors_total", l) - before.Value("wsmon_server_errors_total", l); d != 0 {
			t.Errorf("server wsmon_server_errors_total{code=%q} delta = %v, want 0", c, d)
		}
	}
	if b, a := before.Value("wsmon_server_errors_total", nil), after.Value("wsmon_server_errors_total", nil); a != b {
		t.Errorf("server wsmon_server_errors_total (all codes) delta = %v, want 0", a-b)
	}
}

func noClientServerErrors(t *testing.T, s *Scrape) {
	t.Helper()
	if v := s.Value("wsmon_client_server_errors_received_total", nil); v != 0 {
		t.Errorf("wsmon_client_server_errors_received_total = %v, want 0 (%v)", v, s.ByLabel("wsmon_client_server_errors_received_total", "code"))
	}
}

// status101 reports whether the connection completed an upgrade.
func status101(c *wirecap.Conn) bool {
	head := wirecap.Head(c.Reads())
	return strings.HasPrefix(string(head), "HTTP/1.1 101 ")
}

// ---------------------------------------------------------------------------

// TC-CONTRACT-02: upgrade with token → 101 and wsmon.v1 negotiated (S1, C1).
func tc02(t *testing.T) {
	h := newProbe(t, probeEnv(), false)
	h.Wait("connection up", up, 15*time.Second)
	h.Stop()
	c := h.onlyCap()
	resp := responseHead(t, c)
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status %d, want 101", resp.StatusCode)
	}
	if got := resp.Header.Get("Sec-WebSocket-Protocol"); got != protocol.Subprotocol {
		t.Errorf("negotiated subprotocol %q, want %q", got, protocol.Subprotocol)
	}
	req := requestHead(t, c)
	if got := req.Header.Get("Sec-WebSocket-Protocol"); got != protocol.Subprotocol {
		t.Errorf("C1: requested subprotocol %q", got)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer "+env.Token {
		t.Errorf("C1: Authorization header %q", got)
	}
}

// TC-CONTRACT-03: hello per mode → hello_ack within 1 s, accepted_options ==
// requested by value, conn_id matches (S3, C3, C4).
func tc03(t *testing.T) {
	cases := []struct {
		name string
		kv   []string
		want protocol.Options
	}{
		{"pingpong", nil, protocol.Options{}},
		{"pingpong+push", []string{"WSMON_PUSH_INTERVAL_MS", "1000"}, protocol.Options{PushIntervalMs: 1000}},
		{"burst", []string{"WSMON_MODE", "burst"}, protocol.Options{}},
		{"idle", []string{"WSMON_MODE", "idle"}, protocol.Options{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newProbe(t, probeEnv(tc.kv...), false)
			h.Wait("connection up", up, 15*time.Second)
			h.Stop()
			c := h.onlyCap()
			w := decodeWire(t, c)
			if w.Hello == nil {
				t.Fatal("no hello sent")
			}
			if w.Hello.Options != tc.want {
				t.Errorf("requested options %+v, want %+v", w.Hello.Options, tc.want)
			}
			if len(w.Recv) == 0 || w.RecvErr[0] != nil || w.Recv[0].Type != protocol.TypeHelloAck {
				t.Fatalf("first server frame is not a hello_ack: %+v %v", w.Recv, w.RecvErr)
			}
			ack := w.Recv[0].HelloAck
			if ack.AcceptedOptions != w.Hello.Options {
				t.Errorf("accepted_options %+v != requested %+v", ack.AcceptedOptions, w.Hello.Options)
			}
			if ack.ConnID != w.Hello.ConnID || !protocol.ValidULID(ack.ConnID) {
				t.Errorf("hello_ack conn_id %q, hello conn_id %q", ack.ConnID, w.Hello.ConnID)
			}
			written, err := c.WrittenFrames()
			if err != nil || len(written) == 0 {
				t.Fatalf("written frames: %v", err)
			}
			read, err := c.ReadFrames()
			if err != nil {
				t.Fatal(err)
			}
			ackAt := read[w.From[0].Index].At
			rtt := ackAt.Sub(written[0].At)
			if rtt < 0 || rtt >= time.Second {
				t.Errorf("hello → hello_ack took %v, want < 1s", rtt)
			}
			// C4: hello immediately after the 101.
			gap := written[0].At.Sub(c.Reads()[0].At)
			t.Logf("hello_ack after %v; hello written %v after the first response bytes", rtt, gap)
			if gap > time.Second {
				t.Errorf("C4: hello written %v after the 101", gap)
			}
		})
	}
}

// TC-CONTRACT-04: 100 pingpong round trips (S4, S5, S6).
func tc04(t *testing.T) {
	h := newProbe(t, probeEnv("WSMON_PINGPONG_INTERVAL", "50ms", "WSMON_TRACE_SAMPLE_RATIO", "1"), true)
	h.Wait("100 echoes", echoedAtLeast(100), 60*time.Second)
	h.Stop()
	fin := h.Final()
	for _, n := range []string{"wsmon_client_messages_mismatched_total", "wsmon_client_messages_unexpected_total", "wsmon_client_messages_duplicate_total",
		"wsmon_client_messages_out_of_order_total", "wsmon_client_messages_lost_total", "wsmon_client_messages_late_total"} {
		if v := fin.Value(n, nil); v != 0 {
			t.Errorf("%s = %v, want 0", n, v)
		}
	}
	w := decodeWire(t, h.onlyCap())
	sent := map[int64]protocol.Probe{}
	for i, p := range w.Probes {
		if p.Seq != int64(i+1) || p.ConnID != w.Hello.ConnID {
			t.Errorf("C5: probe %d has seq %d conn_id %q", i, p.Seq, p.ConnID)
		}
		sent[p.Seq] = p
	}
	echoes := w.Of(protocol.TypeEcho)
	if len(echoes) < 100 {
		t.Fatalf("%d echoes on the wire, want ≥ 100", len(echoes))
	}
	seen := map[int64]bool{}
	var maxST time.Duration
	for _, f := range echoes {
		e := f.Echo
		p, ok := sent[e.Seq]
		switch {
		case !ok:
			t.Errorf("echo for unsent seq %d", e.Seq)
			continue
		case seen[e.Seq]:
			t.Errorf("duplicate echo seq %d", e.Seq)
		}
		seen[e.Seq] = true
		if e.TraceID != p.TraceID || e.ConnID != p.ConnID || e.ClientSendEpochNs != p.ClientSendEpochNs {
			t.Errorf("S4: seq %d echo (%s, %s, %d) != probe (%s, %s, %d)", e.Seq,
				e.TraceID, e.ConnID, e.ClientSendEpochNs, p.TraceID, p.ConnID, p.ClientSendEpochNs)
		}
		if e.Server.Name == "" || e.Server.Version == "" {
			t.Errorf("S6: seq %d server info %+v has an empty field", e.Seq, e.Server)
		}
		st := time.Duration(e.ServerSendEpochNs - e.ServerRecvEpochNs)
		if st < 0 || st >= 10*time.Millisecond {
			t.Errorf("S5: seq %d t3−t2 = %v, want 0 ≤ t3−t2 < 10ms", e.Seq, st)
		}
		maxST = max(maxST, st)
	}
	var spans, okSpans int
	for _, s := range h.spans.GetSpans() {
		if s.Name != "ws.roundtrip" {
			continue
		}
		spans++
		a := attrMap(s.Attributes)
		rtt, ok1 := a["wsmon.rtt_ms"]
		net, ok2 := a["wsmon.rtt_net_ms"]
		if !ok1 || !ok2 {
			continue
		}
		okSpans++
		if net.AsFloat64() > rtt.AsFloat64() {
			t.Errorf("rtt_net %v ms > rtt %v ms (seq %v)", net.AsFloat64(), rtt.AsFloat64(), a["msg.seq"].AsInt64())
		}
	}
	if okSpans < 100 {
		t.Errorf("%d ws.roundtrip spans with rtt (of %d), want ≥ 100", okSpans, spans)
	}
	t.Logf("%d probes, %d echoes, max t3−t2 %v, %d round-trip spans checked", len(w.Probes), len(echoes), maxST, okSpans)
}

func attrMap(kvs []attribute.KeyValue) map[string]attribute.Value {
	m := make(map[string]attribute.Value, len(kvs))
	for _, kv := range kvs {
		m[string(kv.Key)] = kv.Value
	}
	return m
}

// TC-CONTRACT-05: large payload round trip; echo has no padding and is
// < 1 KiB (S4, S12).
func tc05(t *testing.T) {
	h := newProbe(t, probeEnv("WSMON_PAYLOADS", "large"), false)
	h.Wait("a large echo", func(s *Scrape) bool { return s.Value("wsmon_client_messages_echoed_total", L{"payload": "large"}) >= 1 }, 20*time.Second)
	h.Stop()
	w := decodeWire(t, h.onlyCap())
	if len(w.Probes) == 0 {
		t.Fatal("no probe sent")
	}
	for _, m := range w.To[1:] {
		if len(m.Payload) != h.cfg.LargePayloadBytes {
			t.Errorf("large probe frame is %d bytes, want %d", len(m.Payload), h.cfg.LargePayloadBytes)
		}
	}
	n := 0
	for i, m := range w.From {
		if w.RecvErr[i] != nil || w.Recv[i].Type != protocol.TypeEcho {
			continue
		}
		n++
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(m.Payload, &raw); err != nil {
			t.Fatal(err)
		}
		if _, ok := raw["padding"]; ok {
			t.Errorf("S4: echo seq %d contains padding", w.Recv[i].Echo.Seq)
		}
		if len(m.Payload) >= 1024 {
			t.Errorf("echo seq %d is %d bytes, want < 1 KiB", w.Recv[i].Echo.Seq, len(m.Payload))
		}
	}
	if n == 0 {
		t.Fatal("no echo frame captured")
	}
}

// TC-CONTRACT-06: push at 200 ms for 3 s (S8, C9).
func tc06(t *testing.T) {
	before := waitServerIdle(t)
	h := newProbe(t, probeEnv("WSMON_PUSH_INTERVAL_MS", "200"), false)
	h.Wait("connection up", up, 15*time.Second)
	time.Sleep(3 * time.Second)
	h.Stop()
	after := waitServerIdle(t)
	fin := h.Final()
	w := decodeWire(t, h.onlyCap())
	pushes := w.Of(protocol.TypePush)
	t.Logf("%d pushes in 3 s at 200 ms, %d push_acks", len(pushes), len(w.Acks))
	if len(pushes) < 10 {
		t.Errorf("only %d pushes in 3 s at 200 ms", len(pushes))
	}
	byTrace := map[string]protocol.Push{}
	for i, f := range pushes {
		p := f.Push
		if p.PushSeq != int64(i+1) {
			t.Errorf("S8: push %d has push_seq %d, want %d", i, p.PushSeq, i+1)
		}
		if p.ConnID != w.Hello.ConnID {
			t.Errorf("push conn_id %q, want %q", p.ConnID, w.Hello.ConnID)
		}
		tid, sid, _, err := protocol.ParseTraceparent(p.Traceparent)
		if err != nil || tid != p.TraceID || sid != p.SpanID {
			t.Errorf("S8: push %d traceparent %q (trace_id %s span_id %s): %v", p.PushSeq, p.Traceparent, p.TraceID, p.SpanID, err)
		}
		byTrace[p.TraceID] = *p
	}
	acked := map[int64]bool{}
	for _, a := range w.Acks {
		p, ok := byTrace[a.TraceID]
		if !ok || p.ConnID != a.ConnID || p.PushSeq != a.PushSeq {
			t.Errorf("C9: push_ack %+v does not copy a received push", a)
		}
		acked[a.PushSeq] = true
	}
	// Every push but possibly the last (in flight at the client's close)
	// is acked.
	for _, f := range pushes[:max(len(pushes)-1, 0)] {
		if !acked[f.Push.PushSeq] {
			t.Errorf("C9: push_seq %d not acked", f.Push.PushSeq)
		}
	}
	if n := len(w.Of(protocol.TypeError)); n != 0 {
		t.Errorf("%d error frames", n)
	}
	if d := after.HistCount("wsmon_server_push_ack_seconds", nil) - before.HistCount("wsmon_server_push_ack_seconds", nil); d == 0 {
		t.Errorf("server wsmon_server_push_ack_seconds_count delta = 0")
	} else {
		t.Logf("server push_ack_seconds_count delta %d", d)
	}
	serverErrorDeltas(t, before, after)
	for _, n := range []string{"wsmon_client_push_lost_total", "wsmon_client_push_out_of_order_total"} {
		if v := fin.Value(n, nil); v != 0 {
			t.Errorf("%s = %v", n, v)
		}
	}
}

// TC-CONTRACT-07: wrong token, then no token → HTTP 401 → reason="auth"
// once per attempt (S2).
func tc07(t *testing.T) {
	for _, tc := range []struct{ name, token string }{{"wrong-token", "not-the-token"}, {"no-token", "<unset>"}} {
		t.Run(tc.name, func(t *testing.T) {
			h := newProbe(t, probeEnv("WSMON_AUTH_TOKEN", tc.token), false)
			h.Wait("an auth failure", func(s *Scrape) bool {
				return s.Value("wsmon_client_connect_failures_total", L{"reason": "auth"}) >= 1
			}, 15*time.Second)
			h.Stop()
			fin := h.Final()
			attempts := fin.Value("wsmon_client_connect_attempts_total", nil)
			auth := fin.Value("wsmon_client_connect_failures_total", L{"reason": "auth"})
			all := fin.Value("wsmon_client_connect_failures_total", nil)
			// An attempt abandoned by the shutdown is not a failure.
			if all != auth || attempts-auth < 0 || attempts-auth > 1 {
				t.Errorf("attempts %v, auth failures %v, all failures %v (%v)", attempts, auth, all,
					fin.ByLabel("wsmon_client_connect_failures_total", "reason"))
			}
			for i, c := range h.Caps() {
				if head := wirecap.Head(c.Reads()); head == nil {
					continue // shutdown before the response
				}
				if resp := responseHead(t, c); resp.StatusCode != http.StatusUnauthorized {
					t.Errorf("conn %d: HTTP %d, want 401", i, resp.StatusCode)
				}
				got := requestHead(t, c).Header.Get("Authorization")
				if tc.token == "<unset>" && got != "" {
					t.Errorf("Authorization sent without a token: %q", got)
				}
				if tc.token != "<unset>" && got != "Bearer "+tc.token {
					t.Errorf("Authorization %q", got)
				}
			}
			if v := fin.Value("wsmon_client_closes_total", nil); v != 0 {
				t.Errorf("wsmon_client_closes_total = %v for connections that never upgraded", v)
			}
		})
	}
}

// TC-CONTRACT-09: idle, one rung of 120 s (10 s under -short). Zero ping or
// pong frames in either direction; the probe after the silence is echoed
// (S7, C10).
func tc09(t *testing.T) {
	rung := D(120*time.Second, 10*time.Second)
	h := newProbe(t, probeEnv("WSMON_MODE", "idle", "WSMON_IDLE_LADDER", rung.String(),
		"WSMON_MESSAGE_TIMEOUT", D(10*time.Second, 5*time.Second).String()), false)
	bucket := config.IdleBucketLabel(rung)
	h.Wait("the rung survived", func(s *Scrape) bool {
		return s.Value("wsmon_client_idle_probe_total", L{"idle_bucket": bucket, "result": "survived"}) >= 1
	}, rung+30*time.Second)
	h.Stop()
	fin := h.Final()
	c := h.onlyCap()
	to, from, err := c.Frames()
	if err != nil {
		t.Fatal(err)
	}
	if p := wirecap.Pings(to); len(p) != 0 {
		t.Errorf("TC-IDLE-03/C10: client sent %d ping/pong frames: %v", len(p), p)
	}
	if p := wirecap.Pings(from); len(p) != 0 {
		t.Errorf("S7: server sent %d ping/pong frames: %v", len(p), p)
	}
	w := decodeWire(t, c)
	if len(w.Probes) == 0 || w.Probes[0].Seq != 1 {
		t.Fatalf("probes %+v", w.Probes)
	}
	written, _ := c.WrittenFrames()
	if len(written) < 2 || frameType(written[1].Frame) != protocol.TypeProbe {
		t.Fatalf("second client frame is not the probe: %v", written)
	}
	if gap := written[1].At.Sub(written[0].At); gap < rung {
		t.Errorf("silence between hello and probe was %v, want ≥ %v", gap, rung)
	}
	if len(w.Recv) < 2 || w.Recv[1].Type != protocol.TypeEcho || w.Recv[1].Echo.TraceID != w.Probes[0].TraceID {
		t.Errorf("server frames after hello_ack: want the echo of seq 1 first, got %+v", w.Recv)
	}
	if v := fin.Value("wsmon_client_idle_probe_total", L{"result": "dropped"}); v != 0 {
		t.Errorf("%v dropped rungs", v)
	}
	t.Logf("silence %v; %d frames from the server", written[1].At.Sub(written[0].At).Round(time.Millisecond), len(from))
}

func frameType(f wirecap.Frame) string {
	var e struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(f.Payload, &e)
	return e.Type
}

// TC-CONTRACT-10: every mode for 60 s (10 s under -short): zero error frames
// received and a zero server error delta for every code (C2–C8).
func tc10(t *testing.T) {
	dur := D(60*time.Second, 10*time.Second)
	idle := []string{"WSMON_MODE", "idle"}
	if testing.Short() {
		idle = append(idle, "WSMON_IDLE_LADDER", "4s", "WSMON_MESSAGE_TIMEOUT", "2s")
	}
	cases := []struct {
		name string
		kv   []string
	}{
		{"pingpong", nil},
		{"pingpong+push", []string{"WSMON_PUSH_INTERVAL_MS", "1000"}},
		{"burst", []string{"WSMON_MODE", "burst"}},
		{"idle", idle},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := waitServerIdle(t)
			h := newProbe(t, probeEnv(tc.kv...), false)
			time.Sleep(dur)
			h.Stop()
			after := waitServerIdle(t)
			fin := h.Final()
			noClientServerErrors(t, fin)
			serverErrorDeltas(t, before, after)
			sent := fin.Value("wsmon_client_messages_sent_total", nil)
			echoed := fin.Value("wsmon_client_messages_echoed_total", nil)
			if echoed == 0 || echoed != sent {
				t.Errorf("sent %v, echoed %v", sent, echoed)
			}
			if d := after.Value("wsmon_server_messages_total", L{"type": "probe"}) - before.Value("wsmon_server_messages_total", L{"type": "probe"}); d != sent {
				t.Errorf("server counted %v probes, client sent %v", d, sent)
			}
			if tc.name == "idle" {
				if v := fin.Value("wsmon_client_idle_probe_total", L{"result": "survived"}); v < 1 {
					t.Errorf("no idle rung survived in %v", dur)
				}
			}
			if v := fin.Value("wsmon_client_connect_attempts_total", nil); v != 1 {
				t.Errorf("%v connect attempts, want 1", v)
			}
			t.Logf("sent %v, echoed %v", sent, echoed)
		})
	}
}

// TC-CONTRACT-11: an additive unknown field on every probe is ignored (S11).
func tc11(t *testing.T) {
	extra, ok := testExtra()
	if !ok {
		t.Fatal("built without -tags wsmontest: the extra-field hook is unavailable")
	}
	before := waitServerIdle(t)
	h := newProbe(t, probeEnv("WSMON_PINGPONG_INTERVAL", "100ms"), false, func(o *probe.Options) { o.Extra = extra })
	h.Wait("10 echoes", echoedAtLeast(10), 20*time.Second)
	h.Stop()
	after := waitServerIdle(t)
	fin := h.Final()
	w := decodeWire(t, h.onlyCap())
	for _, m := range w.To[1:] {
		var raw map[string]json.RawMessage
		_ = json.Unmarshal(m.Payload, &raw)
		if string(raw["wsmon_test_unknown"]) != `"x"` {
			t.Fatalf("probe without the extra field: %s", m.Payload)
		}
	}
	if n := len(w.Of(protocol.TypeError)); n != 0 {
		t.Errorf("%d error frames", n)
	}
	if v := fin.Value("wsmon_client_messages_mismatched_total", nil); v != 0 {
		t.Errorf("%v mismatched", v)
	}
	noClientServerErrors(t, fin)
	serverErrorDeltas(t, before, after)
}

// TC-CONTRACT-12: burst of 100 → all echoed, no rate_limited (S12, C6).
func tc12(t *testing.T) {
	before := waitServerIdle(t)
	h := newProbe(t, probeEnv("WSMON_MODE", "burst", "WSMON_BURST_SIZE", "100"), false)
	h.Wait("a small and a large burst", func(s *Scrape) bool {
		return s.HistCount("wsmon_client_burst_completion_seconds", L{"payload": "small"}) >= 1 &&
			s.HistCount("wsmon_client_burst_completion_seconds", L{"payload": "large"}) >= 1
	}, 45*time.Second)
	h.Stop()
	after := waitServerIdle(t)
	fin := h.Final()
	sent, echoed := fin.Value("wsmon_client_messages_sent_total", nil), fin.Value("wsmon_client_messages_echoed_total", nil)
	if sent < 200 || echoed != sent {
		t.Errorf("sent %v, echoed %v; want ≥ 200, all echoed", sent, echoed)
	}
	if v := fin.Value("wsmon_client_messages_lost_total", nil); v != 0 {
		t.Errorf("%v lost", v)
	}
	if v := fin.Value("wsmon_client_server_errors_received_total", L{"code": "rate_limited"}); v != 0 {
		t.Errorf("%v rate_limited errors received", v)
	}
	l := L{"code": "rate_limited"}
	if d := after.Value("wsmon_server_errors_total", l) - before.Value("wsmon_server_errors_total", l); d != 0 {
		t.Errorf("server rate_limited delta %v", d)
	}
	noClientServerErrors(t, fin)
	serverErrorDeltas(t, before, after)
	w := decodeWire(t, h.onlyCap())
	if n := len(w.Of(protocol.TypeEcho)); float64(n) != echoed {
		t.Errorf("%d echoes on the wire, %v counted", n, echoed)
	}
}

// TC-CONTRACT-13: permessage-deflate negotiated; round trips succeed (S13).
func tc13(t *testing.T) {
	h := newProbe(t, probeEnv("WSMON_ENABLE_PERMESSAGE_DEFLATE", "true"), false)
	h.Wait("small and large echoes", func(s *Scrape) bool {
		return s.Value("wsmon_client_messages_echoed_total", L{"payload": "small"}) >= 2 && s.Value("wsmon_client_messages_echoed_total", L{"payload": "large"}) >= 2
	}, 20*time.Second)
	h.Stop()
	fin := h.Final()
	c := h.onlyCap()
	if ext := requestHead(t, c).Header.Get("Sec-WebSocket-Extensions"); !strings.Contains(ext, "permessage-deflate") {
		t.Errorf("client did not offer permessage-deflate: %q", ext)
	}
	ext := responseHead(t, c).Header.Get("Sec-WebSocket-Extensions")
	if !strings.Contains(ext, "permessage-deflate") {
		t.Fatalf("101 Sec-WebSocket-Extensions = %q, want permessage-deflate", ext)
	}
	w := decodeWire(t, c) // inflates RSV1 messages; a bad stream fails here
	// The library compresses messages of at least 512 bytes when there is
	// no context takeover (coder/websocket flateThreshold).
	toZ, fromZ := 0, 0
	for i, m := range w.To {
		if m.Compressed {
			toZ++
		} else if len(m.Payload) >= 512 {
			t.Errorf("client message %d (%d bytes) sent without RSV1", i, len(m.Payload))
		}
		if len(m.Payload) == h.cfg.LargePayloadBytes && m.WireLen >= len(m.Payload) {
			t.Errorf("large probe not compressed: %d wire bytes", m.WireLen)
		}
	}
	for _, m := range w.From {
		if m.Compressed {
			fromZ++
		}
	}
	if toZ == 0 {
		t.Errorf("no client data frame has RSV1 set")
	}
	for i, err := range w.RecvErr {
		if err != nil {
			t.Errorf("server frame %d: %v", i, err)
		}
	}
	if v := fin.Value("wsmon_client_messages_mismatched_total", nil) + fin.Value("wsmon_client_messages_lost_total", nil); v != 0 {
		t.Errorf("mismatched+lost = %v", v)
	}
	t.Logf("negotiated %q; RSV1 on %d/%d client and %d/%d server data messages", ext, toZ, len(w.To), fromZ, len(w.From))
}

// TC-TEL-05: each server ws.echo is a child of the client ws.roundtrip with
// the same trace ID; with ratio 0 the probe sends -00 and no ws.echo
// arrives (C12).
func tel05(t *testing.T, sink *otlpsink.Sink) {
	const n = 20
	base := []string{"WSMON_PINGPONG_INTERVAL", "50ms", "WSMON_OTLP_ENDPOINT", otlpLocal}

	h0 := newProbe(t, probeEnv(append(base, "WSMON_TRACE_SAMPLE_RATIO", "0")...), false)
	h0.Wait(fmt.Sprint(n, " echoes"), echoedAtLeast(n), 30*time.Second)
	h0.Stop()
	unsampled := map[string]bool{}
	for _, p := range decodeWire(t, h0.onlyCap()).Probes {
		tid, sid, sampled, err := protocol.ParseTraceparent(p.Traceparent)
		if err != nil || sampled || tid != p.TraceID || sid != p.SpanID {
			t.Errorf("ratio 0: probe seq %d traceparent %q (sampled=%v, err=%v)", p.Seq, p.Traceparent, sampled, err)
		}
		unsampled[p.TraceID] = true
	}

	h1 := newProbe(t, probeEnv(append(base, "WSMON_TRACE_SAMPLE_RATIO", "1")...), false)
	h1.Wait(fmt.Sprint(n, " echoes"), echoedAtLeast(n), 30*time.Second)
	h1.Stop()
	w := decodeWire(t, h1.onlyCap())
	probes := map[string]protocol.Probe{}
	for _, p := range w.Probes {
		if _, _, sampled, err := protocol.ParseTraceparent(p.Traceparent); err != nil || !sampled {
			t.Errorf("ratio 1: probe seq %d traceparent %q", p.Seq, p.Traceparent)
		}
		probes[p.TraceID] = p
	}
	var echoed []string
	for _, f := range w.Of(protocol.TypeEcho) {
		echoed = append(echoed, f.Echo.TraceID)
	}
	find := func(spans []otlpsink.Span, svc, name, trace string) []otlpsink.Span {
		var out []otlpsink.Span
		for _, s := range spans {
			if s.Service == svc && s.Name == name && s.TraceID == trace {
				out = append(out, s)
			}
		}
		return out
	}
	all := func(spans []otlpsink.Span) bool {
		for _, id := range echoed {
			if len(find(spans, "wsmon-server", "ws.echo", id)) == 0 || len(find(spans, "wsmon-probe", "ws.roundtrip", id)) == 0 {
				return false
			}
		}
		return true
	}
	if !sink.WaitFor(all, 30*time.Second) {
		t.Fatalf("not every echoed probe has both a client ws.roundtrip and a server ws.echo in the sink (%d spans received)", len(sink.Spans()))
	}
	spans := sink.Spans()
	for _, id := range echoed {
		p := probes[id]
		echo := find(spans, "wsmon-server", "ws.echo", id)
		rt := find(spans, "wsmon-probe", "ws.roundtrip", id)
		if len(echo) != 1 || len(rt) != 1 {
			t.Errorf("trace %s: %d ws.echo, %d ws.roundtrip spans, want 1 each", id, len(echo), len(rt))
			continue
		}
		if rt[0].SpanID != p.SpanID || rt[0].ParentSpanID != "" {
			t.Errorf("trace %s: ws.roundtrip span %s parent %q, wire span_id %s", id, rt[0].SpanID, rt[0].ParentSpanID, p.SpanID)
		}
		if echo[0].ParentSpanID != rt[0].SpanID {
			t.Errorf("trace %s: ws.echo parent %s, want ws.roundtrip %s", id, echo[0].ParentSpanID, rt[0].SpanID)
		}
	}
	for _, s := range spans {
		if s.Name == "ws.echo" && unsampled[s.TraceID] {
			t.Errorf("C12: server recorded ws.echo for unsampled trace %s", s.TraceID)
		}
	}
	t.Logf("%d echoed sampled traces linked; %d unsampled probes produced no ws.echo", len(echoed), len(unsampled))
}

// TC-TEL-10: each client ws.push_ack is a child of the server ws.push of the
// same trace.
func tel10(t *testing.T, sink *otlpsink.Sink) {
	h := newProbe(t, probeEnv("WSMON_PUSH_INTERVAL_MS", "200", "WSMON_TRACE_SAMPLE_RATIO", "1", "WSMON_OTLP_ENDPOINT", otlpLocal), false)
	h.Wait("connection up", up, 15*time.Second)
	time.Sleep(2 * time.Second)
	h.Stop()
	w := decodeWire(t, h.onlyCap())
	if len(w.Acks) < 3 {
		t.Fatalf("only %d push_acks", len(w.Acks))
	}
	pushes := map[string]protocol.Push{}
	for _, f := range w.Of(protocol.TypePush) {
		pushes[f.Push.TraceID] = *f.Push
	}
	pick := func(spans []otlpsink.Span, svc, name, trace string) (otlpsink.Span, bool) {
		for _, s := range spans {
			if s.Service == svc && s.Name == name && s.TraceID == trace {
				return s, true
			}
		}
		return otlpsink.Span{}, false
	}
	ok := sink.WaitFor(func(spans []otlpsink.Span) bool {
		for _, a := range w.Acks {
			_, c := pick(spans, "wsmon-probe", "ws.push_ack", a.TraceID)
			_, s := pick(spans, "wsmon-server", "ws.push", a.TraceID)
			if !c || !s {
				return false
			}
		}
		return true
	}, 40*time.Second)
	if !ok {
		t.Fatalf("not every push_ack has a client ws.push_ack and a server ws.push span (%d spans received)", len(sink.Spans()))
	}
	spans := sink.Spans()
	for _, a := range w.Acks {
		ack, _ := pick(spans, "wsmon-probe", "ws.push_ack", a.TraceID)
		push, _ := pick(spans, "wsmon-server", "ws.push", a.TraceID)
		if ack.ParentSpanID != push.SpanID {
			t.Errorf("push_seq %d: ws.push_ack parent %s, want ws.push %s", a.PushSeq, ack.ParentSpanID, push.SpanID)
		}
		if p := pushes[a.TraceID]; p.SpanID != push.SpanID {
			t.Errorf("push_seq %d: frame span_id %s, server ws.push span %s", a.PushSeq, p.SpanID, push.SpanID)
		}
	}
	t.Logf("%d ws.push_ack spans linked to ws.push", len(w.Acks))
}

// TC-CONTRACT-14: the real binary, SIGTERM → the server records close 1000
// (+1) and no 1006 (C11).
func tc14(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "wsmon-probe")
	build := exec.Command("go", "build", "-o", bin, "github.com/asappinc-opensource/wsmon/cmd/wsmon")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	before := waitServerIdle(t)
	var envList []string
	for k, v := range probeEnv() {
		envList = append(envList, k+"="+v)
	}
	out := &syncBuf{}
	cmd := exec.Command(bin, "client")
	cmd.Env, cmd.Stdout, cmd.Stderr = envList, out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	probes := func(s *Scrape) float64 { return s.Value("wsmon_server_messages_total", L{"type": "probe"}) }
	deadline := time.Now().Add(20 * time.Second)
	for probes(serverScrape(t))-probes(before) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("binary made no round trips\n%s", tail(out.String(), 30))
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("exit: %v\n%s", err, tail(out.String(), 20))
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("binary did not exit after SIGTERM\n%s", tail(out.String(), 20))
	}
	after := waitServerIdle(t)
	closes := func(s *Scrape, c string) float64 { return s.Value("wsmon_server_closes_total", L{"close_code": c}) }
	if d := closes(after, "1000") - closes(before, "1000"); d != 1 {
		t.Errorf("server closes_total{1000} delta %v, want 1", d)
	}
	if d := closes(after, "1006") - closes(before, "1006"); d != 0 {
		t.Errorf("server closes_total{1006} delta %v, want 0", d)
	}
	if d := after.Value("wsmon_server_closes_total", nil) - before.Value("wsmon_server_closes_total", nil); d != 1 {
		t.Errorf("server closes_total (all codes) delta %v, want 1", d)
	}
	if t.Failed() {
		t.Logf("binary output:\n%s\nserver logs:\n%s", tail(out.String(), 30), serverLogs(30))
	}
}

// TC-CONTRACT-08: SIGTERM the server mid-run (process or Docker API), then start it
// again. error{shutting_down} then close 1001, never 1006; reconnect within
// BACKOFF_CAP of readiness with a new conn_id and seq from 1 (S10).
func tc08(t *testing.T) {
	h := newProbe(t, probeEnv("WSMON_PINGPONG_INTERVAL", "200ms"), false)
	h.Wait("3 echoes", echoedAtLeast(3), 15*time.Second)
	first := h.Caps()
	if len(first) != 1 {
		t.Fatalf("%d connections before the stop", len(first))
	}
	stopAt := time.Now()
	srv.Stop(t)
	t.Logf("server stopped after %v", time.Since(stopAt).Round(time.Millisecond))
	srv.Start(t)
	var readyAt time.Time
	for deadline := time.Now().Add(60 * time.Second); readyAt.IsZero(); {
		if serverReady() {
			readyAt = time.Now()
		} else if time.Now().After(deadline) {
			t.Fatalf("server not ready 60 s after start\n%s", serverLogs(30))
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
	// The reconnect: the first later connection that received a hello_ack.
	var next *wirecap.Conn
	var ackAt time.Time
	for deadline := time.Now().Add(h.cfg.BackoffCap + 10*time.Second); next == nil; {
		for _, c := range h.Caps()[1:] {
			if !status101(c) {
				continue
			}
			if rf, err := c.ReadFrames(); err == nil {
				for _, f := range rf {
					if !f.IsControl() {
						next, ackAt = c, f.At
						break
					}
				}
			}
			if next != nil {
				break
			}
		}
		if next == nil {
			if time.Now().After(deadline) {
				t.Fatalf("no reconnect within %v of readiness", time.Since(readyAt))
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	delay := ackAt.Sub(readyAt)
	t.Logf("server ready %v after the stop began; hello_ack on the new connection %v after readiness", readyAt.Sub(stopAt).Round(time.Millisecond), delay.Round(time.Millisecond))
	if delay > h.cfg.BackoffCap {
		t.Errorf("reconnected %v after the server became ready, want ≤ BACKOFF_CAP %v", delay, h.cfg.BackoffCap)
	}
	h.Wait("echoes on the new connection", echoedAtLeast(h.Scrape().Value("wsmon_client_messages_echoed_total", nil)+2), 15*time.Second)
	h.Stop()
	fin := h.Final()

	if v := fin.Value("wsmon_client_closes_total", L{"close_code": "1001", "initiator": "remote"}); v != 1 {
		t.Errorf("wsmon_client_closes_total{close_code=1001,initiator=remote} = %v, want 1", v)
	}
	if v := fin.Value("wsmon_client_closes_total", L{"close_code": "1006"}); v != 0 {
		t.Errorf("wsmon_client_closes_total{close_code=1006} = %v, want 0", v)
	}
	if v := fin.Value("wsmon_client_server_errors_received_total", L{"code": "shutting_down"}); v != 1 {
		t.Errorf("wsmon_client_server_errors_received_total{code=shutting_down} = %v, want 1", v)
	}
	w1 := decodeWire(t, first[0])
	errIdx := -1
	for i, f := range w1.Recv {
		if w1.RecvErr[i] == nil && f.Type == protocol.TypeError && f.Error.Code == protocol.ErrShuttingDown {
			errIdx = w1.From[i].Index
		}
	}
	closeIdx, closeCode := -1, -1
	for i, f := range w1.FromFrames {
		if f.Opcode == wirecap.OpClose {
			closeIdx, closeCode = i, f.CloseCode()
			break
		}
	}
	switch {
	case errIdx < 0:
		t.Errorf("S10: no error{shutting_down} received")
	case closeIdx < 0:
		t.Errorf("S10: no close frame received (that is a 1006)")
	case closeIdx < errIdx:
		t.Errorf("S10: close frame (frame %d) before error{shutting_down} (frame %d)", closeIdx, errIdx)
	case closeCode != protocol.CloseGoingAway:
		t.Errorf("S10: close code %d, want 1001", closeCode)
	}
	w2 := decodeWire(t, next)
	if w2.Hello == nil || w2.Hello.ConnID == w1.Hello.ConnID {
		t.Errorf("reconnect reused conn_id %s", w1.Hello.ConnID)
	}
	if len(w2.Probes) == 0 || w2.Probes[0].Seq != 1 {
		t.Errorf("seq on the new connection does not restart at 1: %+v", w2.Probes)
	}
	var reasons []string
	for r, v := range fin.ByLabel("wsmon_client_connect_failures_total", "reason") {
		if v > 0 {
			reasons = append(reasons, fmt.Sprintf("%s=%v", r, v))
		}
	}
	sort.Strings(reasons)
	t.Logf("connect failures during the outage: %v", reasons)
}

// TC-CONTRACT-01: every server → client frame captured during the suite
// decodes into the protocol types with no missing required field and no
// unknown type. Frames are written to testdata/captured/ for review.
func tc01(t *testing.T) {
	capMu.Lock()
	all := append([]capture(nil), captures...)
	capMu.Unlock()
	dir := filepath.Join("..", "..", "..", "internal", "protocol", "testdata", "captured")
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	const perKind = 3 // files written per (tc, type); every frame is decoded
	written := map[string]int{}
	byType := map[string]int{}
	total := 0
	for ci, c := range all {
		if !status101(c.cap) {
			continue
		}
		_, from, err := c.cap.Frames()
		if err != nil {
			t.Errorf("%s conn %d: %v", c.tc, ci, err)
			continue
		}
		msgs, err := messages(from)
		if err != nil {
			t.Errorf("%s conn %d: %v", c.tc, ci, err)
			continue
		}
		for _, m := range msgs {
			total++
			if m.Opcode != wirecap.OpText {
				t.Errorf("%s: server sent a data frame with opcode 0x%x", c.tc, m.Opcode)
				continue
			}
			f, err := protocol.Decode(m.Payload)
			var mf *protocol.MissingFieldError
			var ut *protocol.UnknownTypeError
			switch {
			case errors.As(err, &mf):
				t.Errorf("%s: missing required field: %v\n%s", c.tc, err, m.Payload)
				continue
			case errors.As(err, &ut):
				t.Errorf("%s: unknown frame type: %v\n%s", c.tc, err, m.Payload)
				continue
			case err != nil:
				t.Errorf("%s: %v\n%s", c.tc, err, m.Payload)
				continue
			}
			byType[f.Type]++
			kind := c.tc + "-" + f.Type
			if f.Type == protocol.TypeError {
				kind += "_" + string(f.Error.Code)
			}
			if written[kind] < perKind {
				written[kind]++
				name := fmt.Sprintf("%s-%d.json", kind, written[kind])
				if err := os.WriteFile(filepath.Join(dir, name), m.Payload, 0o644); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	for _, typ := range []string{protocol.TypeHelloAck, protocol.TypeEcho, protocol.TypePush, protocol.TypeError} {
		if byType[typ] == 0 {
			t.Errorf("no %s frame captured during the suite", typ)
		}
	}
	t.Logf("decoded %d server frames from %d connections: %v; wrote %d files to %s", total, len(all), byType, len(written), dir)
}
