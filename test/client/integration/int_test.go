package integration

import (
	"net"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/test/client/fakeserver"
	"github.com/asappinc-opensource/wsmon/test/client/wirecap"
)

// TC-INT-01: pingpong for 30 s (3 s under -short): echoed == sent,
// connection up, payloads alternate small/large.
func TestTCINT01_Pingpong(t *testing.T) {
	parallelLong(t)
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	h := newHarness(t, probeEnv(srv.URL, "WSMON_PINGPONG_INTERVAL", D(time.Second, 100*time.Millisecond).String()))
	h.WaitCount("echoed{small}", 1, 5*time.Second)
	deadline := time.Now().Add(D(30*time.Second, 3*time.Second))
	for time.Now().Before(deadline) {
		if h.rec.Gauge("connection_up") != 1 {
			t.Fatalf("connection_up = 0 mid-run\n%s", h.rec.Dump())
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.Stop()
	sent := h.Count("sent{small}") + h.Count("sent{large}")
	echoed := h.Count("echoed{small}") + h.Count("echoed{large}")
	if sent < float64(D(25, 15)) || echoed != sent || h.rec.SumPrefix("lost") != 0 {
		t.Errorf("sent %v echoed %v\n%s", sent, echoed, h.rec.Dump())
	}
	for i, p := range srv.Upgraded()[0].Probes() {
		large := p.Padding != ""
		if large != (i%2 == 1) {
			t.Fatalf("probe %d (seq %d) large=%v: payloads do not alternate", i, p.Seq, large)
		}
	}
	if len(srv.Upgraded()) != 1 || h.Count("closes{1000,client}") != 1 {
		t.Errorf("expected one connection closed 1000 by the client\n%s", h.rec.Dump())
	}
}

// TC-INT-02: burst of 20: all echoed; completion observed once per burst.
func TestTCINT02_Burst(t *testing.T) {
	parallelLong(t)
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	h := newHarness(t, probeEnv(srv.URL, "WSMON_MODE", "burst", "WSMON_BURST_SIZE", "20",
		"WSMON_BURST_INTERVAL", D(10*time.Second, 300*time.Millisecond).String()))
	h.Wait("3 bursts", func() bool {
		return len(h.rec.Obs("burst_completion{small}"))+len(h.rec.Obs("burst_completion{large}")) >= 3
	}, D(40*time.Second, 10*time.Second))
	h.Stop()
	sent := h.Count("sent{small}") + h.Count("sent{large}")
	echoed := h.Count("echoed{small}") + h.Count("echoed{large}")
	bursts := len(h.rec.Obs("burst_completion{small}")) + len(h.rec.Obs("burst_completion{large}"))
	if echoed != sent || int(sent)%20 != 0 || bursts != int(sent)/20 {
		t.Errorf("sent %v echoed %v bursts %d\n%s", sent, echoed, bursts, h.rec.Dump())
	}
	if len(h.rec.Obs("burst_completion{small}")) == 0 || len(h.rec.Obs("burst_completion{large}")) == 0 {
		t.Error("payload rotation per burst not observed")
	}
	// Every frame of one burst carries the same payload.
	ps := srv.Upgraded()[0].Probes()
	for i := 0; i+20 <= len(ps); i += 20 {
		for j := i; j < i+20; j++ {
			if (ps[j].Padding != "") != (ps[i].Padding != "") {
				t.Fatalf("burst at %d mixes payloads", i)
			}
		}
	}
}

func phaseCounts(h *harness) map[string]int { return h.rec.ObsPrefix("connect_phase") }

// TC-INT-03: wss://localhost with the test CA → each of the five phases
// observed exactly once.
func TestTCINT03_WSSPhases(t *testing.T) {
	ca := fakeserver.NewCA(t)
	srv := fakeserver.NewTLS(t, fakeserver.Behaviour{}, ca, "localhost")
	h := newHarness(t, probeEnv(srv.URL), func(o *probe.Options) { o.RootCAs = ca.Pool })
	h.WaitCount("echoed{small}", 1, 5*time.Second)
	got := phaseCounts(h)
	for _, ph := range probe.Phases {
		if got["connect_phase{"+string(ph)+"}"] != 1 {
			t.Errorf("phases = %v, want each exactly once", got)
			break
		}
	}
	ci := h.tr.Connects()[0]
	if ci.TLSVersion == "" || ci.TLSCipher == "" || ci.PeerIP != "127.0.0.1" || ci.LocalPort == 0 || ci.Failure != "" {
		t.Errorf("connect info %+v", ci)
	}
	// The capture sits above TLS, so frames are readable.
	to, _, err := h.Caps()[0].Frames()
	if err != nil || len(to) == 0 || frameType(to[0]) != protocol.TypeHello {
		t.Errorf("wss capture: %v %v", to, err)
	}
}

// TLS failure (untrusted CA) → reason tls.
func TestTLSFailure(t *testing.T) {
	srv := fakeserver.NewTLS(t, fakeserver.Behaviour{}, fakeserver.NewCA(t), "localhost")
	h := newHarness(t, probeEnv(srv.URL)) // system roots: the test CA is not trusted
	h.WaitCount("connect_failures{tls}", 1, 5*time.Second)
}

// DNS failure → reason dns, and no tcp phase.
func TestDNSFailure(t *testing.T) {
	h := newHarness(t, probeEnv("ws://no-such-host.invalid:8080/__synthetic/ws"))
	h.WaitCount("connect_failures{dns}", 1, 10*time.Second)
	if n := len(h.rec.Obs("connect_phase{tcp}")); n != 0 {
		t.Errorf("tcp phase observed %d times", n)
	}
}

// TC-INT-04: drain → error{shutting_down} logged, close 1001 by remote, no
// 1006, reconnect follows.
func TestTCINT04_Drain(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{OnHello: func(c *fakeserver.Conn) {
		if c.Index == 0 {
			time.Sleep(200 * time.Millisecond)
			c.Drain()
		}
	}})
	h := newHarness(t, probeEnv(srv.URL))
	h.WaitCount("closes{1001,remote}", 1, 5*time.Second)
	h.Wait("reconnected", func() bool { return h.Count("reconnects") >= 1 && h.rec.Gauge("connection_up") == 1 }, 5*time.Second)
	if h.rec.SumPrefix("closes{1006") != 0 || h.Count("server_errors{shutting_down}") != 1 {
		t.Errorf("recorder:\n%s", h.rec.Dump())
	}
	var logged bool
	for _, w := range h.logs.Find("WARN", "error frame") {
		logged = logged || w["code"] == "shutting_down"
	}
	if !logged {
		t.Error("shutting_down not logged at WARN")
	}
}

// TC-INT-05: socket dropped with no close frame → 1006, remote.
func TestTCINT05_Killed(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{OnHello: func(c *fakeserver.Conn) {
		if c.Index == 0 {
			time.Sleep(200 * time.Millisecond)
			c.Kill()
		}
	}})
	h := newHarness(t, probeEnv(srv.URL))
	h.WaitCount("closes{1006,remote}", 1, 5*time.Second)
	h.WaitCount("reconnects", 1, 5*time.Second)
}

// TC-INT-06: server down; 5 retries with jittered, non-identical backoff,
// all ≤ cap.
func TestTCINT06_Backoff(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	const cap = 200 * time.Millisecond
	h := newHarness(t, probeEnv("ws://"+addr+"/__synthetic/ws", "WSMON_BACKOFF_BASE", "50ms", "WSMON_BACKOFF_CAP", cap.String()))
	h.WaitCount("connect_attempts", 6, 10*time.Second)
	h.Stop()
	bs := h.Backoffs()
	if len(bs) < 5 {
		t.Fatalf("backoffs %v", bs)
	}
	distinct := map[time.Duration]bool{}
	for _, b := range bs {
		if b < 0 || b > cap {
			t.Errorf("backoff %v outside [0, %v]", b, cap)
		}
		distinct[b] = true
	}
	if len(distinct) < 2 {
		t.Errorf("backoffs identical: %v", bs)
	}
	if h.Count("connect_failures{tcp}") < 5 || h.Count("reconnects") < 5 {
		t.Errorf("recorder:\n%s", h.rec.Dump())
	}
	// Attempt spacing is the sleep plus a refused connect: ≤ cap + slack.
	var prev time.Time
	for _, e := range h.rec.Events() {
		if e.Name != "connect_attempts" {
			continue
		}
		if !prev.IsZero() && e.At.Sub(prev) > cap+100*time.Millisecond {
			t.Errorf("attempt interval %v > cap", e.At.Sub(prev))
		}
		prev = e.At
	}
}

// TC-INT-07: wrong token → 401 → auth. 403 is auth too.
func TestTCINT07_Auth(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{Token: "right"})
	h := newHarness(t, probeEnv(srv.URL, "WSMON_AUTH_TOKEN", "wrong"))
	h.WaitCount("connect_failures{auth}", 2, 5*time.Second)
	h.Stop()

	srv403 := fakeserver.New(t, fakeserver.Behaviour{HTTPStatus: 403})
	h2 := newHarness(t, probeEnv(srv403.URL))
	h2.WaitCount("connect_failures{auth}", 1, 5*time.Second)
}

// TC-INT-08: 503 (and 502) → http_status.
func TestTCINT08_HTTPStatus(t *testing.T) {
	for _, code := range []int{503, 502} {
		srv := fakeserver.New(t, fakeserver.Behaviour{HTTPStatus: code})
		h := newHarness(t, probeEnv(srv.URL))
		h.WaitCount("connect_failures{http_status}", 2, 5*time.Second)
		h.Stop()
		if h.Count("closes{1006,remote}") != 0 {
			t.Error("non-101 counted as a close")
		}
	}
}

// TC-INT-09: push at 1 s for 10 s (100 ms for 1 s under -short): ≈ 10
// received, each acked, gap ≈ interval.
func TestTCINT09_Push(t *testing.T) {
	parallelLong(t)
	interval := D(time.Second, 100*time.Millisecond)
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	h := newHarness(t, probeEnv(srv.URL, "WSMON_PUSH_INTERVAL_MS", strconv.FormatInt(interval.Milliseconds(), 10),
		"WSMON_PINGPONG_INTERVAL", "1s"))
	h.WaitCount("echoed{small}", 1, 5*time.Second)
	time.Sleep(10 * interval)
	got := h.Count("push_received")
	c := srv.Upgraded()[0]
	h.Wait("every push acked", func() bool { return c.PushAcks() == c.PushesSent() && c.PushAcks() >= int(got) }, 5*time.Second)
	if got < 8 || got > 12 {
		t.Errorf("push_received = %v, want ≈ 10", got)
	}
	gaps := h.rec.Obs("push_gap")
	var sum time.Duration
	for _, g := range gaps {
		sum += g.Value
	}
	if len(gaps) == 0 || (sum/time.Duration(len(gaps))-interval).Abs() > interval/5 {
		t.Errorf("mean gap over %d = %v, want ≈ %v", len(gaps), sum/time.Duration(max(1, len(gaps))), interval)
	}
	if hello := c.Hello(); hello.Options.PushIntervalMs != interval.Milliseconds() || hello.Options.ServerKeepalive {
		t.Errorf("hello options %+v", hello.Options)
	}
	if h.Count("push_lost") != 0 || h.Count("push_out_of_order") != 0 || len(h.logs.Find("WARN", "not requested")) != 0 {
		t.Errorf("recorder:\n%s", h.rec.Dump())
	}
	if acks := h.tr.PushAcks(); len(acks) == 0 || acks[0].Traceparent == "" || acks[0].ConnID != c.ConnID() {
		t.Errorf("tracer push acks %+v", acks)
	}
}

func pushEnv(url string) map[string]string {
	return probeEnv(url, "WSMON_PUSH_INTERVAL_MS", "100", "WSMON_PINGPONG_INTERVAL", "1s")
}

// TC-INT-11: push_seq 4 then 6 → push_lost +1.
func TestTCINT11_PushLost(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{PushSeqs: []int64{1, 2, 3, 4, 6}, PushInterval: 20 * time.Millisecond})
	h := newHarness(t, pushEnv(srv.URL))
	h.WaitCount("push_received", 5, 5*time.Second)
	h.WaitCount("push_lost", 1, 5*time.Second)
	time.Sleep(100 * time.Millisecond) // no further counting may follow
	if h.Count("push_lost") != 1 || h.Count("push_out_of_order") != 0 {
		t.Errorf("recorder:\n%s", h.rec.Dump())
	}
}

// TC-INT-12: pushes 3, 1, 2 → out of order +2 (and lost +2 from the jump to
// 3; counters never decrement).
func TestTCINT12_PushOutOfOrder(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{PushSeqs: []int64{3, 1, 2}, PushInterval: 20 * time.Millisecond})
	h := newHarness(t, pushEnv(srv.URL))
	h.WaitCount("push_received", 3, 5*time.Second)
	h.WaitCount("push_out_of_order", 2, 5*time.Second)
	time.Sleep(100 * time.Millisecond) // no further counting may follow
	if h.Count("push_out_of_order") != 2 || h.Count("push_lost") != 2 {
		t.Errorf("recorder:\n%s", h.rec.Dump())
	}
}

// Unrequested push (here: idle mode) → WARN, still acked (C9).
func TestUnrequestedPush(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{ForcePush: true, PushSeqs: []int64{1, 2}, PushInterval: 20 * time.Millisecond})
	h := newHarness(t, idleEnv(srv.URL))
	h.WaitCount("push_received", 2, 5*time.Second)
	c := srv.Upgraded()[0]
	h.Wait("acked", func() bool { return c.PushAcks() == 2 }, 5*time.Second)
	if len(h.logs.Find("WARN", "not requested")) != 2 {
		t.Error("no WARN for unrequested push")
	}
}

// TC-INT-13: ws:// → tcp, upgrade, hello; no tls and no dns (IP literal);
// no TLS attributes.
func TestTCINT13_WSPhases(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	h := newHarness(t, probeEnv(srv.URL))
	h.WaitCount("echoed{small}", 1, 5*time.Second)
	got := phaseCounts(h)
	want := map[string]int{"connect_phase{tcp}": 1, "connect_phase{upgrade}": 1, "connect_phase{hello}": 1}
	if len(got) != len(want) {
		t.Errorf("phases = %v, want %v", got, want)
	}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("phases = %v, want %v", got, want)
		}
	}
	if ci := h.tr.Connects()[0]; ci.TLSVersion != "" || ci.TLSCipher != "" {
		t.Errorf("TLS attributes on ws://: %+v", ci)
	}
}

// TC-INT-14: forced reconnects every 200 ms never overlap: at most one open
// connection at the client (wire wrapper) and at the server (listener).
func TestTCINT14_OneConnection(t *testing.T) {
	parallelLong(t)
	srv := fakeserver.New(t, fakeserver.Behaviour{OnHello: func(c *fakeserver.Conn) {
		time.Sleep(200 * time.Millisecond)
		c.Kill()
	}})
	h := newHarness(t, probeEnv(srv.URL, "WSMON_BACKOFF_BASE", "1ms", "WSMON_BACKOFF_CAP", "5ms"))
	time.Sleep(D(10*time.Second, 2*time.Second))
	h.Stop()
	if n := len(srv.Upgraded()); n < 5 {
		t.Errorf("only %d connections", n)
	}
	if m := srv.MaxActive(); m > 1 {
		t.Errorf("server saw %d concurrent connections", m)
	}
	// Server-side socket intervals: each TCP connection accepted only after
	// the previous socket was closed.
	cs := srv.Conns()
	for i := 1; i < len(cs); i++ {
		prev, cur := cs[i-1].Cap, cs[i].Cap
		if cl := prev.Closed(); cl.IsZero() || cur.Opened().Before(cl) {
			t.Errorf("conn %d accepted at %v; conn %d socket closed at %v", i, cur.Opened(), i-1, cl)
		}
	}
}

// TC-INT-15: RESOLVE_OVERRIDE + SNI_HOST + HOST_HEADER: dials the pinned IP,
// SNI and Host as configured, no dns phase.
func TestTCINT15_Overrides(t *testing.T) {
	ca := fakeserver.NewCA(t)
	srv := fakeserver.NewTLS(t, fakeserver.Behaviour{}, ca, "probe.test")
	h := newHarness(t, probeEnv(srv.URL,
		"WSMON_RESOLVE_OVERRIDE", "probe.test:127.0.0.1",
		"WSMON_SNI_HOST", "sni.probe.test",
		"WSMON_HOST_HEADER", "host.probe.test"),
		func(o *probe.Options) { o.RootCAs = ca.Pool })
	h.WaitCount("echoed{small}", 1, 5*time.Second)
	if snis := srv.SNIs(); len(snis) == 0 || snis[0] != "sni.probe.test" {
		t.Errorf("SNI %v", snis)
	}
	if c := srv.Upgraded()[0]; c.Host != "host.probe.test" {
		t.Errorf("Host %q", c.Host)
	}
	got := phaseCounts(h)
	if got["connect_phase{dns}"] != 0 || got["connect_phase{tls}"] != 1 || got["connect_phase{tcp}"] != 1 {
		t.Errorf("phases %v", got)
	}
}

// TC-INT-19: WSMON_HEADERS and WSMON_URL_PARAMS reach the upgrade request on
// every (re)connect; a Host item overrides Host; the subprotocol and the
// URL's own query are untouched.
func TestTCINT19_HeadersAndURLParams(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{
		OnHello: func(c *fakeserver.Conn) { time.Sleep(20 * time.Millisecond); c.Kill() },
	})
	h := newHarness(t, probeEnv(srv.URL+"?own=1",
		"WSMON_HEADERS", "X-Synthetic=wsmon,user-agent=wsmon-probe-test,Host=relay.probe.test",
		"WSMON_URL_PARAMS", "src=wsmon,q=a b&c"))
	h.Wait("2 upgraded connections", func() bool { return len(srv.Upgraded()) >= 2 }, 10*time.Second)
	h.Stop()
	for _, c := range srv.Upgraded() {
		if c.Header.Get("X-Synthetic") != "wsmon" || c.Header.Get("User-Agent") != "wsmon-probe-test" {
			t.Errorf("conn %d: headers %v", c.Index, c.Header)
		}
		if c.Host != "relay.probe.test" {
			t.Errorf("conn %d: Host %q", c.Index, c.Host)
		}
		if c.Subprotocols != protocol.Subprotocol {
			t.Errorf("conn %d: subprotocols %q", c.Index, c.Subprotocols)
		}
		q := c.Query
		if q.Get("own") != "1" || q.Get("src") != "wsmon" || q.Get("q") != "a b&c" || len(q) != 3 {
			t.Errorf("conn %d: query %v", c.Index, q)
		}
	}
}

// Every upgrade request carries a traceparent with the IDs and head decision
// of that attempt's ws.connect; WSMON_UPGRADE_TRACEPARENT=false drops it.
func TestUpgradeTraceparent(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{
		OnHello: func(c *fakeserver.Conn) { time.Sleep(20 * time.Millisecond); c.Kill() },
	})
	h := newHarness(t, probeEnv(srv.URL))
	h.Wait("2 upgraded connections", func() bool { return len(srv.Upgraded()) >= 2 }, 10*time.Second)
	h.Stop()
	// An attempt aborted by Stop records no ws.connect, so match from the
	// recorded spans to the upgrades, not the other way round.
	seen := map[string]bool{}
	for _, c := range srv.Upgraded() {
		tp := c.Header.Get("Traceparent")
		if _, _, _, err := protocol.ParseTraceparent(tp); err != nil || seen[tp] {
			t.Errorf("conn %d: traceparent %q invalid or reused", c.Index, tp)
		}
		seen[tp] = true
	}
	cis := h.tr.Connects()
	if len(cis) == 0 {
		t.Fatal("no ws.connect recorded")
	}
	for _, ci := range cis {
		if tp := protocol.FormatTraceparent(ci.TraceID, ci.SpanID, ci.Sampled); !seen[tp] {
			t.Errorf("ws.connect %s not sent on any upgrade (sent %v)", tp, seen)
		}
	}

	srv = fakeserver.New(t, fakeserver.Behaviour{})
	h = newHarness(t, probeEnv(srv.URL, "WSMON_UPGRADE_TRACEPARENT", "false"))
	h.Wait("upgraded connection", func() bool { return len(srv.Upgraded()) >= 1 }, 10*time.Second)
	h.Stop()
	if tp := srv.Upgraded()[0].Header.Get("Traceparent"); tp != "" {
		t.Errorf("traceparent %q sent with WSMON_UPGRADE_TRACEPARENT=false", tp)
	}
}

// execProbe starts the real binary.
type execProbe struct {
	cmd  *exec.Cmd
	out  *syncBuf
	done chan error
}

func startBinary(t *testing.T, env map[string]string) *execProbe {
	t.Helper()
	e := &execProbe{out: &syncBuf{}, done: make(chan error, 1)}
	e.cmd = exec.Command(probeBin, "client")
	e.cmd.Env = envList(env)
	e.cmd.Stdout, e.cmd.Stderr = e.out, e.out
	if err := e.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { e.done <- e.cmd.Wait() }()
	t.Cleanup(func() { _ = e.cmd.Process.Kill() })
	return e
}

func binEnv(url, site string) map[string]string {
	m := probeEnv(url, "WSMON_LABELS", "site="+site)
	m["WSMON_AUTH_TOKEN"] = canary
	return m
}

// TC-INT-10 (process half): two processes with different WSMON_LABELS each
// carry only their own identity (startup config and hello client.*). The
// /metrics half is in TestReassertViaMetrics.
func TestTCINT10_TwoProcesses(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	a := startBinary(t, binEnv(srv.URL, "entry_a"))
	b := startBinary(t, binEnv(srv.URL, "entry_b"))
	deadline := time.Now().Add(10 * time.Second)
	for len(srv.Upgraded()) < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	for _, e := range []*execProbe{a, b} {
		_ = e.cmd.Process.Signal(syscall.SIGTERM)
		<-e.done
	}
	entries := map[string]bool{}
	for _, c := range srv.Upgraded() {
		if h := c.Hello(); h != nil {
			entries[h.Client.Labels["site"]] = true
		}
	}
	if !entries["entry_a"] || !entries["entry_b"] || len(entries) != 2 {
		t.Errorf("hello labels %v", entries)
	}
	for name, e := range map[string]*execProbe{"entry_a": a, "entry_b": b} {
		var cfgLine map[string]any
		for _, l := range e.out.Lines() {
			if l["msg"] == "configuration" {
				cfgLine = l
			}
		}
		if cfgLine == nil || cfgLine["WSMON_LABELS"] != "site="+name {
			t.Errorf("%s: configuration line %v", name, cfgLine)
		}
		if strings.Contains(e.out.String(), canary) {
			t.Errorf("%s: token leaked", name)
		}
	}
}

// TC-INT-17 (probe half): SIGTERM → close 1000 captured at the server, exit
// 0 within SHUTDOWN_TIMEOUT.
func TestTCINT17_SIGTERM(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	e := startBinary(t, binEnv(srv.URL, "pod_direct"))
	c, err := srv.WaitConn(1, 10*time.Second)
	if err != nil {
		t.Fatalf("no connection:\n%s", e.out.String())
	}
	for i := 0; i < 200 && len(c.Probes()) < 2; i++ {
		time.Sleep(20 * time.Millisecond)
	}
	start := time.Now()
	if err := e.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	const shutdownTimeout = 3 * time.Second // probeEnv's WSMON_SHUTDOWN_TIMEOUT
	select {
	case err := <-e.done:
		if err != nil {
			t.Errorf("exit: %v\n%s", err, e.out.String())
		}
		if el := time.Since(start); el > shutdownTimeout {
			t.Errorf("exited after %v > %v", el, shutdownTimeout)
		}
	case <-time.After(shutdownTimeout + 5*time.Second):
		t.Fatalf("did not exit:\n%s", e.out.String())
	}
	<-c.Done()
	_, from, err := c.Cap.Frames()
	if err != nil {
		t.Fatal(err)
	}
	var closes []int
	for _, f := range from {
		if f.Opcode == wirecap.OpClose {
			closes = append(closes, f.CloseCode())
		}
	}
	if len(closes) != 1 || closes[0] != 1000 {
		t.Errorf("close frames from client: %v", closes)
	}
	var shut bool
	for _, l := range e.out.Lines() {
		shut = shut || l["msg"] == "shut down"
	}
	if !shut {
		t.Errorf("no shut down log:\n%s", e.out.String())
	}
}
