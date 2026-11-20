package fault

import (
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
)

// deadAfterFactor mirrors probe.deadAfterFactor:
// DEAD_AFTER = 4 × MESSAGE_TIMEOUT.
const deadAfterFactor = 4

// closeTimeout mirrors probe.closeTimeout.
const closeTimeout = 2 * time.Second

func ws(hostport string) string { return "ws://" + hostport + wsPath }

// rttWindow waits for n round trips per payload in a window and returns the
// wsmon_client_rtt_seconds histogram for each payload over that window.
func rttWindow(h *harness, n float64, timeout time.Duration, payloads ...string) map[string]Hist {
	h.t.Helper()
	before := h.Scrape()
	after := h.Wait(fmt.Sprintf("%v round trips per payload", n), func(s *Scrape) bool {
		for _, p := range payloads {
			if s.Hist("wsmon_client_rtt_seconds", L{"payload": p}).Count-before.Hist("wsmon_client_rtt_seconds", L{"payload": p}).Count < n {
				return false
			}
		}
		return true
	}, timeout)
	out := map[string]Hist{}
	for _, p := range payloads {
		out[p] = after.Hist("wsmon_client_rtt_seconds", L{"payload": p}).Sub(before.Hist("wsmon_client_rtt_seconds", L{"payload": p}))
	}
	out["server_processing"] = after.Hist("wsmon_client_server_processing_seconds", nil).Sub(before.Hist("wsmon_client_server_processing_seconds", nil))
	return out
}

// TC-FAULT-01: 200 ms latency on the server→client direction only.
// wsmon_client_rtt_seconds mean rises by 200 ms ±20 %; the server-processing p50
// bucket is unchanged.
func TestFault01_Latency(t *testing.T) {
	requireEnv(t)
	px, hp := newProxy(t)
	h := newProbe(t, probeEnv(ws(hp), "WSMON_PINGPONG_INTERVAL", "100ms"))
	n := float64(D(40, 15))
	h.waitEchoes(h.Scrape(), 2, 15*time.Second) // connected
	base := rttWindow(h, n, time.Minute, "small", "large")

	addToxic(t, px, "latency_down", "latency", "downstream", toxiproxy.Attributes{"latency": 200, "jitter": 0})
	h.waitEchoes(h.Scrape(), 2, 15*time.Second) // drop the round trip in flight at injection
	fault := rttWindow(h, n, 2*time.Minute, "small", "large")

	for _, p := range []string{"small", "large"} {
		rise := fault[p].Mean() - base[p].Mean()
		t.Logf("%s: baseline %v; with latency %v; mean rise %.1f ms", p, base[p], fault[p], rise*1000)
		if rise < 0.160 || rise > 0.240 {
			t.Errorf("%s: mean RTT rose by %.1f ms, want 200 ms ±20%%", p, rise*1000)
		}
	}
	bp, fp := base["server_processing"].Quantile(0.5), fault["server_processing"].Quantile(0.5)
	t.Logf("server processing: baseline %v; with latency %v", base["server_processing"], fault["server_processing"])
	if bp != fp {
		t.Errorf("wsmon_client_server_processing_seconds p50 bucket changed: %v → %v", bp, fp)
	}
}

// TC-FAULT-02: bandwidth cap 64 kbps (8 KB/s) client→server. large RTT
// degrades by seconds; small is only charged its own serialisation time
// (not queued behind large).
func TestFault02_Bandwidth(t *testing.T) {
	requireEnv(t)
	const rateKBps = 8
	px, hp := newProxy(t)
	h := newProbe(t, probeEnv(ws(hp), "WSMON_PINGPONG_INTERVAL", "100ms"))
	h.waitEchoes(h.Scrape(), 2, 15*time.Second)
	base := rttWindow(h, float64(D(20, 10)), time.Minute, "small", "large")

	addToxic(t, px, "bw_up", "bandwidth", "upstream", toxiproxy.Attributes{"rate": rateKBps})
	h.waitEchoes(h.Scrape(), 2, 30*time.Second)
	fault := rttWindow(h, float64(D(10, 4)), 3*time.Minute, "small", "large")

	t.Logf("small: baseline %v; capped %v", base["small"], fault["small"])
	t.Logf("large: baseline %v; capped %v", base["large"], fault["large"])
	// 32 KiB at 8 KB/s is 4 s; require at least 2.5 s of degradation.
	if rise := fault["large"].Mean() - base["large"].Mean(); rise < 2.5 {
		t.Errorf("large: mean RTT rose by %.2f s, want ≥ 2.5 s (32 KiB at %d KB/s ≈ 4 s)", rise, rateKBps)
	}
	// A small frame (< 1 KiB) costs at most 1024 B / rate on the wire.
	allow := 2*base["small"].Mean() + 1024.0/(rateKBps*1000)
	if m := fault["small"].Mean(); m > allow {
		t.Errorf("small: mean RTT %.1f ms, want ≤ %.1f ms (2× baseline + its own wire time)", m*1000, allow*1000)
	}
	if a := fault["small"].Above(0.25); a != 0 {
		t.Errorf("small: %v round trips above 250 ms; small must not queue behind large", a)
	}
}

// TC-FAULT-03: cut with no close frame (reset_peer) → closes{1006,remote}
// +1, then a reconnect.
func TestFault03_Cut(t *testing.T) {
	requireEnv(t)
	px, hp := newProxy(t)
	h := newProbe(t, probeEnv(ws(hp), "WSMON_PINGPONG_INTERVAL", "100ms", "WSMON_BACKOFF_BASE", "500ms"))
	s0 := h.waitEchoes(h.Scrape(), 3, 15*time.Second)
	c1006 := L{"close_code": "1006", "initiator": "remote"}

	remove := addToxic(t, px, "reset", "reset_peer", "downstream", toxiproxy.Attributes{"timeout": 0})
	s1 := h.Wait("closes{1006,remote} +1", func(s *Scrape) bool {
		return s.Value("wsmon_client_closes_total", c1006) >= s0.Value("wsmon_client_closes_total", c1006)+1
	}, 15*time.Second)
	remove()

	h.Wait("reconnect", func(s *Scrape) bool {
		return s.Value("wsmon_client_reconnects_total", nil) >= s0.Value("wsmon_client_reconnects_total", nil)+1 &&
			s.Value("wsmon_client_connection_up", nil) == 1
	}, 15*time.Second)
	s2 := h.waitEchoes(h.Scrape(), 3, 15*time.Second)

	if d := s2.Value("wsmon_client_closes_total", c1006) - s0.Value("wsmon_client_closes_total", c1006); d != 1 {
		t.Errorf("closes{1006,remote} delta = %v, want 1", d)
	}
	if d := s2.Value("wsmon_client_closes_total", L{"initiator": "client"}) - s0.Value("wsmon_client_closes_total", L{"initiator": "client"}); d != 0 {
		t.Errorf("client-initiated closes delta = %v, want 0", d)
	}
	t.Logf("closes after cut: %v; connect failures during the cut: %v", s1.Value("wsmon_client_closes_total", nil),
		s2.Value("wsmon_client_connect_failures_total", nil)-s0.Value("wsmon_client_connect_failures_total", nil))
	if ids := h.ConnIDs(); len(ids) < 2 || ids[0] == ids[len(ids)-1] {
		t.Errorf("conn_ids %v: want a new conn_id after the reconnect", ids)
	}
}

// TC-FAULT-04: 5 % packet loss with tc netem on the server's interface (in
// the server's network namespace, via the netem sidecar). Expected:
// elevated tail, no crash, the probe keeps running (lost may stay 0).
func TestFault04_PacketLoss(t *testing.T) {
	requireEnv(t)
	if env.Netem == "" {
		t.Skip("WSMON_FAULT_NETEM_CONTAINER not set")
	}
	_, hp := newProxy(t)
	h := newProbe(t, probeEnv(ws(hp), "WSMON_PINGPONG_INTERVAL", "100ms"))
	h.waitEchoes(h.Scrape(), 2, 15*time.Second)
	base := rttWindow(h, 20, time.Minute, "small", "large")
	s0 := h.Scrape()

	if out, rc := dockerExec(t, env.Netem, "tc", "qdisc", "add", "dev", "eth0", "root", "netem", "loss", "5%"); rc != 0 {
		t.Fatalf("tc qdisc add: exit %d: %s", rc, out)
	}
	t.Cleanup(func() {
		if out, rc := dockerExec(t, env.Netem, "tc", "qdisc", "del", "dev", "eth0", "root"); rc != 0 {
			t.Errorf("tc qdisc del: exit %d: %s", rc, out)
		}
	})
	window := D(5*time.Minute, time.Minute)
	time.Sleep(window)
	s1 := h.Scrape()

	all := func(s *Scrape) Hist { return s.Hist("wsmon_client_rtt_seconds", nil) }
	lossy := all(s1).Sub(all(s0))
	lost := s1.Value("wsmon_client_messages_lost_total", nil) - s0.Value("wsmon_client_messages_lost_total", nil)
	t.Logf("baseline small %v, large %v; under 5%% loss for %v: %v; lost %v; reconnects %v", base["small"], base["large"], window,
		lossy, lost, s1.Value("wsmon_client_reconnects_total", nil)-s0.Value("wsmon_client_reconnects_total", nil))

	if lossy.Count == 0 {
		t.Fatalf("no round trips under loss: the probe stopped")
	}
	// Elevated tail: Linux's minimum RTO is 200 ms, so a lost segment shows
	// up above the 0.2 s bucket; the baseline has none there.
	baseTail := base["small"].Above(0.2) + base["large"].Above(0.2)
	if lossy.Above(0.2) <= baseTail {
		t.Errorf("tail not elevated: %v round trips > 200 ms under loss vs %v in the baseline", lossy.Above(0.2), baseTail)
	}
	// Over TCP, 5% packet loss surfaces as
	// retransmission latency, not lost messages; lost may stay 0. Logged only.
	t.Logf("wsmon_client_messages_lost_total delta = %v (informational; MESSAGE_TIMEOUT %v)", lost, h.cfg.MessageTimeout)
	if hz, rz := h.Health(); hz != 200 || rz != 200 {
		t.Errorf("health %d/%d, want 200/200", hz, rz)
	}
	// Still running after the loss is removed.
	h.waitEchoes(h.Scrape(), 3, 30*time.Second)
}

// TC-FAULT-05: half-open socket. timeout toxic with timeout=0 in both
// directions: data stops, the TCP connection stays open. The first loss
// is counted within MESSAGE_TIMEOUT; the half-open rule declares the
// connection dead within DEAD_AFTER + MESSAGE_TIMEOUT of the last inbound
// frame; the client closes 1000 and reconnects with a new conn_id.
func TestFault05_HalfOpen(t *testing.T) {
	requireEnv(t)
	mt := D(10*time.Second, 2*time.Second)
	deadAfter := deadAfterFactor * mt
	px, hp := newProxy(t)
	h := newProbe(t, probeEnv(ws(hp), "WSMON_MESSAGE_TIMEOUT", mt.String()))
	s0 := h.waitEchoes(h.Scrape(), 3, 15*time.Second)

	rmUp := addToxic(t, px, "blackhole_up", "timeout", "upstream", toxiproxy.Attributes{"timeout": 0})
	rmDown := addToxic(t, px, "blackhole_down", "timeout", "downstream", toxiproxy.Attributes{"timeout": 0})
	injected := time.Now()

	h.Wait("first loss", func(s *Scrape) bool {
		return s.Value("wsmon_client_messages_lost_total", nil) > s0.Value("wsmon_client_messages_lost_total", nil)
	}, h.cfg.PingpongInterval+mt+mt/4+time.Second)
	firstLoss := time.Since(injected)

	h.Wait("client close 1000", func(s *Scrape) bool {
		return s.Value("wsmon_client_closes_total", L{"close_code": "1000", "initiator": "client"}) >
			s0.Value("wsmon_client_closes_total", L{"close_code": "1000", "initiator": "client"})
	}, deadAfter+mt+closeTimeout+5*time.Second)
	closed := time.Since(injected)
	// The fault is a property of the dead connection only: lift it so the
	// reconnect can complete.
	rmUp()
	rmDown()

	s1 := h.Wait("reconnected", func(s *Scrape) bool {
		return s.Value("wsmon_client_reconnects_total", nil) > s0.Value("wsmon_client_reconnects_total", nil) &&
			s.Value("wsmon_client_connection_up", nil) == 1
	}, h.cfg.BackoffCap+h.cfg.ConnectTimeout+5*time.Second)
	h.waitEchoes(s1, 2, 15*time.Second)

	warns := h.logs.Find("WARN", "presumed half-open")
	if len(warns) != 1 {
		t.Fatalf("want one half-open WARN, got %d", len(warns))
	}
	detected := logTime(t, warns[0]).Sub(injected)
	t.Logf("first loss %v, half-open detected %v, close recorded %v after injection (MESSAGE_TIMEOUT %v, DEAD_AFTER %v)",
		firstLoss, detected, closed, mt, deadAfter)
	if detected > deadAfter+mt {
		t.Errorf("half-open detected %v after injection, want ≤ DEAD_AFTER + MESSAGE_TIMEOUT = %v", detected, deadAfter+mt)
	}
	if detected < deadAfter-h.cfg.PingpongInterval {
		t.Errorf("half-open detected %v after injection, before DEAD_AFTER %v", detected, deadAfter)
	}
	if ids := h.ConnIDs(); len(ids) != 2 || ids[0] == ids[1] {
		t.Errorf("conn_ids %v: want two distinct", ids)
	}
	if d := s1.Value("wsmon_client_closes_total", L{"close_code": "1006"}) - s0.Value("wsmon_client_closes_total", L{"close_code": "1006"}); d != 0 {
		t.Errorf("closes{1006} delta = %v, want 0 (the client closes with a frame)", d)
	}
}

// TC-FAULT-06: a test-owned HTTP listener answers the upgrade with 502.
// reason="http_status" rises; successive attempts back off (growing
// envelope, jittered).
func TestFault06_Upgrade502(t *testing.T) {
	requireEnv(t)
	var mu sync.Mutex
	var at []time.Time
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		at = append(at, time.Now())
		mu.Unlock()
		http.Error(w, "bad gateway", http.StatusBadGateway)
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	base, bcap := 50*time.Millisecond, 1600*time.Millisecond
	h := newProbe(t, probeEnv(ws(ln.Addr().String()), "WSMON_BACKOFF_BASE", base.String(), "WSMON_BACKOFF_CAP", bcap.String()))
	const attempts = 12
	s := h.Wait("12 http_status failures", func(s *Scrape) bool {
		return s.Value("wsmon_client_connect_failures_total", L{"reason": "http_status"}) >= attempts
	}, 60*time.Second)
	h.Stop()

	if other := s.Value("wsmon_client_connect_failures_total", nil) - s.Value("wsmon_client_connect_failures_total", L{"reason": "http_status"}); other != 0 {
		t.Errorf("%v failures with another reason", other)
	}
	mu.Lock()
	times := append([]time.Time(nil), at...)
	mu.Unlock()
	var iv []time.Duration
	for i := 1; i < len(times); i++ {
		iv = append(iv, times[i].Sub(times[i-1]))
	}
	t.Logf("attempt intervals: %v", iv)
	if len(iv) < attempts-1 {
		t.Fatalf("only %d attempts reached the listener", len(times))
	}
	// Interval k follows failure k: sleep = rand(0, min(cap, base·2^k)).
	const slack = 100 * time.Millisecond
	distinct := map[time.Duration]bool{}
	for k, d := range iv {
		envelope := min(bcap, base<<k)
		if d > envelope+slack {
			t.Errorf("interval %d = %v exceeds the backoff envelope %v", k, d, envelope)
		}
		distinct[d.Round(time.Millisecond)] = true
	}
	early, late := maxDur(iv[:3]), maxDur(iv[len(iv)-5:])
	if late <= early || late < 2*base<<2 {
		t.Errorf("backoff does not grow: max of the first 3 intervals %v, of the last 5 %v", early, late)
	}
	if len(distinct) < len(iv)/2 {
		t.Errorf("intervals look unjittered: %v", iv)
	}
}

func maxDur(ds []time.Duration) time.Duration {
	var m time.Duration
	for _, d := range ds {
		m = max(m, d)
	}
	return m
}

// TC-FAULT-07: wss:// to a proxy that swallows everything: TCP connects,
// the TLS ClientHello goes unanswered. reason="timeout" after
// CONNECT_TIMEOUT ±10 %.
func TestFault07_TLSHang(t *testing.T) {
	requireEnv(t)
	// CONNECT_TIMEOUT must stay under the real server's 10 s
	// ReadHeaderTimeout: the toxic starves the server of bytes, so after 10 s
	// it closes the idle TCP connection, the proxy forwards the close, and the
	// client correctly reports reason="tls" (EOF mid-handshake) instead.
	ct := D(5*time.Second, 3*time.Second)
	px, hp := newProxy(t)
	addToxic(t, px, "blackhole_up", "timeout", "upstream", toxiproxy.Attributes{"timeout": 0})
	h := newProbe(t, probeEnv("wss://"+hp+wsPath, "WSMON_CONNECT_TIMEOUT", ct.String(),
		"WSMON_BACKOFF_BASE", "10ms", "WSMON_BACKOFF_CAP", "10s"))
	s := h.Wait("reason=timeout", func(s *Scrape) bool {
		return s.Value("wsmon_client_connect_failures_total", L{"reason": "timeout"}) >= 1
	}, ct+10*time.Second)
	if other := s.Value("wsmon_client_connect_failures_total", nil) - s.Value("wsmon_client_connect_failures_total", L{"reason": "timeout"}); other != 0 {
		t.Errorf("%v failures with another reason:\n%s", other, nonZero(s.Raw))
	}
	warns := h.logs.Find("WARN", "connect failed")
	if len(warns) == 0 {
		t.Fatal("no connect-failed WARN")
	}
	// The first attempt starts within BACKOFF_BASE (10 ms) of Run.
	took := logTime(t, warns[0]).Sub(h.started)
	t.Logf("first attempt failed after %v (CONNECT_TIMEOUT %v)", took, ct)
	if math.Abs(float64(took-ct)) > 0.1*float64(ct) {
		t.Errorf("attempt took %v, want CONNECT_TIMEOUT %v ±10%%", took, ct)
	}
	if s.Hist("wsmon_client_connect_duration_seconds", L{"phase": "tls"}).Count != 0 {
		t.Errorf("a tls phase was observed for a handshake that never completed")
	}
}

// TC-FAULT-08: NXDOMAIN (.invalid is guaranteed to fail, RFC 6761).
func TestFault08_DNS(t *testing.T) {
	requireEnv(t)
	h := newProbe(t, probeEnv("ws://nxdomain.wsmon.invalid:8080"+wsPath))
	s := h.Wait("reason=dns", func(s *Scrape) bool {
		return s.Value("wsmon_client_connect_failures_total", L{"reason": "dns"}) >= 2
	}, 30*time.Second)
	if other := s.Value("wsmon_client_connect_failures_total", nil) - s.Value("wsmon_client_connect_failures_total", L{"reason": "dns"}); other != 0 {
		t.Errorf("%v failures with another reason:\n%s", other, nonZero(s.Raw))
	}
}

// coalescer is a test-owned TCP proxy that forwards client→server bytes
// immediately and buffers server→client bytes, flushing them every flush
// interval (toxiproxy has no coalescing toxic).
type coalescer struct {
	ln    net.Listener
	flush time.Duration
	mu    sync.Mutex
	on    bool
}

func newCoalescer(t *testing.T, upstream string, flush time.Duration) *coalescer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c := &coalescer{ln: ln, flush: flush}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			cl, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", upstream)
			if err != nil {
				_ = cl.Close()
				continue
			}
			go func() { _, _ = io.Copy(up, cl); _ = up.Close() }()
			go c.down(up, cl)
		}
	}()
	return c
}

func (c *coalescer) set(on bool) {
	c.mu.Lock()
	c.on = on
	c.mu.Unlock()
}

func (c *coalescer) down(up, cl net.Conn) {
	defer cl.Close()
	var mu sync.Mutex
	var buf []byte
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(c.flush)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				mu.Lock()
				b := buf
				buf = nil
				mu.Unlock()
				if len(b) > 0 {
					if _, err := cl.Write(b); err != nil {
						return
					}
				}
			}
		}
	}()
	defer close(done)
	b := make([]byte, 64<<10)
	for {
		n, err := up.Read(b)
		if n > 0 {
			c.mu.Lock()
			on := c.on
			c.mu.Unlock()
			mu.Lock()
			if on {
				buf = append(buf, b[:n]...)
				mu.Unlock()
			} else {
				pending := buf
				buf = nil
				mu.Unlock()
				if _, werr := cl.Write(append(pending, b[:n]...)); werr != nil {
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}

// TC-FAULT-09: frame coalescing. Burst completion is elevated; pingpong RTT
// rises by no more than one flush interval.
func TestFault09_Coalescing(t *testing.T) {
	requireEnv(t)
	const flush = 200 * time.Millisecond
	n := float64(D(15, 8))

	t.Run("burst", func(t *testing.T) {
		co := newCoalescer(t, env.Upstream, flush)
		h := newProbe(t, probeEnv(ws(co.ln.Addr().String()), "WSMON_MODE", "burst", "WSMON_BURST_INTERVAL", "300ms"))
		window := func() Hist {
			b := h.Scrape()
			a := h.Wait(fmt.Sprintf("%v bursts", n), func(s *Scrape) bool {
				return s.Hist("wsmon_client_burst_completion_seconds", nil).Count >= b.Hist("wsmon_client_burst_completion_seconds", nil).Count+n
			}, 2*time.Minute)
			return a.Hist("wsmon_client_burst_completion_seconds", nil).Sub(b.Hist("wsmon_client_burst_completion_seconds", nil))
		}
		h.Wait("first burst", func(s *Scrape) bool { return s.Hist("wsmon_client_burst_completion_seconds", nil).Count >= 1 }, 30*time.Second)
		base := window()
		co.set(true)
		time.Sleep(time.Second) // let the burst in flight at the switch complete
		fault := window()
		t.Logf("burst completion: baseline %v; coalesced every %v: %v", base, flush, fault)
		if rise := fault.Mean() - base.Mean(); rise < 0.050 {
			t.Errorf("burst completion mean rose by %.1f ms, want ≥ 50 ms (flush every %v)", rise*1000, flush)
		}
		if s := h.Scrape(); s.Value("wsmon_client_messages_lost_total", nil) != 0 {
			t.Errorf("coalescing caused losses")
		}
	})

	t.Run("pingpong", func(t *testing.T) {
		co := newCoalescer(t, env.Upstream, flush)
		h := newProbe(t, probeEnv(ws(co.ln.Addr().String()), "WSMON_PINGPONG_INTERVAL", "100ms"))
		h.waitEchoes(h.Scrape(), 2, 15*time.Second)
		base := rttWindow(h, n, time.Minute, "small", "large")
		co.set(true)
		h.waitEchoes(h.Scrape(), 1, 15*time.Second)
		fault := rttWindow(h, n, 2*time.Minute, "small", "large")
		for _, p := range []string{"small", "large"} {
			t.Logf("%s: baseline %v; coalesced %v", p, base[p], fault[p])
			if rise := fault[p].Mean() - base[p].Mean(); rise > flush.Seconds() {
				t.Errorf("%s: mean RTT rose by %.1f ms, more than one flush interval", p, rise*1000)
			}
			// No round trip beyond baseline + one flush: nothing above 250 ms.
			if a := fault[p].Above(0.25); a != 0 {
				t.Errorf("%s: %v round trips above 250 ms", p, a)
			}
		}
	})
}

// TC-FAULT-10: docker pause the server (30 s; -short 3 × a 2 s
// MESSAGE_TIMEOUT), then unpause. Losses during the stall, late echoes
// after it, normal echoes resume, and no reconnect: same conn_id,
// wsmon_client_reconnects_total delta 0.
func TestFault10_Stall(t *testing.T) {
	requireEnv(t)
	mt := D(10*time.Second, 2*time.Second)
	stall := D(30*time.Second, 3*mt)
	_, hp := newProxy(t)
	h := newProbe(t, probeEnv(ws(hp), "WSMON_MESSAGE_TIMEOUT", mt.String(),
		"WSMON_PINGPONG_INTERVAL", "200ms"))
	s0 := h.waitEchoes(h.Scrape(), 3, 15*time.Second)

	mustDocker(t, http.MethodPost, "/containers/"+env.Container+"/pause", http.StatusNoContent)
	paused := true
	t.Cleanup(func() {
		if paused {
			_, _, _ = docker(http.MethodPost, "/containers/"+env.Container+"/unpause", nil)
		}
	})
	time.Sleep(stall)
	s1 := h.Scrape()
	mustDocker(t, http.MethodPost, "/containers/"+env.Container+"/unpause", http.StatusNoContent)
	paused = false

	s2 := h.Wait("late echoes", func(s *Scrape) bool {
		return s.Value("wsmon_client_messages_late_total", nil) > s0.Value("wsmon_client_messages_late_total", nil)
	}, 15*time.Second)
	s3 := h.waitEchoes(s2, 5, 15*time.Second)
	// Hold past DEAD_AFTER from the start of the stall so a late teardown
	// would show.
	time.Sleep(max(0, deadAfterFactor*mt-stall) + mt)
	s4 := h.Scrape()

	lost := s1.Value("wsmon_client_messages_lost_total", nil) - s0.Value("wsmon_client_messages_lost_total", nil)
	late := s4.Value("wsmon_client_messages_late_total", nil) - s0.Value("wsmon_client_messages_late_total", nil)
	t.Logf("stall %v: lost %v during it, late %v after; echoed after unpause %v",
		stall, lost, late, s4.Value("wsmon_client_messages_echoed_total", nil)-s2.Value("wsmon_client_messages_echoed_total", nil))
	if lost < 1 {
		t.Errorf("wsmon_client_messages_lost_total did not rise during the stall")
	}
	if late < 1 {
		t.Errorf("no late echoes after the stall")
	}
	if d := s4.Value("wsmon_client_reconnects_total", nil) - s0.Value("wsmon_client_reconnects_total", nil); d != 0 {
		t.Errorf("wsmon_client_reconnects_total delta = %v, want 0", d)
	}
	if d := s4.Value("wsmon_client_closes_total", nil) - s0.Value("wsmon_client_closes_total", nil); d != 0 {
		t.Errorf("wsmon_client_closes_total delta = %v, want 0", d)
	}
	if ids := h.ConnIDs(); len(ids) != 1 {
		t.Errorf("conn_ids %v: want the same single connection", ids)
	}
	if w := h.logs.Find("WARN", "presumed half-open"); len(w) != 0 {
		t.Errorf("stall was treated as half-open")
	}
	if s4.Value("wsmon_client_messages_echoed_total", nil) <= s3.Value("wsmon_client_messages_echoed_total", nil) {
		t.Errorf("echoes did not keep flowing after recovery")
	}
}
