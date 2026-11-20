package integration

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/test/client/fakeserver"
)

func smallOnly(url string, kv ...string) map[string]string {
	return probeEnv(url, append([]string{"WSMON_PAYLOADS", "small"}, kv...)...)
}

// TC-CORR-01: echo with an unknown seq → unexpected.
func TestTCCORR01_Unexpected(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{OnProbe: func(c *fakeserver.Conn, p fakeserver.Probe) {
		_ = c.Echo(p)
		if p.Seq == 1 {
			e := c.MakeEcho(p)
			e.Seq = 999999
			_ = c.SendJSON(e)
		}
	}})
	h := newHarness(t, smallOnly(srv.URL))
	h.WaitCount("unexpected", 1, 5*time.Second)
	h.WaitCount("echoed{small}", 3, 5*time.Second)
	if h.Count("unexpected") != 1 || h.Count("duplicate") != 0 {
		t.Errorf("recorder:\n%s", h.rec.Dump())
	}
}

// TC-CORR-02: altered trace_id → mismatched{trace_id}, not echoed.
func TestTCCORR02_Mismatched(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{OnProbe: func(c *fakeserver.Conn, p fakeserver.Probe) {
		e := c.MakeEcho(p)
		if p.Seq == 1 {
			e.TraceID = "ffffffffffffffffffffffffffffffff"
		}
		_ = c.SendJSON(e)
	}})
	h := newHarness(t, smallOnly(srv.URL))
	h.WaitCount("mismatched{trace_id}", 1, 5*time.Second)
	h.WaitCount("echoed{small}", 2, 5*time.Second)
	h.Stop()
	if h.Count("mismatched{trace_id}") != 1 || h.rec.SumPrefix("mismatched") != 1 {
		t.Errorf("recorder:\n%s", h.rec.Dump())
	}
	if sent, echoed := h.Count("sent{small}"), h.Count("echoed{small}"); echoed != sent-1 {
		t.Errorf("sent %v echoed %v: the mismatched echo must not count as echoed", sent, echoed)
	}
	var sawMismatch bool
	for _, rt := range h.tr.RoundTrips() {
		if rt.Seq == 1 && rt.Outcome == probe.OutcomeMismatched {
			sawMismatch = true
		}
	}
	if !sawMismatch {
		t.Error("no mismatched round trip span for seq 1")
	}
}

// TC-CORR-03: the same echo twice → duplicate +1, RTT observed once.
func TestTCCORR03_Duplicate(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{OnProbe: func(c *fakeserver.Conn, p fakeserver.Probe) {
		e := c.MakeEcho(p)
		_ = c.SendJSON(e)
		if p.Seq == 1 {
			_ = c.SendJSON(e)
		}
	}})
	h := newHarness(t, smallOnly(srv.URL))
	h.WaitCount("duplicate", 1, 5*time.Second)
	h.WaitCount("echoed{small}", 3, 5*time.Second)
	h.Stop()
	if got, rtts := h.Count("echoed{small}"), len(h.rec.Obs("rtt{small}")); float64(rtts) != got {
		t.Errorf("echoed %v but %d RTT observations", got, rtts)
	}
	if h.Count("duplicate") != 1 {
		t.Errorf("duplicate = %v", h.Count("duplicate"))
	}
}

// TC-CORR-04: echoes 3, 1, 2 within a burst → out_of_order +2, all
// delivered.
func TestTCCORR04_OutOfOrder(t *testing.T) {
	var mu sync.Mutex
	var held []fakeserver.Probe
	srv := fakeserver.New(t, fakeserver.Behaviour{OnProbe: func(c *fakeserver.Conn, p fakeserver.Probe) {
		mu.Lock()
		defer mu.Unlock()
		held = append(held, p)
		if len(held) == 3 {
			for _, i := range []int{2, 0, 1} {
				_ = c.Echo(held[i])
			}
			held = nil
		}
	}})
	h := newHarness(t, smallOnly(srv.URL, "WSMON_MODE", "burst", "WSMON_BURST_SIZE", "3", "WSMON_BURST_INTERVAL", "10s"))
	h.WaitCount("echoed{small}", 3, 5*time.Second)
	h.Wait("burst resolved", func() bool { return len(h.rec.Obs("burst_completion{small}")) == 1 }, 5*time.Second)
	if h.Count("out_of_order") != 2 || h.Count("lost{small}") != 0 {
		t.Errorf("recorder:\n%s", h.rec.Dump())
	}
	if len(h.rec.Obs("burst_completion{small}")) != 1 {
		t.Errorf("burst completion not observed once")
	}
}

// TC-CORR-05: an echo that never arrives is lost after MESSAGE_TIMEOUT and
// swept; pingpong then moves on.
func TestTCCORR05_Lost(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{OnProbe: func(c *fakeserver.Conn, p fakeserver.Probe) {
		if p.Seq != 1 {
			_ = c.Echo(p)
		}
	}})
	start := time.Now()
	h := newHarness(t, smallOnly(srv.URL, "WSMON_MESSAGE_TIMEOUT", "300ms"))
	h.WaitCount("lost{small}", 1, 5*time.Second)
	h.WaitCount("echoed{small}", 1, 5*time.Second)
	if el := time.Since(start); el < 300*time.Millisecond {
		t.Errorf("lost after %v, before the message timeout", el)
	}
	var swept bool
	for _, rt := range h.tr.RoundTrips() {
		swept = swept || (rt.Seq == 1 && rt.Outcome == probe.OutcomeLost)
	}
	if !swept || h.Count("lost{small}") != 1 {
		t.Errorf("swept=%v\n%s", swept, h.rec.Dump())
	}
}

// TC-CORR-06: an echo after the sweep → late +1, not echoed.
func TestTCCORR06_Late(t *testing.T) {
	const mt = 300 * time.Millisecond
	srv := fakeserver.New(t, fakeserver.Behaviour{OnProbe: func(c *fakeserver.Conn, p fakeserver.Probe) {
		if p.Seq == 1 {
			e := c.MakeEcho(p)
			go func() { time.Sleep(3 * mt); _ = c.SendJSON(e) }()
			return
		}
		_ = c.Echo(p)
	}})
	h := newHarness(t, smallOnly(srv.URL, "WSMON_MESSAGE_TIMEOUT", mt.String()))
	h.WaitCount("late{small}", 1, 5*time.Second)
	h.Stop()
	sent, echoed := h.Count("sent{small}"), h.Count("echoed{small}")
	if h.Count("lost{small}") != 1 || echoed != sent-1 || h.Count("late{small}") != 1 {
		t.Errorf("sent=%v echoed=%v\n%s", sent, echoed, h.rec.Dump())
	}
}

// TC-CORR-07: 10 000 round trips (1 000 under -short); in-flight map drains
// to 0 and the remembered window stays ≤ 1024.
func TestTCCORR07_Bounded(t *testing.T) {
	parallelLong(t)
	n := float64(D(10000, 1000))
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	// 100 per 500 ms is the 200 msg/s cap.
	h := newHarness(t, smallOnly(srv.URL, "WSMON_MODE", "burst", "WSMON_BURST_SIZE", "100", "WSMON_BURST_INTERVAL", "500ms"))
	h.WaitCount("echoed{small}", n, D(90*time.Second, 15*time.Second))
	h.Wait("in-flight map empty", func() bool { in, _ := h.p.CorrelatorSizes(); return in == 0 }, 5*time.Second)
	_, win := h.p.CorrelatorSizes()
	if win > 1024 {
		t.Errorf("remembered window %d > 1024", win)
	}
	if h.Count("lost{small}") != 0 || h.Count("reconnects") != 0 {
		t.Errorf("recorder:\n%s", h.rec.Dump())
	}
}

// TC-CORR-08: the connection drops with 3 in flight → 3 lost; the new
// connection has a new conn_id and seq restarts at 1.
func TestTCCORR08_DropInFlight(t *testing.T) {
	var mu sync.Mutex
	got := map[int]int{}
	srv := fakeserver.New(t, fakeserver.Behaviour{OnProbe: func(c *fakeserver.Conn, p fakeserver.Probe) {
		if c.Index > 0 {
			_ = c.Echo(p)
			return
		}
		mu.Lock()
		got[c.Index]++
		n := got[c.Index]
		mu.Unlock()
		if n == 3 {
			c.Kill()
		}
	}})
	h := newHarness(t, smallOnly(srv.URL, "WSMON_MODE", "burst", "WSMON_BURST_SIZE", "3", "WSMON_BURST_INTERVAL", "10s"))
	h.WaitCount("echoed{small}", 3, 5*time.Second)
	h.Stop()
	if h.Count("lost{small}") != 3 || h.Count("closes{1006,remote}") != 1 {
		t.Errorf("recorder:\n%s", h.rec.Dump())
	}
	cs := srv.Upgraded()
	if len(cs) < 2 || cs[0].ConnID() == cs[1].ConnID() || cs[1].ConnID() == "" {
		t.Fatalf("conn_ids: %v", connIDs(cs))
	}
	if ps := cs[1].Probes(); len(ps) == 0 || ps[0].Seq != 1 {
		t.Errorf("second connection's first seq: %v", ps)
	}
}

func connIDs(cs []*fakeserver.Conn) []string {
	var out []string
	for _, c := range cs {
		out = append(out, strconv.Itoa(c.Index)+":"+c.ConnID())
	}
	return out
}
