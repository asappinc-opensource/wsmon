//go:build soak

package integration

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/test/server/wstest"
)

// TC-SOAK-01, scaled for a single machine: N connections at 1 probe/s for a
// duration. Passes when goroutines and heap do not grow and the server's
// handle-time p99 stays under 1 ms. The staging run uses 2000 connections
// for 24 h (WSMON_SOAK_CONNS, WSMON_SOAK_DURATION).
//
// TC-SOAK-03 (rolling restart with zero 1006) needs a cluster and is run in
// staging; TestInt04SIGTERMDrain covers the per-pod behaviour it relies on.
func TestSoak01(t *testing.T) {
	conns := envInt("WSMON_SOAK_CONNS", 2000)
	d, err := time.ParseDuration(envStr("WSMON_SOAK_DURATION", "60s"))
	if err != nil {
		t.Fatal(err)
	}
	e := startEnv(t, nil, false)

	clients := make([]*wstest.Client, conns)
	for i := range clients {
		clients[i], _ = e.connectWith(wstest.DialOptions{NoCapture: true}, wstest.ClientSkyEdge, protocol.Options{})
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures int
	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Spread the connections across the second.
			time.Sleep(time.Duration(i) * time.Second / time.Duration(conns))
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
					// Not ctxT: its per-call t.Cleanup would itself grow the heap.
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					_, _, err := c.RoundTrip(ctx, c.NextProbe(wstest.ClientSkyEdge))
					cancel()
					if err != nil {
						mu.Lock()
						failures++
						mu.Unlock()
						return
					}
				}
			}
		}()
	}

	warm := time.Now().Add(d / 10)
	time.Sleep(time.Until(warm))
	g0, h0 := sample()
	time.Sleep(d - d/10)
	g1, h1 := sample()
	close(stop)
	wg.Wait()

	t.Logf("goroutines %d → %d, heap %d → %d KiB", g0, g1, h0>>10, h1>>10)
	if failures > 0 {
		t.Errorf("%d connections failed a round trip", failures)
	}
	if g1 > g0+conns/100+10 {
		t.Errorf("goroutines grew from %d to %d", g0, g1)
	}
	if h1 > h0*3/2+8<<20 {
		t.Errorf("heap grew from %d to %d bytes", h0, h1)
	}

	p99 := handleP99(t, e)
	t.Logf("wsmon_server_handle_seconds{type=probe} p99 <= %v", p99)
	if p99 > 0.001 {
		t.Errorf("handle p99 bucket %v s exceeds 1 ms", p99)
	}
}

func sample() (int, uint64) {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return runtime.NumGoroutine(), ms.HeapAlloc
}

// handleP99 returns the upper bound of the bucket holding the 99th
// percentile of probe handling time.
func handleP99(t *testing.T, e *env) float64 {
	families, err := e.metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() != "wsmon_server_handle_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if m.GetLabel()[0].GetValue() != protocol.TypeProbe {
				continue
			}
			h := m.GetHistogram()
			target := uint64(float64(h.GetSampleCount()) * 0.99)
			for _, b := range h.GetBucket() {
				if b.GetCumulativeCount() >= target {
					return b.GetUpperBound()
				}
			}
		}
	}
	t.Fatal("no probe handle histogram")
	return 0
}

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}
