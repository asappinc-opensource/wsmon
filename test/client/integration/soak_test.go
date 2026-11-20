//go:build soak

package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/client/probetel"
	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/test/client/fakeserver"
)

// TC-SOAK-02 and TC-SOAK-04, scaled. `make test-soak` sets
// WSMON_SOAK_DURATION (default 5m); staging runs 24 h (SOAK-02, per mode)
// and 1 h (SOAK-04).

// Pass thresholds (TC-SOAK-02), applied to the samples after warm-up.
const (
	soakSample          = 10 * time.Second
	heapSlopeMax        = 2048.0 // bytes/s of HeapInuse after GC (≈ 7 MiB/h)
	goroutineSlopeMax   = 0.01   // goroutines/s (< 1 per 100 s)
	goroutineSpreadMax  = 10     // max − min
	fdSpreadMax         = 4      // max − min open fds (a reconnect in flight)
	windowMax           = 1024
	soakCPUFractionMax  = 0.01 // TC-SOAK-04: CPU time / wall time
	soakSlackPerAttempt = 500 * time.Millisecond
)

func soakDuration(t *testing.T) time.Duration {
	s := os.Getenv("WSMON_SOAK_DURATION")
	if s == "" {
		s = "5m"
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		t.Fatalf("WSMON_SOAK_DURATION: %v", err)
	}
	return d
}

// soakProbe runs one in-process probe with the real telemetry and a
// discarding logger (a log buffer would itself grow).
type soakProbe struct {
	p      *probe.Probe
	tel    *probetel.Telemetry
	base   string
	cancel context.CancelFunc
	done   chan error

	mu       sync.Mutex
	backoffs []backoffAt
}

type backoffAt struct {
	at time.Time
	d  time.Duration
}

func startSoakProbe(t *testing.T, env map[string]string) *soakProbe {
	t.Helper()
	cfg, err := config.LoadClient(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	log := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tel, err := probetel.New(context.Background(), cfg, probetel.Options{Logger: log, Version: "0.0.0-soak"})
	if err != nil {
		t.Fatal(err)
	}
	if err := tel.Start(); err != nil {
		t.Fatal(err)
	}
	sp := &soakProbe{tel: tel, base: "http://" + tel.Addr().String(), done: make(chan error, 1)}
	sp.p = probe.New(cfg, tel.Recorder(), tel.Tracer(), probe.Options{
		Logger: log, Version: "0.0.0-soak",
		OnBackoff: func(d time.Duration) {
			sp.mu.Lock()
			sp.backoffs = append(sp.backoffs, backoffAt{time.Now(), d})
			sp.mu.Unlock()
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	sp.cancel = cancel
	go func() { sp.done <- sp.p.Run(ctx) }()
	return sp
}

func (sp *soakProbe) stop(t *testing.T) {
	sp.tel.BeginShutdown()
	sp.cancel()
	select {
	case <-sp.done:
	case <-time.After(20 * time.Second):
		t.Errorf("probe did not stop")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = sp.tel.Shutdown(ctx)
}

func (sp *soakProbe) health() (int, int) {
	c := &http.Client{Timeout: 2 * time.Second}
	get := func(p string) int {
		resp, err := c.Get(sp.base + p)
		if err != nil {
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	return get("/healthz"), get("/readyz")
}

type soakSampleRow struct {
	t                float64 // seconds since start
	postKill         bool    // the kill sample or the one after it
	heap             uint64
	goroutines, fds  int
	inflight, window int
	echoed           float64
}

func openFDs() int {
	es, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(es)
}

// slope is the least-squares slope of ys over xs.
func slope(xs, ys []float64) float64 {
	n := float64(len(xs))
	var sx, sy, sxx, sxy float64
	for i := range xs {
		sx += xs[i]
		sy += ys[i]
		sxx += xs[i] * xs[i]
		sxy += xs[i] * ys[i]
	}
	den := n*sxx - sx*sx
	if den == 0 {
		return 0
	}
	return (n*sxy - sx*sy) / den
}

// TestSoak02 — TC-SOAK-02: each mode in turn for the soak duration against
// the fake server, which kills the connection periodically (1006) so
// reconnects are exercised. No memory, goroutine or fd growth; in-flight
// map ≤ burst size; window ≤ 1024.
func TestSoak02(t *testing.T) {
	dur := soakDuration(t)
	killEvery := min(max(dur/5, 20*time.Second), 3*time.Minute)
	warmup := max(dur/5, 30*time.Second)
	const burstSize = 20

	modes := []struct {
		mode string
		kv   []string
	}{
		{"pingpong", []string{"WSMON_PINGPONG_INTERVAL", "50ms", "WSMON_PUSH_INTERVAL_MS", "200"}},
		{"burst", []string{"WSMON_BURST_SIZE", fmt.Sprint(burstSize), "WSMON_BURST_INTERVAL", "500ms"}},
		{"idle", []string{"WSMON_IDLE_LADDER", "1s,2s,4s", "WSMON_MESSAGE_TIMEOUT", "500ms"}},
	}
	for _, m := range modes {
		t.Run(m.mode, func(t *testing.T) {
			srv := fakeserver.New(t, fakeserver.Behaviour{NoRecord: true})
			env := probeEnv(srv.URL, append([]string{"WSMON_MODE", m.mode, "WSMON_LOG_LEVEL", "warn"}, m.kv...)...)
			sp := startSoakProbe(t, env)
			defer sp.stop(t)

			start := time.Now()
			nextKill := start.Add(killEvery)
			kills := 0
			var lastKill time.Time
			var rows []soakSampleRow
			peakG := 0
			var peakDump strings.Builder
			var csv strings.Builder
			csv.WriteString("t_s,heap_inuse_bytes,goroutines,fds,inflight,window,echoed\n")
			tick := time.NewTicker(soakSample)
			defer tick.Stop()
			for now := range tick.C {
				if now.After(nextKill) {
					if cs := srv.Upgraded(); len(cs) > 0 {
						cs[len(cs)-1].Kill()
						kills++
						lastKill = now
					}
					nextKill = now.Add(killEvery)
				}
				runtime.GC()
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				inf, win := sp.p.CorrelatorSizes()
				r := soakSampleRow{
					postKill: !lastKill.IsZero() && now.Sub(lastKill) < 2*soakSample,
					t:        now.Sub(start).Seconds(), heap: ms.HeapInuse, goroutines: runtime.NumGoroutine(),
					fds: openFDs(), inflight: inf, window: win,
					echoed: scrapeValue(t, sp.base, "wsmon_client_messages_echoed_total"),
				}
				rows = append(rows, r)
				if now.Sub(start) >= warmup && r.goroutines > peakG {
					// Evidence for a goroutine-stability failure: the stacks
					// at the post-warm-up high-water mark.
					peakG = r.goroutines
					peakDump.Reset()
					fmt.Fprintf(&peakDump, "t=%.0fs goroutines=%d fds=%d\n", r.t, r.goroutines, r.fds)
					_ = pprof.Lookup("goroutine").WriteTo(&peakDump, 1)
				}
				fmt.Fprintf(&csv, "%.0f,%d,%d,%d,%d,%d,%.0f\n", r.t, r.heap, r.goroutines, r.fds, r.inflight, r.window, r.echoed)
				if hz, rz := sp.health(); hz != 200 || rz != 200 {
					t.Errorf("t=%.0fs: health %d/%d, want 200/200", r.t, hz, rz)
				}
				if now.Sub(start) >= dur {
					break
				}
			}
			t.Logf("TC-SOAK-02 %s samples (%v, kill every %v, %d kills, warm-up %v):\n%s", m.mode, dur, killEvery, kills, warmup, csv.String())

			var xs, hxs, heap, gor []float64
			minFD, maxFD, minG, maxG := 1<<30, 0, 1<<30, 0
			for _, r := range rows {
				if r.inflight > burstSize {
					t.Errorf("t=%.0fs: in-flight map %d > burst size %d", r.t, r.inflight, burstSize)
				}
				if r.window > windowMax {
					t.Errorf("t=%.0fs: window %d > %d", r.t, r.window, windowMax)
				}
				if r.t < warmup.Seconds() {
					continue
				}
				// The kill sample and the one after it sit ~1 MB low (GC has
				// just freed the old connection's buffers); with few samples
				// that sawtooth dominates the fit, so they are left out of the
				// heap slope only. Goroutines and fds use every sample.
				if !r.postKill {
					hxs = append(hxs, r.t)
					heap = append(heap, float64(r.heap))
				}
				xs = append(xs, r.t)
				gor = append(gor, float64(r.goroutines))
				minFD, maxFD = min(minFD, r.fds), max(maxFD, r.fds)
				minG, maxG = min(minG, r.goroutines), max(maxG, r.goroutines)
			}
			if len(xs) < 3 {
				t.Fatalf("only %d samples after warm-up; raise WSMON_SOAK_DURATION", len(xs))
			}
			if len(hxs) < 3 {
				t.Fatalf("only %d heap samples after warm-up; raise WSMON_SOAK_DURATION", len(hxs))
			}
			hs, gs := slope(hxs, heap), slope(xs, gor)
			t.Logf("%s: heap slope %.1f B/s (max %.0f), goroutine slope %.4f/s (max %.2f), goroutines %d–%d, fds %d–%d, echoed %.0f",
				m.mode, hs, heapSlopeMax, gs, goroutineSlopeMax, minG, maxG, minFD, maxFD, rows[len(rows)-1].echoed)
			if hs >= heapSlopeMax {
				t.Errorf("heap grows: slope %.1f B/s ≥ %.0f B/s", hs, heapSlopeMax)
			}
			if gs >= goroutineSlopeMax || maxG-minG > goroutineSpreadMax {
				t.Errorf("goroutines not stable: slope %.4f/s, range %d–%d", gs, minG, maxG)
				t.Logf("goroutine dump at the post-warm-up peak:\n%s", peakDump.String())
			}
			if minFD < 0 {
				t.Errorf("cannot read /proc/self/fd")
			} else if maxFD-minFD > fdSpreadMax {
				t.Errorf("fd count not stable: %d–%d", minFD, maxFD)
			}
			if kills == 0 {
				t.Errorf("no reconnect exercised: soak too short for the kill interval %v", killEvery)
			}
			if rows[len(rows)-1].echoed == 0 {
				t.Errorf("no echoes")
			}
		})
	}
}

// TestSoak04 — TC-SOAK-04: target a closed port for the soak duration with
// the default BACKOFF_BASE (1 s) and BACKOFF_CAP (30 s).
func TestSoak04(t *testing.T) {
	dur := soakDuration(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens: every connect is refused

	env := probeEnv("ws://"+addr+"/__synthetic/ws",
		"WSMON_BACKOFF_BASE", "<unset>", "WSMON_BACKOFF_CAP", "<unset>", "WSMON_LOG_LEVEL", "warn")
	cfg, _ := config.LoadClient(func(k string) string { return env[k] })
	base, bcap := cfg.BackoffBase, cfg.BackoffCap

	var ru0, ru1 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru0)
	start := time.Now()
	sp := startSoakProbe(t, env)
	for time.Since(start) < dur {
		time.Sleep(5 * time.Second)
		if hz, rz := sp.health(); hz != 200 || rz != 200 {
			t.Errorf("t=%v: health %d/%d, want 200/200", time.Since(start).Round(time.Second), hz, rz)
		}
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru1)
	wall := time.Since(start)
	attempts := scrapeValue(t, sp.base, "wsmon_client_connect_attempts_total")
	failures := scrapeValue(t, sp.base, "wsmon_client_connect_failures_total")
	sp.stop(t)

	cpu := time.Duration(ru1.Utime.Nano()-ru0.Utime.Nano()) + time.Duration(ru1.Stime.Nano()-ru0.Stime.Nano())
	frac := cpu.Seconds() / wall.Seconds()

	sp.mu.Lock()
	bs := append([]backoffAt(nil), sp.backoffs...)
	sp.mu.Unlock()

	// Attempt count: sleeps are rand(0, min(cap, base·2^k)); once capped,
	// the mean sleep is cap/2, so attempts ≈ dur / (cap/2). Bounds: 0.6× to
	// 1.6× that, plus the ~5 uncapped attempts on the way up.
	expect := dur.Seconds() / (bcap.Seconds() / 2)
	lo, hi := 0.6*expect, 1.6*expect+6
	var sb strings.Builder
	for i, b := range bs {
		fmt.Fprintf(&sb, "%d:%v ", i, b.d.Round(time.Millisecond))
	}
	t.Logf("TC-SOAK-04 over %v: %v attempts (bounds [%.0f, %.0f], expected ≈ %.0f), %v failures; CPU %v = %.3f%% of wall; backoffs: %s",
		wall.Round(time.Second), attempts, lo, hi, expect, failures, cpu, 100*frac, sb.String())
	if attempts < lo || attempts > hi {
		t.Errorf("attempts = %v, want within [%.0f, %.0f]", attempts, lo, hi)
	}
	if failures < attempts-1 {
		t.Errorf("failures %v < attempts %v − 1", failures, attempts)
	}
	// Jitter maths: sleep k is in [0, min(cap, base·2^k)], and the next
	// attempt starts no earlier than that sleep (no busy loop).
	short, capped := 0, 0
	for k, b := range bs {
		envelope := bcap
		if k < 30 {
			envelope = min(bcap, base<<k)
		}
		if b.d < 0 || b.d > envelope {
			t.Errorf("backoff %d = %v outside [0, %v]", k, b.d, envelope)
		}
		if k > 0 {
			gap := b.at.Sub(bs[k-1].at)
			if gap < bs[k-1].d {
				t.Errorf("attempt %d came %v after the previous one, sooner than its %v sleep", k, gap, bs[k-1].d)
			}
			if gap > bs[k-1].d+soakSlackPerAttempt {
				t.Errorf("attempt %d came %v after the previous one; sleep was %v", k, gap, bs[k-1].d)
			}
		}
		if envelope == bcap {
			capped++
			if b.d < time.Second {
				short++
			}
		}
	}
	// P(sleep < 1 s | capped) = 1/30: allow 4× the expectation, at least 3.
	if allow := max(3, 4*capped/int(bcap.Seconds())); short > allow {
		t.Errorf("%d of %d capped sleeps under 1 s; jitter maths allows ≈ %d", short, capped, capped/int(bcap.Seconds()))
	}
	if frac >= soakCPUFractionMax {
		t.Errorf("CPU %v over %v = %.2f%% of wall, want < %.0f%%", cpu, wall, 100*frac, 100*soakCPUFractionMax)
	}
}

// scrapeValue sums every series of a counter in the probe's /metrics.
func scrapeValue(t *testing.T, base, name string) float64 {
	t.Helper()
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	v := 0.0
	for _, l := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(l, name) || (len(l) > len(name) && l[len(name)] != '{' && l[len(name)] != ' ') {
			continue
		}
		var f float64
		if _, err := fmt.Sscan(l[strings.LastIndexByte(l, ' ')+1:], &f); err == nil {
			v += f
		}
	}
	return v
}
