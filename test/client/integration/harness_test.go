package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/client/probe/probetest"
	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/test/client/wirecap"
)

// D picks the spec duration under `make test-acceptance` and the shortened
// one under `make test` (-short). Assertions are identical in both.
func D(full, short time.Duration) time.Duration {
	if testing.Short() {
		return short
	}
	return full
}

// parallelLong marks a test parallel only at acceptance durations, so the
// timing-sensitive shortened runs never contend for CPU.
func parallelLong(t *testing.T) {
	if !testing.Short() {
		t.Parallel()
	}
}

// syncBuf is a concurrency-safe log sink.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// Lines returns the JSON log records.
func (s *syncBuf) Lines() []map[string]any {
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(s.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil {
			out = append(out, m)
		}
	}
	return out
}

// Find returns the log records at level whose msg contains substr.
func (s *syncBuf) Find(level, substr string) []map[string]any {
	var out []map[string]any
	for _, m := range s.Lines() {
		if m["level"] == level && strings.Contains(m["msg"].(string), substr) {
			out = append(out, m)
		}
	}
	return out
}

// probeEnv is a valid in-process configuration pointing at url, with fast
// backoff. kv overrides it ("<unset>" deletes).
func probeEnv(url string, kv ...string) map[string]string {
	m := map[string]string{
		"WSMON_MODE":                  "pingpong",
		"WSMON_URL":                   url,
		"WSMON_METRICS_ADDR":          "127.0.0.1:0",
		"WSMON_BACKOFF_BASE":          "20ms",
		"WSMON_BACKOFF_CAP":           "200ms",
		"WSMON_PINGPONG_INTERVAL":     "50ms",
		"WSMON_MESSAGE_TIMEOUT":       "1s",
		"WSMON_CONNECT_TIMEOUT":       "5s",
		"WSMON_SHUTDOWN_TIMEOUT":      "3s",
		"WSMON_CLOCK_OFFSET_INTERVAL": "100ms",
	}
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i+1] == "<unset>" {
			delete(m, kv[i])
		} else {
			m[kv[i]] = kv[i+1]
		}
	}
	return m
}

// harness runs one probe in process against a fake server.
type harness struct {
	t    *testing.T
	cfg  config.Client
	rec  *probetest.Recorder
	tr   *probetest.Tracer
	logs *syncBuf
	p    *probe.Probe

	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	ended  time.Duration // how long Run took to return after cancel

	mu       sync.Mutex
	caps     []*wirecap.Conn
	open     int
	maxOpen  int
	backoffs []time.Duration
}

// newHarness loads env through config.Load (so every test config is a
// valid production config) and starts Run. The probe is stopped in
// t.Cleanup, which also asserts the client never held more than one
// connection open (TC-INT-14, checked in every test).
func newHarness(t *testing.T, env map[string]string, tweak ...func(*probe.Options)) *harness {
	t.Helper()
	cfg, err := config.LoadClient(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	h := &harness{t: t, cfg: cfg, rec: probetest.NewRecorder(), tr: &probetest.Tracer{}, logs: &syncBuf{}, done: make(chan error, 1)}
	opts := probe.Options{
		Logger:   slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Version:  "0.0.0-test",
		WrapConn: h.wrap,
		OnBackoff: func(d time.Duration) {
			h.mu.Lock()
			h.backoffs = append(h.backoffs, d)
			h.mu.Unlock()
		},
	}
	for _, f := range tweak {
		f(&opts)
	}
	h.p = probe.New(cfg, h.rec, h.tr, opts)
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { h.done <- h.p.Run(ctx) }()
	t.Cleanup(h.Stop)
	return h
}

// wrap counts open client connections and captures every byte.
func (h *harness) wrap(c net.Conn) net.Conn {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.open++
	h.maxOpen = max(h.maxOpen, h.open)
	w := wirecap.WrapNotify(c, func() {
		h.mu.Lock()
		h.open--
		h.mu.Unlock()
	})
	h.caps = append(h.caps, w)
	return w
}

// Caps returns the client-side capture of every connection, in order.
func (h *harness) Caps() []*wirecap.Conn {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*wirecap.Conn(nil), h.caps...)
}

func (h *harness) Backoffs() []time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]time.Duration(nil), h.backoffs...)
}

// Stop cancels the probe (graceful shutdown) and waits for Run to return.
func (h *harness) Stop() {
	h.once.Do(func() {
		start := time.Now()
		h.cancel()
		select {
		case err := <-h.done:
			h.ended = time.Since(start)
			if err != nil {
				h.t.Errorf("Run returned %v", err)
			}
		case <-time.After(h.cfg.ShutdownTimeout + 5*time.Second):
			h.t.Errorf("Run did not return after cancel\nlogs:\n%s", h.logs.String())
		}
		h.mu.Lock()
		maxOpen, open := h.maxOpen, h.open
		h.mu.Unlock()
		if maxOpen > 1 {
			h.t.Errorf("TC-INT-14: client held %d connections open at once", maxOpen)
		}
		if open != 0 {
			h.t.Errorf("client left %d connection(s) open after shutdown", open)
		}
	})
}

// Count / Wait helpers.
func (h *harness) Count(k string) float64 { return h.rec.Count(k) }

func (h *harness) WaitCount(k string, n float64, timeout time.Duration) {
	h.t.Helper()
	h.rec.MustWait(h.t, k+" ≥ "+ftoa(n), func() bool { return h.rec.Count(k) >= n }, timeout)
}

func (h *harness) Wait(what string, cond func() bool, timeout time.Duration) {
	h.t.Helper()
	if !h.rec.WaitFor(cond, timeout) {
		h.t.Fatalf("timed out after %v waiting for %s\nrecorder:\n%s\nlogs:\n%s", timeout, what, h.rec.Dump(), tail(h.logs.String(), 40))
	}
}

func ftoa(f float64) string {
	b, _ := json.Marshal(f)
	return string(b)
}

func tail(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// textFrames decodes the JSON "type" of every text frame.
func frameType(f wirecap.Frame) string {
	var env struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(f.Payload, &env)
	return env.Type
}
