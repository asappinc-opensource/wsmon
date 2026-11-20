package contract

import (
	"bufio"
	"bytes"
	"compress/flate"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/client/probetel"
	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/test/client/wirecap"
)

// contractEnv locates the server under test: set by startLocalServer, or
// read from WSMON_CONTRACT_* when `make test-contract-image` runs the suite
// against a released server image.
type contractEnv struct {
	URL        string // ws://wsmon-contract-server:8080/__synthetic/ws
	MetricsURL string // http://wsmon-contract-server:9090/metrics
	AdminBase  string // http://wsmon-contract-server:9090
	Token      string
	Container  string // server container name, for the Docker API (TC-08)
}

var env contractEnv

// loadEnv reads WSMON_CONTRACT_*; ok is false when they are not set, and the
// suite then runs against a local `wsmon server` (server_ctl_test.go).
func loadEnv() bool {
	env = contractEnv{
		URL:        os.Getenv("WSMON_CONTRACT_URL"),
		MetricsURL: os.Getenv("WSMON_CONTRACT_METRICS_URL"),
		Token:      os.Getenv("WSMON_CONTRACT_TOKEN"),
		Container:  os.Getenv("WSMON_CONTRACT_SERVER_CONTAINER"),
	}
	env.AdminBase = strings.TrimSuffix(env.MetricsURL, "/metrics")
	return env.URL != "" && env.MetricsURL != "" && env.Token != "" && env.Container != ""
}

// D picks the spec duration, or the shortened one under -short
// (`make test-contract SHORT=1`). Assertions are identical in both.
func D(full, short time.Duration) time.Duration {
	if testing.Short() {
		return short
	}
	return full
}

// ---------------------------------------------------------------------------
// Prometheus scraping (client /metrics and the server's :9090).

type Scrape struct {
	Raw  string
	Fams map[string]*dto.MetricFamily
}

func scrapeURL(t testing.TB, url string) *Scrape {
	t.Helper()
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		t.Fatalf("scrape %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("scrape %s: HTTP %d", url, resp.StatusCode)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := parser.TextToMetricFamilies(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("parse %s: %v", url, err)
	}
	return &Scrape{Raw: string(b), Fams: fams}
}

func match(m *dto.Metric, want map[string]string) bool {
	got := map[string]string{}
	for _, lp := range m.GetLabel() {
		got[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// Value sums counter/gauge values over matching series; 0 if the family
// or series is absent (series created on first use).
func (s *Scrape) Value(name string, labels map[string]string) float64 {
	v := 0.0
	mf := s.Fams[name]
	for _, m := range mf.GetMetric() {
		if !match(m, labels) {
			continue
		}
		switch mf.GetType() {
		case dto.MetricType_COUNTER:
			v += m.GetCounter().GetValue()
		case dto.MetricType_GAUGE:
			v += m.GetGauge().GetValue()
		case dto.MetricType_UNTYPED:
			v += m.GetUntyped().GetValue()
		}
	}
	return v
}

// HistCount sums the sample count over matching histogram series.
func (s *Scrape) HistCount(name string, labels map[string]string) uint64 {
	var n uint64
	for _, m := range s.Fams[name].GetMetric() {
		if match(m, labels) {
			n += m.GetHistogram().GetSampleCount()
		}
	}
	return n
}

// ByLabel returns value per value of label, over a counter family.
func (s *Scrape) ByLabel(name, label string) map[string]float64 {
	out := map[string]float64{}
	for _, m := range s.Fams[name].GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == label {
				out[lp.GetValue()] += m.GetCounter().GetValue()
			}
		}
	}
	return out
}

// nonZero keeps the non-zero, non-bucket sample lines (failure messages).
func nonZero(raw string) string {
	var out []string
	for _, l := range strings.Split(raw, "\n") {
		if l != "" && !strings.HasPrefix(l, "#") && !strings.HasSuffix(l, " 0") && !strings.Contains(l, "_bucket") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

type L = map[string]string

func serverScrape(t testing.TB) *Scrape { t.Helper(); return scrapeURL(t, env.MetricsURL) }

// waitServerIdle waits until the server has no open connections, so the
// next "before" snapshot is not raced by a previous test's close.
func waitServerIdle(t testing.TB) *Scrape {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		s := serverScrape(t)
		if s.Value("wsmon_server_connections_active", nil) == 0 {
			return s
		}
		if time.Now().After(deadline) {
			t.Fatalf("server still has %v open connections", s.Value("wsmon_server_connections_active", nil))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// serverReady polls the server's /readyz.
func serverReady() bool {
	c := &http.Client{Timeout: time.Second}
	resp, err := c.Get(env.AdminBase + "/readyz")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}

// ---------------------------------------------------------------------------
// Probe harness: production config.Load, probe.New and the real probetel.

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

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// probeEnv is a valid configuration against the contract server with fast
// reconnects. Everything else is the default. kv overrides it
// ("<unset>" deletes).
func probeEnv(kv ...string) map[string]string {
	m := map[string]string{
		"WSMON_URL":              env.URL,
		"WSMON_AUTH_TOKEN":       env.Token,
		"WSMON_LABELS":           "suite=contract",
		"WSMON_METRICS_ADDR":     "127.0.0.1:0",
		"WSMON_BACKOFF_BASE":     "100ms",
		"WSMON_BACKOFF_CAP":      "2s",
		"WSMON_SHUTDOWN_TIMEOUT": "5s",
		"WSMON_LOG_LEVEL":        "debug",
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

// capture is one client connection's wire capture, tagged with the test
// that made it (TC-CONTRACT-01 decodes all of them).
type capture struct {
	tc  string
	cap *wirecap.Conn
}

var (
	capMu    sync.Mutex
	captures []capture
)

type harness struct {
	t     *testing.T
	cfg   config.Client
	tel   *probetel.Telemetry
	spans *tracetest.InMemoryExporter // nil unless withSpans
	p     *probe.Probe
	logs  *syncBuf
	base  string

	cancel context.CancelFunc
	done   chan error
	once   sync.Once

	mu            sync.Mutex
	caps          []*wirecap.Conn
	open, maxOpen int
}

type keepSpans struct{ *tracetest.InMemoryExporter }

func (keepSpans) Shutdown(context.Context) error { return nil }

// newProbe starts one in-process probe. withSpans routes client spans to an
// in-memory exporter (probetel.Options test hook); otherwise spans go to
// WSMON_OTLP_ENDPOINT if set.
func newProbe(t *testing.T, e map[string]string, withSpans bool, tweak ...func(*probe.Options)) *harness {
	t.Helper()
	cfg, err := config.LoadClient(func(k string) string { return e[k] })
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	h := &harness{t: t, cfg: cfg, logs: &syncBuf{}, done: make(chan error, 1)}
	log := slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	topts := probetel.Options{Logger: log, Version: "0.0.0-contract"}
	if withSpans {
		h.spans = tracetest.NewInMemoryExporter()
		topts.SpanExporter = keepSpans{h.spans}
	}
	if h.tel, err = probetel.New(context.Background(), cfg, topts); err != nil {
		t.Fatal(err)
	}
	if err := h.tel.Start(); err != nil {
		t.Fatal(err)
	}
	h.base = "http://" + h.tel.Addr().String()
	opts := probe.Options{Logger: log, Version: "0.0.0-contract", WrapConn: h.wrap}
	for _, f := range tweak {
		f(&opts)
	}
	h.p = probe.New(cfg, h.tel.Recorder(), h.tel.Tracer(), opts)
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { h.done <- h.p.Run(ctx) }()
	t.Cleanup(func() {
		h.Stop()
		if t.Failed() {
			h.evidence()
		}
	})
	return h
}

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
	capMu.Lock()
	captures = append(captures, capture{tc: tcName(h.t), cap: w})
	capMu.Unlock()
	return w
}

// tcName is the top-level subtest name, e.g. "TC-CONTRACT-04".
func tcName(t *testing.T) string {
	parts := strings.Split(t.Name(), "/")
	if len(parts) > 1 {
		return parts[1]
	}
	return parts[0]
}

func (h *harness) Caps() []*wirecap.Conn {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*wirecap.Conn(nil), h.caps...)
}

// Stop runs the main shutdown order: readyz 503, probe drain + close 1000,
// telemetry shutdown (trace flush). It asserts one connection at a time.
func (h *harness) Stop() {
	h.once.Do(func() {
		h.tel.BeginShutdown()
		h.cancel()
		select {
		case err := <-h.done:
			if err != nil {
				h.t.Errorf("Run returned %v", err)
			}
		case <-time.After(h.cfg.ShutdownTimeout + 5*time.Second):
			h.t.Errorf("Run did not return after cancel")
		}
		ctx, cancel := context.WithTimeout(context.Background(), h.cfg.ShutdownTimeout)
		defer cancel()
		_ = h.tel.Shutdown(ctx)
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.maxOpen > 1 {
			h.t.Errorf("TC-INT-14: client held %d connections open at once", h.maxOpen)
		}
	})
}

func (h *harness) Scrape() *Scrape { return scrapeURL(h.t, h.base+"/metrics") }

// Wait polls the client's /metrics until cond holds.
func (h *harness) Wait(what string, cond func(*Scrape) bool, timeout time.Duration) *Scrape {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		s := h.Scrape()
		if cond(s) {
			return s
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out after %v waiting for %s\nclient /metrics:\n%s", timeout, what, nonZero(s.Raw))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// evidence logs, on failure, what is needed to diagnose it: wire frames,
// both sides' logs, the server's /metrics.
func (h *harness) evidence() {
	t := h.t
	for i, c := range h.Caps() {
		to, from, err := c.Frames()
		t.Logf("conn %d: %d frames to server, %d from server (parse err %v)", i, len(to), len(from), err)
		for j, f := range to {
			if j < 6 || j >= len(to)-3 {
				t.Logf("  → %s", f)
			}
		}
		for j, f := range from {
			if j < 6 || j >= len(from)-3 {
				t.Logf("  ← %s", f)
			}
		}
	}
	t.Logf("client logs:\n%s", tail(h.logs.String(), 40))
	t.Logf("server logs:\n%s", serverLogs(40))
	if s := scrapeURLNoFail(env.MetricsURL); s != "" {
		var keep []string
		for _, l := range strings.Split(nonZero(s), "\n") {
			if strings.HasPrefix(l, "wsmon_server_") {
				keep = append(keep, l)
			}
		}
		t.Logf("server /metrics (wsmon_server_*, non-zero):\n%s", strings.Join(keep, "\n"))
	}
}

func scrapeURLNoFail(url string) string {
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// ---------------------------------------------------------------------------
// Wire decoding.

// message is one reassembled WebSocket data message.
type message struct {
	Opcode     byte
	Compressed bool   // RSV1 on the first frame (permessage-deflate)
	WireLen    int    // payload bytes on the wire (compressed if Compressed)
	Payload    []byte // uncompressed
	Index      int    // index of the message's first frame
}

// messages reassembles data messages (skipping control frames) and inflates
// permessage-deflate ones. The client always requests no context takeover
// in both directions, so each message inflates on its own.
func messages(fs []wirecap.Frame) ([]message, error) {
	var out []message
	var cur *message
	for i, f := range fs {
		if f.IsControl() {
			continue
		}
		if f.Opcode != wirecap.OpContinuation {
			if cur != nil {
				return out, fmt.Errorf("frame %d: new message before FIN of the previous one", i)
			}
			cur = &message{Opcode: f.Opcode, Compressed: f.RSV1, Index: i}
		} else if cur == nil {
			return out, fmt.Errorf("frame %d: continuation without a message", i)
		}
		cur.WireLen += len(f.Payload)
		cur.Payload = append(cur.Payload, f.Payload...)
		if f.Fin {
			if cur.Compressed {
				r := flate.NewReader(io.MultiReader(bytes.NewReader(cur.Payload), bytes.NewReader([]byte{0, 0, 0xff, 0xff})))
				b, err := io.ReadAll(r)
				if err != nil && err != io.ErrUnexpectedEOF {
					return out, fmt.Errorf("frame %d: inflate: %v", i, err)
				}
				cur.Payload = b
			}
			out = append(out, *cur)
			cur = nil
		}
	}
	return out, nil
}

// wire is one connection's decoded traffic.
type wire struct {
	ToFrames, FromFrames []wirecap.Frame
	To, From             []message
	// Typed views.
	Hello  *protocol.Hello
	Probes []protocol.Probe
	Acks   []protocol.PushAck
	Recv   []protocol.Frame
	// RecvErr holds Decode errors of server frames, in order.
	RecvErr []error
}

func decodeWire(t testing.TB, c *wirecap.Conn) *wire {
	t.Helper()
	to, from, err := c.Frames()
	if err != nil {
		t.Fatalf("wirecap: %v", err)
	}
	w := &wire{ToFrames: to, FromFrames: from}
	if w.To, err = messages(to); err != nil {
		t.Fatalf("client → server: %v", err)
	}
	if w.From, err = messages(from); err != nil {
		t.Fatalf("server → client: %v", err)
	}
	for i, m := range w.To {
		var env struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(m.Payload, &env); err != nil {
			t.Fatalf("client sent a non-JSON frame %d: %v", i, err)
		}
		switch env.Type {
		case protocol.TypeHello:
			var h protocol.Hello
			_ = json.Unmarshal(m.Payload, &h)
			if i != 0 {
				t.Errorf("C3: hello is client frame %d, want 0", i)
			}
			w.Hello = &h
		case protocol.TypeProbe:
			var p protocol.Probe
			_ = json.Unmarshal(m.Payload, &p)
			w.Probes = append(w.Probes, p)
		case protocol.TypePushAck:
			var a protocol.PushAck
			_ = json.Unmarshal(m.Payload, &a)
			w.Acks = append(w.Acks, a)
		default:
			t.Errorf("client sent frame type %q", env.Type)
		}
	}
	for _, m := range w.From {
		f, err := protocol.Decode(m.Payload)
		w.Recv = append(w.Recv, f)
		w.RecvErr = append(w.RecvErr, err)
	}
	return w
}

// Of returns the received frames of one type.
func (w *wire) Of(typ string) []protocol.Frame {
	var out []protocol.Frame
	for i, f := range w.Recv {
		if w.RecvErr[i] == nil && f.Type == typ {
			out = append(out, f)
		}
	}
	return out
}

// responseHead parses the upgrade response the client read.
func responseHead(t testing.TB, c *wirecap.Conn) *http.Response {
	t.Helper()
	head := wirecap.Head(c.Reads())
	if head == nil {
		t.Fatalf("no HTTP response captured")
	}
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(head)), nil)
	if err != nil {
		t.Fatalf("parse upgrade response: %v\n%q", err, head)
	}
	return resp
}

// requestHead parses the upgrade request the client wrote.
func requestHead(t testing.TB, c *wirecap.Conn) *http.Request {
	t.Helper()
	head := wirecap.Head(c.Writes())
	req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(head)))
	if err != nil {
		t.Fatalf("parse upgrade request: %v\n%q", err, head)
	}
	return req
}
