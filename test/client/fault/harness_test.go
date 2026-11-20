package fault

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/client/probetel"
	"github.com/asappinc-opensource/wsmon/internal/config"
)

// faultEnv is what `make test-fault` passes to the runner container.
type faultEnv struct {
	ToxiAPI   string // wsmon-fault-toxiproxy:8474
	ToxiHost  string // wsmon-fault-toxiproxy (proxies listen on it)
	Upstream  string // wsmon-fault-server:8080
	Token     string
	Container string // server container, for docker pause/unpause
	Netem     string // netem sidecar container (TC-FAULT-04); may be empty
}

var (
	env  faultEnv
	toxi *toxiproxy.Client
	port atomic.Int32
)

const wsPath = "/__synthetic/ws"

func TestMain(m *testing.M) {
	env = faultEnv{
		ToxiAPI:   os.Getenv("WSMON_FAULT_TOXIPROXY_API"),
		ToxiHost:  os.Getenv("WSMON_FAULT_TOXIPROXY_HOST"),
		Upstream:  os.Getenv("WSMON_FAULT_UPSTREAM"),
		Token:     os.Getenv("WSMON_CONTRACT_TOKEN"),
		Container: os.Getenv("WSMON_CONTRACT_SERVER_CONTAINER"),
		Netem:     os.Getenv("WSMON_FAULT_NETEM_CONTAINER"),
	}
	if env.ToxiAPI != "" {
		toxi = toxiproxy.NewClient(env.ToxiAPI)
	}
	port.Store(20000)
	os.Exit(m.Run())
}

func requireEnv(t *testing.T) {
	t.Helper()
	if env.ToxiAPI == "" || env.ToxiHost == "" || env.Upstream == "" || env.Token == "" || env.Container == "" {
		t.Skip("WSMON_FAULT_* not set; run `make test-fault`")
	}
}

// D picks the spec duration, or the shortened one under -short
// (`make test-fault SHORT=1`). Assertions are identical in both.
func D(full, short time.Duration) time.Duration {
	if testing.Short() {
		return short
	}
	return full
}

// ---------------------------------------------------------------------------
// Toxiproxy.

// newProxy creates a test-owned proxy listening on a fresh port of the
// toxiproxy container, upstream the real server. It is deleted (with its
// toxics) in t.Cleanup. It returns the proxy and the probe URL scheme://host:port.
func newProxy(t *testing.T) (*toxiproxy.Proxy, string) {
	t.Helper()
	p := int(port.Add(1))
	name := fmt.Sprintf("%s-%d", strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()), p)
	px, err := toxi.CreateProxy(name, fmt.Sprintf("0.0.0.0:%d", p), env.Upstream)
	if err != nil {
		t.Fatalf("toxiproxy CreateProxy: %v", err)
	}
	t.Cleanup(func() {
		if err := px.Delete(); err != nil {
			t.Errorf("toxiproxy Delete: %v", err)
		}
	})
	return px, fmt.Sprintf("%s:%d", env.ToxiHost, p)
}

// addToxic adds a toxic and removes it in t.Cleanup (unless already removed).
func addToxic(t *testing.T, px *toxiproxy.Proxy, name, typ, stream string, attrs toxiproxy.Attributes) func() {
	t.Helper()
	if _, err := px.AddToxic(name, typ, stream, 1.0, attrs); err != nil {
		t.Fatalf("toxiproxy AddToxic %s/%s: %v", typ, stream, err)
	}
	var once sync.Once
	remove := func() {
		once.Do(func() {
			if err := px.RemoveToxic(name); err != nil {
				t.Errorf("toxiproxy RemoveToxic %s: %v", name, err)
			}
		})
	}
	t.Cleanup(remove)
	return remove
}

// ---------------------------------------------------------------------------
// Docker Engine API over the mounted unix socket (pause/unpause, exec, logs).

var dockerHTTP = &http.Client{
	Timeout: 90 * time.Second,
	Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", "/var/run/docker.sock")
	}},
}

func docker(method, path string, body any) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, "http://docker"+path, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := dockerHTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

func mustDocker(t *testing.T, method, path string, want int) {
	t.Helper()
	code, b, err := docker(method, path, nil)
	if err != nil || code != want {
		t.Fatalf("docker %s %s: %d %v %s", method, path, code, err, b)
	}
}

// demux strips the 8-byte Docker stream frame headers.
func demux(b []byte) string {
	var out bytes.Buffer
	for len(b) >= 8 {
		size := int(binary.BigEndian.Uint32(b[4:8]))
		if 8+size > len(b) {
			break
		}
		out.Write(b[8 : 8+size])
		b = b[8+size:]
	}
	return out.String()
}

func serverLogs(n int) string {
	code, b, err := docker(http.MethodGet, fmt.Sprintf("/containers/%s/logs?stdout=1&stderr=1&tail=%d", env.Container, n), nil)
	if err != nil || code != 200 {
		return fmt.Sprintf("(server logs unavailable: %d %v)", code, err)
	}
	return demux(b)
}

// dockerExec runs cmd in container and returns its output and exit code.
func dockerExec(t *testing.T, container string, cmd ...string) (string, int) {
	t.Helper()
	code, b, err := docker(http.MethodPost, "/containers/"+container+"/exec",
		map[string]any{"Cmd": cmd, "AttachStdout": true, "AttachStderr": true})
	if err != nil || code != http.StatusCreated {
		t.Fatalf("docker exec create in %s: %d %v %s", container, code, err, b)
	}
	var created struct{ Id string }
	_ = json.Unmarshal(b, &created)
	code, out, err := docker(http.MethodPost, "/exec/"+created.Id+"/start", map[string]any{"Detach": false, "Tty": false})
	if err != nil || code != http.StatusOK {
		t.Fatalf("docker exec start: %d %v", code, err)
	}
	code, b, err = docker(http.MethodGet, "/exec/"+created.Id+"/json", nil)
	if err != nil || code != http.StatusOK {
		t.Fatalf("docker exec inspect: %d %v", code, err)
	}
	var info struct{ ExitCode int }
	_ = json.Unmarshal(b, &info)
	return demux(out), info.ExitCode
}

// ---------------------------------------------------------------------------
// Prometheus scraping of the probe's own /metrics.

type Scrape struct {
	Raw  string
	Fams map[string]*dto.MetricFamily
}

type L = map[string]string

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

// Value sums counter/gauge values over matching series (0 if absent).
func (s *Scrape) Value(name string, labels L) float64 {
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

// Hist is a merged classic histogram (count, sum, cumulative buckets).
type Hist struct {
	Count   float64
	Sum     float64
	Bounds  []float64
	Buckets []float64 // cumulative counts per bound
}

func (s *Scrape) Hist(name string, labels L) Hist {
	var h Hist
	for _, m := range s.Fams[name].GetMetric() {
		if !match(m, labels) {
			continue
		}
		mh := m.GetHistogram()
		h.Count += float64(mh.GetSampleCount())
		h.Sum += mh.GetSampleSum()
		for i, b := range mh.GetBucket() {
			if len(h.Bounds) <= i {
				h.Bounds = append(h.Bounds, b.GetUpperBound())
				h.Buckets = append(h.Buckets, 0)
			}
			h.Buckets[i] += float64(b.GetCumulativeCount())
		}
	}
	return h
}

// Sub is the histogram of observations made between before and h.
func (h Hist) Sub(before Hist) Hist {
	out := Hist{Count: h.Count - before.Count, Sum: h.Sum - before.Sum, Bounds: h.Bounds}
	for i := range h.Buckets {
		b := 0.0
		if i < len(before.Buckets) {
			b = before.Buckets[i]
		}
		out.Buckets = append(out.Buckets, h.Buckets[i]-b)
	}
	return out
}

func (h Hist) Mean() float64 {
	if h.Count == 0 {
		return math.NaN()
	}
	return h.Sum / h.Count
}

// Quantile returns the upper bound of the bucket holding quantile q (+Inf
// if beyond the last bound).
func (h Hist) Quantile(q float64) float64 {
	for i, c := range h.Buckets {
		if c >= q*h.Count {
			return h.Bounds[i]
		}
	}
	return math.Inf(1)
}

// Above is the number of observations above bound (which must be a bucket
// boundary).
func (h Hist) Above(bound float64) float64 {
	for i, b := range h.Bounds {
		if b == bound {
			return h.Count - h.Buckets[i]
		}
	}
	return math.NaN()
}

func (h Hist) String() string {
	return fmt.Sprintf("count=%v mean=%.4fs p50≤%v p99≤%v", h.Count, h.Mean(), h.Quantile(0.5), h.Quantile(0.99))
}

func nonZero(raw string) string {
	var out []string
	for _, l := range strings.Split(raw, "\n") {
		if l != "" && !strings.HasPrefix(l, "#") && !strings.HasSuffix(l, " 0") && !strings.Contains(l, "_bucket") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
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

// Find returns the JSON log records at level whose msg contains substr.
func (s *syncBuf) Find(level, substr string) []map[string]any {
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(s.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) != nil {
			continue
		}
		if msg, _ := m["msg"].(string); m["level"] == level && strings.Contains(msg, substr) {
			out = append(out, m)
		}
	}
	return out
}

func logTime(t *testing.T, m map[string]any) time.Time {
	t.Helper()
	s, _ := m["time"].(string)
	at, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("log record without a time: %v", m)
	}
	return at
}

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// probeEnv is a valid configuration for url with fast reconnects; the rest
// is the default. kv overrides it ("<unset>" deletes).
func probeEnv(url string, kv ...string) map[string]string {
	m := map[string]string{
		"WSMON_URL":              url,
		"WSMON_AUTH_TOKEN":       env.Token,
		"WSMON_LABELS":           "suite=fault",
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

type harness struct {
	t       *testing.T
	cfg     config.Client
	tel     *probetel.Telemetry
	p       *probe.Probe
	logs    *syncBuf
	base    string
	started time.Time

	cancel context.CancelFunc
	done   chan error
	once   sync.Once

	mu            sync.Mutex
	open, maxOpen int
	backoffs      []time.Duration
}

func newProbe(t *testing.T, e map[string]string) *harness {
	t.Helper()
	cfg, err := config.LoadClient(func(k string) string { return e[k] })
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	h := &harness{t: t, cfg: cfg, logs: &syncBuf{}, done: make(chan error, 1)}
	log := slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if h.tel, err = probetel.New(context.Background(), cfg, probetel.Options{Logger: log, Version: "0.0.0-fault"}); err != nil {
		t.Fatal(err)
	}
	if err := h.tel.Start(); err != nil {
		t.Fatal(err)
	}
	h.base = "http://" + h.tel.Addr().String()
	opts := probe.Options{
		Logger: log, Version: "0.0.0-fault", WrapConn: h.wrap,
		OnBackoff: func(d time.Duration) {
			h.mu.Lock()
			h.backoffs = append(h.backoffs, d)
			h.mu.Unlock()
		},
	}
	h.p = probe.New(cfg, h.tel.Recorder(), h.tel.Tracer(), opts)
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.started = time.Now()
	go func() { h.done <- h.p.Run(ctx) }()
	t.Cleanup(func() {
		h.Stop()
		if t.Failed() {
			t.Logf("client logs:\n%s", tail(h.logs.String(), 40))
			t.Logf("server logs:\n%s", serverLogs(30))
		}
	})
	return h
}

// countConn is a net.Conn that reports its first Close.
type countConn struct {
	net.Conn
	once   sync.Once
	closed func()
}

func (c *countConn) Close() error {
	c.once.Do(c.closed)
	return c.Conn.Close()
}

func (h *harness) wrap(c net.Conn) net.Conn {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.open++
	h.maxOpen = max(h.maxOpen, h.open)
	return &countConn{Conn: c, closed: func() {
		h.mu.Lock()
		h.open--
		h.mu.Unlock()
	}}
}

func (h *harness) Backoffs() []time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]time.Duration(nil), h.backoffs...)
}

// Stop runs the main shutdown order and asserts one connection at a time.
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

// Wait polls the probe's /metrics until cond holds.
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

// ConnIDs returns the conn_id of every "connected" log record, in order.
func (h *harness) ConnIDs() []string {
	var out []string
	for _, m := range h.logs.Find("INFO", "connected") {
		if m["msg"] != "connected" {
			continue
		}
		if id, _ := m["conn_id"].(string); id != "" {
			out = append(out, id)
		}
	}
	return out
}

// Healthy reports /healthz and /readyz status codes.
func (h *harness) Health() (healthz, readyz int) {
	c := &http.Client{Timeout: 2 * time.Second}
	get := func(p string) int {
		resp, err := c.Get(h.base + p)
		if err != nil {
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	return get("/healthz"), get("/readyz")
}

// waitEchoes waits until n more echoes have been delivered than in s.
func (h *harness) waitEchoes(s *Scrape, n float64, timeout time.Duration) *Scrape {
	h.t.Helper()
	base := s.Value("wsmon_client_messages_echoed_total", nil)
	return h.Wait(fmt.Sprintf("%v more echoes", n), func(s *Scrape) bool {
		return s.Value("wsmon_client_messages_echoed_total", nil) >= base+n
	}, timeout)
}
