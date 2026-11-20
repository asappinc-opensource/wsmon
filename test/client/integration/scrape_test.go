package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
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
)

// Scrape is one parsed /metrics response (classic text format).
type Scrape struct {
	Raw  string
	Fams map[string]*dto.MetricFamily
}

// openMetricsAccept is what a Prometheus server sends to get exemplars.
const openMetricsAccept = "application/openmetrics-text;version=1.0.0"

func fetch(base, accept string) (string, int, error) {
	req, _ := http.NewRequest(http.MethodGet, base+"/metrics", nil)
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), resp.StatusCode, err
}

// scrape GETs base/metrics in the classic text format and parses it with
// expfmt.
func scrape(t testing.TB, base string) *Scrape {
	t.Helper()
	raw, code, err := fetch(base, "")
	if err != nil || code != 200 {
		t.Fatalf("scrape %s: %d %v", base, code, err)
	}
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("parse /metrics: %v\n%s", err, raw)
	}
	return &Scrape{Raw: raw, Fams: fams}
}

// scrapeOM GETs base/metrics as OpenMetrics and returns the raw body.
func scrapeOM(t testing.TB, base string) string {
	t.Helper()
	raw, code, err := fetch(base, openMetricsAccept)
	if err != nil || code != 200 {
		t.Fatalf("scrape %s: %d %v", base, code, err)
	}
	if !strings.HasSuffix(strings.TrimSpace(raw), "# EOF") {
		t.Fatalf("not OpenMetrics:\n%s", raw)
	}
	return raw
}

func labelMap(m *dto.Metric) map[string]string {
	out := map[string]string{}
	for _, lp := range m.GetLabel() {
		out[lp.GetName()] = lp.GetValue()
	}
	return out
}

// match reports whether m carries every label in want.
func match(m *dto.Metric, want map[string]string) bool {
	got := labelMap(m)
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// Families returns the sorted family names.
func (s *Scrape) Families() []string {
	out := make([]string, 0, len(s.Fams))
	for n := range s.Fams {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Value sums counter/gauge values over the series matching labels; -1 if
// the family is absent.
func (s *Scrape) Value(name string, labels map[string]string) float64 {
	mf, ok := s.Fams[name]
	if !ok {
		return -1
	}
	v := 0.0
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

// Counter is Value for a counter.
func (s *Scrape) Counter(name string, labels map[string]string) float64 { return s.Value(name, labels) }

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

// HistBucket sums the cumulative count at upper bound le.
func (s *Scrape) HistBucket(name string, le float64, labels map[string]string) uint64 {
	var n uint64
	for _, m := range s.Fams[name].GetMetric() {
		if !match(m, labels) {
			continue
		}
		for _, b := range m.GetHistogram().GetBucket() {
			if b.GetUpperBound() == le {
				n += b.GetCumulativeCount()
			}
		}
	}
	return n
}

// HistSum sums the sample sum over matching histogram series.
func (s *Scrape) HistSum(name string, labels map[string]string) float64 {
	var v float64
	for _, m := range s.Fams[name].GetMetric() {
		if match(m, labels) {
			v += m.GetHistogram().GetSampleSum()
		}
	}
	return v
}

// sampleLine is one exposition sample line: name{labels} value.
var sampleLine = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})? (\S+)`)

// labelPair parses `name="value"` pairs (values here never contain quotes).
var labelPair = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"`)

// SeriesKeys lists every sample line of the raw text exposition as a
// canonical key, independent of expfmt and of the probe's own code.
func (s *Scrape) SeriesKeys(extra map[string]string) []string {
	var out []string
	for _, line := range strings.Split(s.Raw, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		m := sampleLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		ls := map[string]string{"__name__": m[1]}
		for _, p := range labelPair.FindAllStringSubmatch(m[3], -1) {
			ls[p[1]] = p[2]
		}
		for k, v := range extra {
			ls[k] = v
		}
		out = append(out, seriesKey(ls))
	}
	sort.Strings(out)
	return out
}

func seriesKey(ls map[string]string) string {
	var names []string
	for k := range ls {
		if k != "__name__" {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString(ls["__name__"] + "{")
	for i, k := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k + `="` + ls[k] + `"`)
	}
	b.WriteString("}")
	return b.String()
}

// telHarness runs one probe in process with the real telemetry (registry,
// /metrics, health, tracer). Spans go to an in-memory exporter when
// requested.
type telHarness struct {
	t     *testing.T
	cfg   config.Client
	tel   *probetel.Telemetry
	spans *tracetest.InMemoryExporter // nil unless withSpans
	p     *probe.Probe
	logs  *syncBuf
	base  string // http://127.0.0.1:port

	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

func newTelHarness(t *testing.T, env map[string]string, withSpans bool, tweak ...func(*probe.Options)) *telHarness {
	t.Helper()
	cfg, err := config.LoadClient(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	h := &telHarness{t: t, cfg: cfg, logs: &syncBuf{}, done: make(chan error, 1)}
	log := slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	topts := probetel.Options{Logger: log, Version: "0.0.0-test"}
	if withSpans {
		h.spans = tracetest.NewInMemoryExporter()
		topts.SpanExporter = keepSpans{h.spans}
	}
	h.tel, err = probetel.New(context.Background(), cfg, topts)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.tel.Start(); err != nil {
		t.Fatal(err)
	}
	h.base = "http://" + h.tel.Addr().String()
	opts := probe.Options{Logger: log, Version: "0.0.0-test"}
	for _, f := range tweak {
		f(&opts)
	}
	h.p = probe.New(cfg, h.tel.Recorder(), h.tel.Tracer(), opts)
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { h.done <- h.p.Run(ctx) }()
	t.Cleanup(h.Stop)
	return h
}

// Stop runs the main shutdown order: readyz 503, probe drain + close,
// telemetry shutdown (trace flush, final push, server stop).
func (h *telHarness) Stop() {
	h.once.Do(func() {
		h.tel.BeginShutdown()
		h.cancel()
		select {
		case err := <-h.done:
			if err != nil {
				h.t.Errorf("Run returned %v", err)
			}
		case <-time.After(h.cfg.ShutdownTimeout + 5*time.Second):
			h.t.Errorf("Run did not return\n%s", tail(h.logs.String(), 40))
		}
		ctx, cancel := context.WithTimeout(context.Background(), h.cfg.ShutdownTimeout)
		defer cancel()
		_ = h.tel.Shutdown(ctx)
	})
}

func (h *telHarness) Scrape() *Scrape { return scrape(h.t, h.base) }

// Wait polls /metrics until cond holds.
func (h *telHarness) Wait(what string, cond func(*Scrape) bool, timeout time.Duration) *Scrape {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		s := h.Scrape()
		if cond(s) {
			return s
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out after %v waiting for %s\n/metrics:\n%s\nlogs:\n%s", timeout, what, grepWS(s.Raw), tail(h.logs.String(), 30))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// grepWS keeps the non-zero sample lines, for failure messages.
func grepWS(raw string) string {
	var out []string
	for _, l := range strings.Split(raw, "\n") {
		if l != "" && !strings.HasPrefix(l, "#") && !strings.HasSuffix(l, " 0") && !strings.Contains(l, "_bucket") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

// keepSpans stops InMemoryExporter.Shutdown from discarding the spans, so
// tests can inspect them after the telemetry shutdown flushed everything.
type keepSpans struct{ *tracetest.InMemoryExporter }

func (keepSpans) Shutdown(context.Context) error { return nil }

// L is shorthand for a label set.
type L = map[string]string
