// Package integration runs `wsmon server` on a real loopback socket and drives
// it with the minimal fake client in test/wstest.
package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/internal/server"
	"github.com/asappinc-opensource/wsmon/internal/server/servertel"
	"github.com/asappinc-opensource/wsmon/test/server/wstest"
)

var testIdentity = protocol.ServerInfo{Name: "wsmon-server-test-0", Version: "test"}

// testLabels stand in for WSMON_LABELS: placement the deployment knows and
// the server does not. Tests opt in with withTestLabels; by default the
// server has none, so series text stays exact.
var testLabels = map[string]string{"zone": "test-1a"}

func withTestLabels(c *config.Server) { c.Labels = testLabels }

// env is one in-process server listening on loopback.
type env struct {
	t        *testing.T
	cfg      config.Server
	srv      *server.Server
	metrics  *servertel.Metrics
	spans    *tracetest.SpanRecorder
	wsURL    string
	adminURL string
}

// start runs a server with test-friendly defaults, adjusted by mutate. Every
// span is sampled unless the client's traceparent says otherwise.
func start(t *testing.T, mutate func(*config.Server)) *env {
	t.Helper()
	return startEnv(t, mutate, true)
}

// startEnv is start with span recording optional; long runs turn it off so
// the in-memory recorder does not dominate the heap.
func startEnv(t *testing.T, mutate func(*config.Server), recordSpans bool) *env {
	t.Helper()
	cfg, err := config.LoadServer(func(string) string { return "" }) // all defaults
	if err != nil {
		t.Fatal(err)
	}
	cfg.ListenAddr, cfg.MetricsAddr = "127.0.0.1:0", "127.0.0.1:0"
	cfg.DrainDelay = 0
	cfg.EnablePermessageDeflate = false
	if mutate != nil {
		mutate(&cfg)
	}

	rec := tracetest.NewSpanRecorder()
	var tp trace.TracerProvider = noop.NewTracerProvider()
	if recordSpans {
		sdk := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithSampler(servertel.NewSampler(1)))
		t.Cleanup(func() { _ = sdk.Shutdown(context.Background()) })
		tp = sdk
	}

	m := servertel.NewMetrics(testIdentity.Version, testIdentity.Name, cfg.Labels)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := server.New(cfg, log, m, servertel.NewTracingWithProvider(tp, testIdentity, cfg.Labels), testIdentity)

	wsLn := listen(t)
	adminLn := listen(t)
	hs := server.NewHTTPServer(srv.Handler())
	// Shorter than any idle period in the suite, so a header deadline that
	// leaked onto upgraded connections would be caught.
	hs.ReadHeaderTimeout = 500 * time.Millisecond
	admin := &http.Server{Handler: srv.AdminHandler()}
	go func() { _ = hs.Serve(wsLn) }()
	go func() { _ = admin.Serve(adminLn) }()
	srv.SetReady(true)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.CloseConnections(ctx)
		_ = hs.Close()
		_ = admin.Close()
	})

	return &env{
		t: t, cfg: cfg, srv: srv, metrics: m, spans: rec,
		wsURL:    "ws://" + wsLn.Addr().String() + cfg.WSPath,
		adminURL: "http://" + adminLn.Addr().String(),
	}
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func ctxT(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// dial connects without sending hello.
func (e *env) dial(o wstest.DialOptions) *wstest.Client {
	e.t.Helper()
	c, err := wstest.Dial(ctxT(e.t, 5*time.Second), e.wsURL, o)
	if err != nil {
		e.t.Fatalf("dial: %v", err)
	}
	e.t.Cleanup(func() { _ = c.WS.CloseNow() })
	return c
}

// connect dials and completes hello with opts.
func (e *env) connect(client protocol.ClientInfo, opts protocol.Options) (*wstest.Client, protocol.HelloAck) {
	e.t.Helper()
	return e.connectWith(wstest.DialOptions{}, client, opts)
}

func (e *env) connectWith(o wstest.DialOptions, client protocol.ClientInfo, opts protocol.Options) (*wstest.Client, protocol.HelloAck) {
	e.t.Helper()
	c := e.dial(o)
	ack, err := c.Hello(ctxT(e.t, 5*time.Second), client, opts)
	if err != nil {
		e.t.Fatalf("hello: %v", err)
	}
	return c, ack
}

// expectError reads one frame and asserts it is error{code}.
func expectError(t *testing.T, c *wstest.Client, code protocol.ErrorCode) protocol.Error {
	t.Helper()
	typ, b, err := c.RecvType(ctxT(t, 5*time.Second))
	if err != nil {
		t.Fatalf("waiting for error{%s}: %v", code, err)
	}
	var ef protocol.Error
	if typ != protocol.TypeError || json.Unmarshal(b, &ef) != nil || ef.Code != code {
		t.Fatalf("expected error{%s}, got %s", code, b)
	}
	return ef
}

// expectClose asserts the next read ends in a close frame with code.
func expectClose(t *testing.T, c *wstest.Client, code int) {
	t.Helper()
	b, err := c.Recv(ctxT(t, 10*time.Second))
	if err == nil {
		t.Fatalf("expected close %d, got frame %s", code, b)
	}
	if got := int(websocket.CloseStatus(err)); got != code {
		t.Fatalf("expected close %d, got %d (%v)", code, got, err)
	}
}

// expectOpen asserts the connection still round-trips a probe.
func expectOpen(t *testing.T, c *wstest.Client) {
	t.Helper()
	if _, b, err := c.RoundTrip(ctxT(t, 5*time.Second), c.NextProbe(wstest.ClientSkyEdge)); err != nil {
		t.Fatalf("connection not usable: %v %s", err, b)
	}
}

// scrape returns the /metrics body in Prometheus text format.
func (e *env) scrape() string {
	e.t.Helper()
	resp, err := http.Get(e.adminURL + e.cfg.MetricsPath)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// metric returns the value of the series whose name-and-labels text is
// exactly series, e.g. `wsmon_server_errors_total{code="bad_type"}`.
func metric(body, series string) (float64, bool) {
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		line := sc.Text()
		if rest, ok := strings.CutPrefix(line, series+" "); ok {
			v, err := strconv.ParseFloat(strings.Fields(rest)[0], 64)
			return v, err == nil
		}
	}
	return 0, false
}

// eventually polls cond until it holds or d elapses.
func eventually(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}

// dur picks the spec duration, or a short stand-in under -short.
func dur(full, short time.Duration) time.Duration {
	if testing.Short() {
		return short
	}
	return full
}

func isClosed(err error) bool {
	return err != nil && (websocket.CloseStatus(err) != -1 || errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF))
}
