package probetel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
)

// Self-monitoring.
const (
	TickInterval = 100 * time.Millisecond
	// WedgedAfter: /healthz fails once the ticker has not fired for this
	// long.
	WedgedAfter = 30 * time.Second
)

// Options are process wiring plus test-only hooks.
type Options struct {
	Logger  *slog.Logger
	Version string
	// SpanExporter (test only) replaces the OTLP exporter and enables
	// tracing; spans are exported synchronously.
	SpanExporter sdktrace.SpanExporter
	// Start (test only) overrides wsmon_client_start_time_seconds.
	Start time.Time
}

// Telemetry owns the registry, the recorder, the tracer, the remote-write
// pusher, the self-monitoring ticker and the health server.
type Telemetry struct {
	cfg    config.Client
	log    *slog.Logger
	reg    *prometheus.Registry
	rec    *Recorder
	tr     *Tracer
	pusher *pusher // nil unless remote-write is enabled

	ln  net.Listener
	srv *http.Server

	ready    atomic.Bool
	lastTick atomic.Int64 // unix ns of the last self-monitoring tick
	stopTick chan struct{}
	tickDone chan struct{}

	shutOnce sync.Once
}

// ConfigHash is wsmon_client_config_hash: the first 16 hex characters of
// sha256(config.HashInput()). Secrets are excluded by HashInput.
func ConfigHash(cfg config.Client) string {
	sum := sha256.Sum256(cfg.HashInput())
	return hex.EncodeToString(sum[:])[:16]
}

// New builds the telemetry for cfg. Nothing runs until Start.
func New(ctx context.Context, cfg config.Client, opts Options) (*Telemetry, error) {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	start := opts.Start
	if start.IsZero() {
		start = time.Now()
	}
	t := &Telemetry{cfg: cfg, log: log, reg: prometheus.NewRegistry(),
		stopTick: make(chan struct{}), tickDone: make(chan struct{})}
	// Every metric carries exactly the WSMON_LABELS pairs. No Go
	// or process collector: they are not part of the metric contract, and the pushed set
	// must equal the pulled set (TC-TEL-11).
	reg := prometheus.WrapRegistererWith(prometheus.Labels(cfg.Labels), t.reg)
	t.rec = newRecorder(reg, cfg, opts.Version, ConfigHash(cfg), start)
	tr, err := newTracer(ctx, cfg, opts.Version, opts.SpanExporter)
	if err != nil {
		return nil, err
	}
	t.tr = tr
	if cfg.RemoteWriteURL != "" {
		t.pusher = newPusher(cfg, opts.Version, t.reg, t.rec, log)
	}
	t.lastTick.Store(time.Now().UnixNano())
	return t, nil
}

// Recorder is the probe.Recorder.
func (t *Telemetry) Recorder() probe.Recorder { return t.rec }

// Tracer is the probe.Tracer.
func (t *Telemetry) Tracer() probe.Tracer { return t.tr }

// Gatherer is the registry behind /metrics and remote-write.
func (t *Telemetry) Gatherer() prometheus.Gatherer { return t.reg }

// Start listens on METRICS_ADDR and starts the health server, the
// self-monitoring ticker and the remote-write pusher. It must be called
// before the probe's first connect. /readyz is 200 from here on.
func (t *Telemetry) Start() error {
	ln, err := net.Listen("tcp", t.cfg.MetricsAddr)
	if err != nil {
		return err
	}
	t.ln = ln
	t.srv = telemetry.NewAdminServer(telemetry.AdminHandler("/metrics", t.reg, t.healthy, t.isReady))
	go func() {
		if err := t.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.log.Error("metrics server stopped", "error", err.Error())
		}
	}()
	go t.tick()
	if t.pusher != nil {
		t.pusher.start()
	}
	t.ready.Store(true)
	t.log.Info("metrics listening", "addr", ln.Addr().String())
	return nil
}

// Addr is the listener's address (useful with port 0).
func (t *Telemetry) Addr() net.Addr { return t.ln.Addr() }

// BeginShutdown turns /readyz to 503.
func (t *Telemetry) BeginShutdown() { t.ready.Store(false) }

// Shutdown runs the final shutdown steps within ctx: flush traces, final remote-write
// push, stop the metrics server. It is idempotent.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	var err error
	t.shutOnce.Do(func() {
		t.BeginShutdown()
		if e := t.tr.Shutdown(ctx); e != nil {
			t.log.Warn("trace flush failed", "error", e.Error())
			err = e
		}
		if t.pusher != nil {
			t.pusher.final(ctx)
		}
		close(t.stopTick)
		if t.srv != nil {
			if e := t.srv.Shutdown(ctx); e != nil {
				_ = t.srv.Close()
			}
			<-t.tickDone
		}
	})
	return err
}

// healthy fails once the self-monitoring ticker stalls. It never looks at
// WebSocket state.
func (t *Telemetry) healthy() error {
	if time.Since(time.Unix(0, t.lastTick.Load())) > WedgedAfter {
		return errStalled
	}
	return nil
}

// isReady fails from the shutdown signal on. It never looks at WebSocket
// state.
func (t *Telemetry) isReady() error {
	if !t.ready.Load() {
		return errShuttingDown
	}
	return nil
}

var (
	errStalled      = errors.New("self-monitoring ticker stalled")
	errShuttingDown = errors.New("shutting down")
)

// tick is the 100 ms self-monitoring loop. Each tick observes
// actual − scheduled fire time. When ticks are missed entirely, the schedule
// skips ahead so one stall is observed once, as its full delay.
func (t *Telemetry) tick() {
	defer close(t.tickDone)
	next := time.Now().Add(TickInterval)
	timer := time.NewTimer(TickInterval)
	defer timer.Stop()
	for {
		select {
		case <-t.stopTick:
			return
		case <-timer.C:
		}
		now := time.Now()
		lag := max(now.Sub(next), 0)
		t.rec.LoopLag(lag)
		t.lastTick.Store(now.UnixNano())
		next = next.Add(TickInterval)
		for !next.After(now) {
			next = next.Add(TickInterval)
		}
		timer.Reset(time.Until(next))
	}
}
