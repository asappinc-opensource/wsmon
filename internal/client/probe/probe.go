package probe

import (
	"context"
	crand "crypto/rand"
	"crypto/x509"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// Options are process wiring (Logger, Version) plus test-only hooks. None of
// the test hooks is reachable from the environment.
type Options struct {
	// Logger receives all probe logs. Default: slog.Default().
	Logger *slog.Logger
	// Version is reported as client.version in hello and probe frames.
	Version string

	// WrapConn (test only) wraps the connection that carries WebSocket
	// bytes: the tls.Conn for wss://, the TCP conn for ws://. It is applied
	// to every connection, including reconnects.
	WrapConn func(net.Conn) net.Conn
	// RootCAs (test only) lets TLS tests trust a test CA without
	// WSMON_INSECURE_SKIP_VERIFY.
	RootCAs *x509.CertPool
	// Rand (test only) is the backoff/jitter source. Used only by the
	// lifecycle goroutine.
	Rand *rand.Rand
	// Now (test only) drives lifecycle scheduling decisions (connection
	// lifetime, backoff reset). t1 and t4 always use time.Now().
	Now func() time.Time
	// Extra (test only) is spliced into every probe frame (TC-CONTRACT-11).
	Extra protocol.Extra
	// OnBackoff (test only) observes every reconnect backoff sleep.
	OnBackoff func(time.Duration)
}

// Probe holds exactly one WebSocket connection at a time and drives one mode
// over it.
type Probe struct {
	cfg  config.Client
	rec  Recorder
	tr   Tracer
	opts Options
	log  *slog.Logger
	rnd  *rand.Rand
	now  func() time.Time

	clientJSON []byte
	clock      clockState

	payloadIdx int // payload rotation; owned by the mode goroutine

	ulidMu  sync.Mutex
	entropy io.Reader

	cur atomic.Pointer[conn] // the live connection, for diagnostics
}

// shutdownMargin is subtracted from WSMON_SHUTDOWN_TIMEOUT when waiting for
// in-flight echoes, leaving time for the close handshake and telemetry
// flush.
const shutdownMargin = time.Second

// closeTimeout bounds a client-initiated close handshake; after it the
// socket is abandoned.
const closeTimeout = 2 * time.Second

// readLimit is the largest inbound message accepted. Echo frames never carry
// padding, so this is generous.
const readLimit = 1 << 20

// New builds a probe. rec and tr may be NopRecorder / NopTracer.
func New(cfg config.Client, rec Recorder, tr Tracer, opts Options) *Probe {
	p := &Probe{cfg: cfg, rec: rec, tr: tr, opts: opts, log: opts.Logger, rnd: opts.Rand, now: opts.Now}
	if p.log == nil {
		p.log = slog.Default()
	}
	if p.rnd == nil {
		var seed [32]byte
		_, _ = crand.Read(seed[:])
		p.rnd = rand.New(rand.NewChaCha8(seed))
	}
	if p.now == nil {
		p.now = time.Now
	}
	p.clientJSON = protocol.EncodeClientInfo(protocol.ClientInfo{
		Name: cfg.Name, Mode: cfg.Mode, Version: opts.Version, Labels: cfg.Labels,
	})
	p.entropy = ulid.Monotonic(crand.Reader, 0)
	return p
}

// newConnID returns a fresh ULID. A conn_id is never reused.
func (p *Probe) newConnID() string {
	p.ulidMu.Lock()
	defer p.ulidMu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now()), p.entropy).String()
}

// nextPayload rotates through WSMON_PAYLOADS.
func (p *Probe) nextPayload() Payload {
	pl := Payload(p.cfg.Payloads[p.payloadIdx%len(p.cfg.Payloads)])
	p.payloadIdx++
	return pl
}

// Run is the lifecycle loop: jittered first connect, connect, run the mode,
// reconnect with full-jitter backoff. It returns nil when ctx is
// cancelled, after the graceful shutdown (readiness off, drain, close 1000).
func (p *Probe) Run(ctx context.Context) error {
	stopClock := make(chan struct{})
	defer close(stopClock)
	go p.clockLoop(stopClock)

	base, cap := p.cfg.BackoffBase, p.cfg.BackoffCap
	// The first connection also waits rand(0, base) so a fleet-wide rollout
	// does not connect in lockstep.
	if !sleepCtx(ctx, backoffDelay(p.rnd, base, cap, 0)) {
		return nil
	}

	attempt := 0
	for first := true; ; first = false {
		if !first {
			p.rec.Reconnect()
		}
		p.rec.ConnectAttempt()
		c, reason, err := p.connect(ctx)
		if ctx.Err() != nil && c == nil {
			// Shutdown during connect: exit immediately. Not counted as a
			// failure; the attempt was abandoned by us.
			return nil
		}
		if err != nil {
			p.rec.ConnectFailure(reason)
			p.log.Warn("connect failed", "reason", string(reason), "error", err.Error())
		} else {
			if lifetime := c.run(ctx); lifetime >= cap {
				attempt = 0
			}
			if ctx.Err() != nil {
				return nil
			}
		}
		d := backoffDelay(p.rnd, base, cap, attempt)
		if attempt < 62 {
			attempt++
		}
		if p.opts.OnBackoff != nil {
			p.opts.OnBackoff(d)
		}
		p.log.Debug("reconnecting after backoff", "delay", d)
		if !sleepCtx(ctx, d) {
			return nil
		}
	}
}

// CorrelatorSizes reports the live connection's in-flight map size and
// remembered-window size (diagnostics for TC-CORR-07). Both are 0 when no
// connection is up.
func (p *Probe) CorrelatorSizes() (inflight, window int) {
	if c := p.cur.Load(); c != nil {
		return c.corr.sizes()
	}
	return 0, 0
}

// sleepCtx sleeps for d, returning false early if ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
