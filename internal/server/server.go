// Package server implements `wsmon server`: a trivial, layer-agnostic
// WebSocket echo server. It never branches on any client.* field.
package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/internal/server/servertel"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
)

// Server owns the WebSocket listener's handler and the admin (metrics and
// health) handler. The only state shared across connections is the
// connection count and the shutdown broadcast, both touched only at connect,
// disconnect and shutdown, never per frame.
type Server struct {
	cfg        config.Server
	log        *slog.Logger
	metrics    *servertel.Metrics
	tracing    *servertel.Tracing
	identity   protocol.ServerInfo
	serverJSON []byte
	accept     websocket.AcceptOptions

	active atomic.Int64
	ready  atomic.Bool

	// lifecycleMu orders connection registration against shutdown so no
	// connection is added to wg after CloseConnections starts waiting.
	lifecycleMu sync.Mutex
	rejecting   bool
	wg          sync.WaitGroup
	draining    atomic.Bool
	shutdown    chan struct{}
}

// New builds a server. identity is echoed in every server → client frame.
func New(cfg config.Server, log *slog.Logger, m *servertel.Metrics, t *servertel.Tracing, identity protocol.ServerInfo) *Server {
	s := &Server{
		cfg:        cfg,
		log:        log,
		metrics:    m,
		tracing:    t,
		identity:   identity,
		serverJSON: protocol.EncodeServerInfo(identity),
		shutdown:   make(chan struct{}),
	}
	s.accept = websocket.AcceptOptions{
		Subprotocols: []string{protocol.Subprotocol},
		// Probes are not browsers and authenticate with a bearer token, so
		// origin checks add nothing and could differ by entry point.
		InsecureSkipVerify: true,
		CompressionMode:    websocket.CompressionDisabled,
	}
	if cfg.EnablePermessageDeflate {
		// Negotiated only if the client offers it.
		s.accept.CompressionMode = websocket.CompressionNoContextTakeover
	}
	return s
}

// Handler serves WebSocket upgrades on WSPath and 404 elsewhere.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(s.cfg.WSPath, s.serveWS)
	return mux
}

// AdminHandler serves metrics, /healthz and /readyz. It belongs on
// MetricsAddr, never on the WebSocket port.
func (s *Server) AdminHandler() http.Handler {
	return telemetry.AdminHandler(s.cfg.MetricsPath, s.metrics.Registry,
		func() error { return nil },
		func() error {
			if !s.ready.Load() || s.draining.Load() {
				return errNotReady
			}
			return nil
		})
}

var errNotReady = errors.New("not ready")

// NewHTTPServer wraps h for the WebSocket listener. Only the header read is
// bounded: ReadTimeout, WriteTimeout and IdleTimeout would become deadlines
// on upgraded connections, i.e. a server-side idle timeout.
func NewHTTPServer(h http.Handler) *http.Server {
	return &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
}

// SetReady marks the listener as accepting.
func (s *Server) SetReady(ready bool) { s.ready.Store(ready) }

// BeginDrain fails /readyz. Connections are untouched.
func (s *Server) BeginDrain() { s.draining.Store(true) }

// CloseConnections sends error{shutting_down} and then close 1001 on every
// connection, rejects new upgrades with 503, and waits for
// connections to finish or ctx to expire.
func (s *Server) CloseConnections(ctx context.Context) error {
	s.lifecycleMu.Lock()
	if !s.rejecting {
		s.rejecting = true
		close(s.shutdown)
	}
	s.lifecycleMu.Unlock()

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ActiveConnections reports the number of open connections.
func (s *Server) ActiveConnections() int64 { return s.active.Load() }

func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	if s.cfg.AuthToken != "" && !s.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="wsmon"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.register() {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	defer s.unregister()

	ws, err := websocket.Accept(w, r, &s.accept)
	if err != nil {
		// Accept has already written an HTTP error response.
		s.log.Debug("upgrade failed", "err", err)
		return
	}
	// The library default is 32 KiB and it would close with 1009 on its own,
	// before we could send error{too_large}. The limit is enforced in
	// conn.readLoop instead.
	ws.SetReadLimit(-1)

	s.metrics.ConnectionsTotal.Inc()
	s.metrics.ConnectionsActive.Inc()
	defer s.metrics.ConnectionsActive.Dec()

	newConn(s, ws).run()
}

func (s *Server) authorized(r *http.Request) bool {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) < len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(h[len(prefix):]), []byte(s.cfg.AuthToken)) == 1
}

// register reserves a connection slot, or reports false when at
// max_connections or shutting down (HTTP 503).
func (s *Server) register() bool {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.rejecting || s.active.Load() >= int64(s.cfg.MaxConnections) {
		return false
	}
	s.active.Add(1)
	s.wg.Add(1)
	return true
}

func (s *Server) unregister() {
	s.active.Add(-1)
	s.wg.Done()
}
