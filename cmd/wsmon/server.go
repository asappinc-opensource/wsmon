package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/server"
	"github.com/asappinc-opensource/wsmon/internal/server/servertel"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
)

// runServer runs `wsmon server` until SIGTERM/SIGINT, then drains.
func runServer(stdout io.Writer) int {
	cfg, err := config.LoadServer(os.Getenv)
	if err != nil {
		return configError(stdout, err)
	}
	log := telemetry.NewLogger(stdout, cfg.LogLevel)
	slog.SetDefault(log)

	// A small live heap collected rarely keeps GC pauses out of t3 - t2
	//. An explicit GOGC still wins.
	if _, ok := os.LookupEnv("GOGC"); !ok {
		debug.SetGCPercent(400)
	}

	identity := cfg.Identity(version)
	if cfg.NameDefaulted {
		log.Warn(config.EnvName+" is unset; using the host name", "name", cfg.Name)
	}
	log.LogAttrs(context.Background(), slog.LevelInfo, "configuration", cfg.Redacted()...)
	log.Info("wsmon server starting", "version", version)

	metrics := servertel.NewMetrics(version, identity.Name, cfg.Labels)
	tracing, err := servertel.NewTracing(context.Background(), cfg.OTLPEndpoint, cfg.TraceSampleRatio, identity, cfg.Labels)
	if err != nil {
		log.Error("tracing setup failed", "error", err.Error())
		return 1
	}
	srv := server.New(cfg, log, metrics, tracing, identity)

	wsLn, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Error("listen failed", "addr", cfg.ListenAddr, "error", err.Error())
		return 1
	}
	adminLn, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		log.Error("listen failed", "addr", cfg.MetricsAddr, "error", err.Error())
		return 1
	}
	wsSrv := server.NewHTTPServer(srv.Handler())
	adminSrv := telemetry.NewAdminServer(srv.AdminHandler())

	errc := make(chan error, 2)
	go func() { errc <- wsSrv.Serve(wsLn) }()
	go func() { errc <- adminSrv.Serve(adminLn) }()
	srv.SetReady(true)
	log.Info("listening", "ws", wsLn.Addr().String(), "path", cfg.WSPath, "admin", adminLn.Addr().String())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	select {
	case s := <-sig:
		log.Info("signal received; draining", "signal", s.String())
	case err := <-errc:
		log.Error("listener failed", "error", err.Error())
		return 1
	}

	// The whole drain sequence completes within shutdown_timeout.
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	// Step 1: fail /readyz. Step 2: give load balancers time to deregister.
	srv.BeginDrain()
	select {
	case <-time.After(cfg.DrainDelay):
	case <-ctx.Done():
	}

	// Steps 3–4: error{shutting_down}, then close 1001, on every connection.
	log.Info("closing connections", "active", srv.ActiveConnections())
	code := 0
	if err := srv.CloseConnections(ctx); err != nil {
		log.Warn("connections still open at shutdown timeout", "active", srv.ActiveConnections())
		code = 1
	}
	if err := wsSrv.Shutdown(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		log.Warn("ws listener shutdown", "error", err.Error())
	}
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer flushCancel()
	_ = tracing.Shutdown(flushCtx)
	_ = adminSrv.Shutdown(flushCtx)
	log.Info("shut down")
	return code
}

// configError logs every configuration problem in one line and
// returns exit code 2. Both roles report invalid configuration the same way.
func configError(stdout io.Writer, err error) int {
	problems, ok := config.IsError(err)
	if !ok {
		problems = []string{err.Error()}
	}
	telemetry.NewLogger(stdout, slog.LevelInfo).Error("invalid configuration", "errors", problems)
	return 2
}
