package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/client/probetel"
	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
)

// runClient runs `wsmon client`: load and validate the configuration,
// log it with secrets redacted, then probe until
// SIGTERM/SIGINT.
func runClient(stdout io.Writer) int {
	cfg, err := config.LoadClient(os.Getenv)
	if err != nil {
		return configError(stdout, err)
	}
	log := telemetry.NewLogger(stdout, cfg.LogLevel)
	slog.SetDefault(log)

	log.LogAttrs(context.Background(), slog.LevelInfo, "configuration", cfg.Redacted()...)
	log.Info("wsmon client starting", "version", version)
	if cfg.NameDefaulted {
		log.Warn(config.EnvName+" is unset; using the host name", "name", cfg.Name)
	}

	tel, err := probetel.New(context.Background(), cfg, probetel.Options{Logger: log, Version: version})
	if err != nil {
		log.Error("cannot set up telemetry", "error", err.Error())
		return 1
	}
	// The listener is up before the first connect.
	if err := tel.Start(); err != nil {
		log.Error("cannot listen", "addr", cfg.MetricsAddr, "error", err.Error())
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	// Shutdown step 1: /readyz → 503 as soon as the signal arrives, before the
	// probe drains. The shutdown budget counts from the signal.
	signalled := make(chan time.Time, 1)
	go func() {
		<-ctx.Done()
		tel.BeginShutdown()
		signalled <- time.Now()
	}()

	p := probe.New(cfg, tel.Recorder(), tel.Tracer(), probe.Options{Logger: log, Version: version})
	runErr := p.Run(ctx) // shutdown steps 2–3: drain, close 1000

	var deadline time.Time
	select {
	case at := <-signalled:
		deadline = at.Add(cfg.ShutdownTimeout)
	default:
		deadline = time.Now().Add(cfg.ShutdownTimeout)
	}
	sctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	_ = tel.Shutdown(sctx) // shutdown steps 4–6: flush traces, final push, stop the server

	if runErr != nil {
		log.Error("probe stopped", "error", runErr.Error())
		return 1
	}
	log.Info("shut down")
	return 0
}
