package telemetry

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Health endpoint paths, identical for both roles so one probe definition
// (Kubernetes, container tasks, Docker HEALTHCHECK, `wsmon healthcheck`) fits either.
const (
	PathHealthz = "/healthz"
	PathReadyz  = "/readyz"
)

// AdminHandler serves metrics at metricsPath plus /healthz and /readyz on the
// admin listener, which is never the WebSocket port. healthy and
// ready return nil when OK, or the reason to report with a 503.
func AdminHandler(metricsPath string, g prometheus.Gatherer, healthy, ready func() error) http.Handler {
	mux := http.NewServeMux()
	// OpenMetrics is enabled so exemplars are exposed to scrapers that ask.
	mux.Handle(metricsPath, promhttp.HandlerFor(g, promhttp.HandlerOpts{EnableOpenMetrics: true}))
	mux.HandleFunc(PathHealthz, statusHandler(healthy))
	mux.HandleFunc(PathReadyz, statusHandler(ready))
	return mux
}

func statusHandler(check func() error) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if err := check(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	}
}

// NewAdminServer wraps an admin handler with the header-read bound both roles
// use.
func NewAdminServer(h http.Handler) *http.Server {
	return &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
}

// Healthcheck probes http://127.0.0.1:<port>/healthz for runtimes whose
// health check execs a command (distroless has no curl). It is the
// `wsmon healthcheck` subcommand and works for either role.
func Healthcheck(port string, timeout time.Duration) error {
	c := &http.Client{Timeout: timeout}
	resp, err := c.Get("http://" + net.JoinHostPort("127.0.0.1", port) + PathHealthz)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &statusError{resp.StatusCode}
	}
	return nil
}

type statusError struct{ code int }

func (e *statusError) Error() string { return "healthz returned " + http.StatusText(e.code) }

// NewLogger is the JSON logger of both roles, written to w (stdout in
// production).
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}
