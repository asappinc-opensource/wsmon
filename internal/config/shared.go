package config

import (
	"log/slog"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Variables that mean the same thing in both roles. Each is declared once;
// the role tables in server.go and client.go reference these names, so the
// spelling, parsing and validation cannot differ between the two.
const (
	// EnvAuthToken is the bearer token: the server requires it on the
	// upgrade when set, the client sends it.
	EnvAuthToken = "WSMON_AUTH_TOKEN"
	// EnvMetricsAddr is the admin listener (/metrics, /healthz, /readyz).
	EnvMetricsAddr = "WSMON_METRICS_ADDR"
	// EnvEnablePermessageDeflate: the server accepts the extension when
	// offered, the client offers it.
	EnvEnablePermessageDeflate = "WSMON_ENABLE_PERMESSAGE_DEFLATE"
	EnvOTLPEndpoint            = "WSMON_OTLP_ENDPOINT"
	EnvTraceSampleRatio        = "WSMON_TRACE_SAMPLE_RATIO"
	EnvShutdownTimeout         = "WSMON_SHUTDOWN_TIMEOUT"
	EnvLogLevel                = "WSMON_LOG_LEVEL"
	// EnvName is this process's identity: server.name / client.name on the
	// wire, the name label on build_info, service.instance.id in traces.
	EnvName = "WSMON_NAME"
	// EnvLabels are free-form name=value pairs put on every metric and span.
	// Topology (pod, node, zone, region, site, ...) goes here: the binary
	// knows none of it.
	EnvLabels = "WSMON_LABELS"
)

// DefaultMetricsAddr is the admin listener of both roles. Each role runs in
// its own container, so they never clash in production; when running both on
// one host, move one of them with WSMON_METRICS_ADDR.
const DefaultMetricsAddr = ":9090"

// DefaultTraceSampleRatio is the head-sampling ratio of both roles.
// The server follows the client's decision for remote children, so in
// practice only the client's value matters for ws.echo.
const DefaultTraceSampleRatio = "0.02"

// sharedVar returns the canonical definition of a shared variable, with the
// role's default when the roles differ.
func sharedVar(name, def string) Var {
	help := map[string]string{
		EnvAuthToken:               "bearer token for the WebSocket upgrade",
		EnvMetricsAddr:             "admin listener: /metrics, /healthz, /readyz",
		EnvEnablePermessageDeflate: "permessage-deflate (no context takeover)",
		EnvOTLPEndpoint:            "OTLP/gRPC trace endpoint: host:port (plaintext) or URL; empty disables tracing",
		EnvTraceSampleRatio:        "head-sampling ratio in [0,1]",
		EnvShutdownTimeout:         "graceful shutdown budget",
		EnvLogLevel:                "debug, info, warn or error",
		EnvLabels:                  "name=value,... attached to every metric and span",
		EnvName:                    "this process's identity (host name if unset)",
	}[name]
	if help == "" {
		panic("config: " + name + " is not a shared variable")
	}
	return Var{Name: name, Default: def, Secret: name == EnvAuthToken, Help: help}
}

// Common holds the shared variables, resolved. Both role configs embed it.
type Common struct {
	// Name is WSMON_NAME, or the host name (the pod name in Kubernetes) when
	// unset, or "unknown" if that fails. NameDefaulted reports the fallback.
	Name          string
	NameDefaulted bool

	AuthToken               string
	MetricsAddr             string
	EnablePermessageDeflate bool
	OTLPEndpoint            string
	TraceSampleRatio        float64
	ShutdownTimeout         time.Duration
	LogLevel                slog.Level
}

// loadCommon reads the shared variables with the rules common to both roles.
func (l *loader) loadCommon() Common {
	c := Common{
		Name:                    l.str(EnvName),
		AuthToken:               l.secret(EnvAuthToken),
		MetricsAddr:             l.str(EnvMetricsAddr),
		EnablePermessageDeflate: l.boolean(EnvEnablePermessageDeflate),
		OTLPEndpoint:            l.str(EnvOTLPEndpoint),
		TraceSampleRatio:        l.ratio(EnvTraceSampleRatio),
		ShutdownTimeout:         l.duration(EnvShutdownTimeout),
		LogLevel:                l.logLevel(EnvLogLevel),
	}
	if _, port, err := net.SplitHostPort(c.MetricsAddr); err != nil || port == "" {
		l.addf(EnvMetricsAddr, "must be host:port or :port, got %q", c.MetricsAddr)
	} else if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		l.addf(EnvMetricsAddr, "invalid port %q", port)
	}
	if e := c.OTLPEndpoint; e != "" && strings.Contains(e, "://") {
		if u, err := url.Parse(e); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			l.addf(EnvOTLPEndpoint, "must be host:port or an http:// or https:// URL")
		}
	}
	if c.Name == "" {
		c.Name, c.NameDefaulted = defaultName(), true
	}
	return c
}

// values renders the shared variables for logging and hashing.
func (c Common) values() []kv {
	return []kv{
		{EnvName, c.Name},
		{EnvAuthToken, c.AuthToken},
		{EnvMetricsAddr, c.MetricsAddr},
		{EnvEnablePermessageDeflate, strconv.FormatBool(c.EnablePermessageDeflate)},
		{EnvOTLPEndpoint, c.OTLPEndpoint},
		{EnvTraceSampleRatio, strconv.FormatFloat(c.TraceSampleRatio, 'g', -1, 64)},
		{EnvShutdownTimeout, c.ShutdownTimeout.String()},
		{EnvLogLevel, strings.ToLower(c.LogLevel.String())},
	}
}

// MetricsPort is the port of MetricsAddr, for the healthcheck subcommand.
func MetricsPort(getenv func(string) string) (string, bool) {
	addr := strings.TrimSpace(getenv(EnvMetricsAddr))
	if addr == "" {
		addr = DefaultMetricsAddr
	}
	_, port, err := net.SplitHostPort(addr)
	return port, err == nil && port != ""
}

// labelName is the Prometheus label-name grammar.
var labelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// labels parses WSMON_LABELS: "name=value,name=value". Empty means none.
// reserved are the label names the role sets itself on its own series.
func (l *loader) labels(name string, reserved map[string]bool) map[string]string {
	s := l.raw(name)
	if s == "" {
		return nil
	}
	out := map[string]string{}
	for _, item := range l.csv(name) {
		k, v, ok := strings.Cut(item, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		switch {
		case !ok || v == "":
			l.addf(name, "item %q must be name=value", item)
		case !labelName.MatchString(k) || strings.HasPrefix(k, "__"):
			l.addf(name, "invalid label name %q", k)
		case reserved[k]:
			l.addf(name, "label name %q is reserved", k)
		case out[k] != "":
			l.addf(name, "duplicate label %q", k)
		default:
			out[k] = v
		}
	}
	return out
}

// labelsString renders labels canonically: sorted by name, "a=1,b=2".
func labelsString(labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for _, k := range sortedKeys(labels) {
		parts = append(parts, k+"="+labels[k])
	}
	return strings.Join(parts, ",")
}

// hostname is os.Hostname, replaceable in tests.
var hostname = os.Hostname

// defaultName is the identity of a process without WSMON_NAME.
func defaultName() string {
	if h, err := hostname(); err == nil && h != "" {
		return h
	}
	return "unknown"
}
