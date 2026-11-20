package config

import (
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// Server is the resolved `wsmon server` configuration. There is
// deliberately nothing that names a target, entry point or vantage: the
// server does not know those things.
type Server struct {
	Common

	ListenAddr         string
	WSPath             string
	MetricsPath        string
	MaxFrameBytes      int64
	MaxConnections     int
	MaxMsgsPerSec      int
	HelloTimeout       time.Duration
	WriteTimeout       time.Duration
	DrainDelay         time.Duration
	ServerPingInterval time.Duration
	// Labels are free-form name=value pairs from WSMON_LABELS, put on every
	// server metric and span. Pod, node, zone and the like go here.
	Labels map[string]string
}

// Server-only variable names. Shared ones are in shared.go.
const (
	EnvListenAddr         = "WSMON_LISTEN_ADDR"
	EnvWSPath             = "WSMON_WS_PATH"
	EnvMetricsPath        = "WSMON_METRICS_PATH"
	EnvMaxFrameBytes      = "WSMON_MAX_FRAME_BYTES"
	EnvMaxConnections     = "WSMON_MAX_CONNECTIONS"
	EnvMaxMsgsPerSec      = "WSMON_MAX_MSGS_PER_SEC"
	EnvHelloTimeout       = "WSMON_HELLO_TIMEOUT"
	EnvWriteTimeout       = "WSMON_WRITE_TIMEOUT"
	EnvDrainDelay         = "WSMON_DRAIN_DELAY"
	EnvServerPingInterval = "WSMON_SERVER_PING_INTERVAL"
)

// serverReservedLabels are label names the server sets itself on its own
// series (and that the Go collector's go_info uses).
var serverReservedLabels = map[string]bool{
	"name": true, "version": true, "type": true, "code": true, "close_code": true,
}

// ServerVars lists every `wsmon server` variable with its default.
var ServerVars = []Var{
	{Name: EnvListenAddr, Default: ":8080", Help: "WebSocket listener"},
	{Name: EnvWSPath, Default: protocol.DefaultWSPath, Help: "WebSocket upgrade path"},
	sharedVar(EnvMetricsAddr, DefaultMetricsAddr),
	{Name: EnvMetricsPath, Default: "/metrics", Help: "metrics path on the admin listener"},
	sharedVar(EnvAuthToken, ""),
	{Name: EnvMaxFrameBytes, Default: strconv.Itoa(protocol.DefaultMaxFrameBytes), Help: "largest inbound frame; larger closes 1009"},
	{Name: EnvMaxConnections, Default: "2000", Help: "connections above this get HTTP 503"},
	{Name: EnvMaxMsgsPerSec, Default: strconv.Itoa(protocol.MaxMsgsPerSec), Help: "per-connection message cap"},
	{Name: EnvHelloTimeout, Default: "10s", Help: "time allowed for the first frame"},
	{Name: EnvWriteTimeout, Default: "5s", Help: "bound on one write; exceeded closes 1011"},
	{Name: EnvDrainDelay, Default: "5s", Help: "pause between failing /readyz and closing connections"},
	sharedVar(EnvShutdownTimeout, "30s"),
	{Name: EnvServerPingInterval, Default: "30s", Help: "keepalive ping interval, only when the client asks"},
	sharedVar(EnvEnablePermessageDeflate, "true"),
	sharedVar(EnvName, ""),
	sharedVar(EnvLabels, ""),
	sharedVar(EnvOTLPEndpoint, ""),
	sharedVar(EnvTraceSampleRatio, DefaultTraceSampleRatio),
	sharedVar(EnvLogLevel, "info"),
}

// LoadServer reads and validates the server configuration. getenv is
// os.Getenv in production. As for the client, an unset and an empty variable
// are the same: the default applies.
func LoadServer(getenv func(string) string) (Server, error) {
	l := newLoader(getenv, ServerVars)
	c := Server{Common: l.loadCommon()}

	c.ListenAddr = l.str(EnvListenAddr)
	c.WSPath = l.str(EnvWSPath)
	c.MetricsPath = l.str(EnvMetricsPath)
	c.MaxFrameBytes = l.positive(EnvMaxFrameBytes)
	c.MaxConnections = int(l.positive(EnvMaxConnections))
	c.MaxMsgsPerSec = int(l.positive(EnvMaxMsgsPerSec))
	c.HelloTimeout = l.duration(EnvHelloTimeout)
	c.WriteTimeout = l.duration(EnvWriteTimeout)
	c.DrainDelay = l.durationMin0(EnvDrainDelay)
	c.ServerPingInterval = l.duration(EnvServerPingInterval)
	c.Labels = l.labels(EnvLabels, serverReservedLabels)

	if c.ListenAddr == c.MetricsAddr && !strings.HasSuffix(c.ListenAddr, ":0") {
		l.addf(EnvMetricsAddr, "must differ from %s", EnvListenAddr)
	}
	if !strings.HasPrefix(c.WSPath, "/") {
		l.addf(EnvWSPath, "must start with /")
	}
	switch {
	case !strings.HasPrefix(c.MetricsPath, "/"):
		l.addf(EnvMetricsPath, "must start with /")
	case c.MetricsPath == "/healthz" || c.MetricsPath == "/readyz":
		l.addf(EnvMetricsPath, "must not shadow a health endpoint")
	}
	if c.ShutdownTimeout > 0 && c.ShutdownTimeout <= c.DrainDelay {
		l.addf(EnvShutdownTimeout, "must exceed %s", EnvDrainDelay)
	}
	if err := l.err(); err != nil {
		return Server{}, err
	}
	return c, nil
}

// Identity returns the server identity echoed in every server → client
// frame.
func (c Server) Identity(version string) protocol.ServerInfo {
	return protocol.ServerInfo{Name: c.Name, Version: version}
}

// LabelsString renders Labels canonically: sorted by name, "a=1,b=2".
func (c Server) LabelsString() string { return labelsString(c.Labels) }

func (c Server) values() []kv {
	return append([]kv{
		{EnvListenAddr, c.ListenAddr},
		{EnvWSPath, c.WSPath},
		{EnvMetricsPath, c.MetricsPath},
		{EnvMaxFrameBytes, strconv.FormatInt(c.MaxFrameBytes, 10)},
		{EnvMaxConnections, strconv.Itoa(c.MaxConnections)},
		{EnvMaxMsgsPerSec, strconv.Itoa(c.MaxMsgsPerSec)},
		{EnvHelloTimeout, c.HelloTimeout.String()},
		{EnvWriteTimeout, c.WriteTimeout.String()},
		{EnvDrainDelay, c.DrainDelay.String()},
		{EnvServerPingInterval, c.ServerPingInterval.String()},
		{EnvLabels, c.LabelsString()},
	}, c.Common.values()...)
}

// Redacted returns every variable as a log attribute, keyed by its env name,
// with the auth token redacted.
func (c Server) Redacted() []slog.Attr { return redactedAttrs(ServerVars, c.values()) }
