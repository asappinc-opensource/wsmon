package config

import (
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// Modes. The binary knows these three values and nothing about
// topology.
const (
	ModePingpong = "pingpong"
	ModeBurst    = "burst"
	ModeIdle     = "idle"
)

// Payload names.
const (
	PayloadSmall = "small"
	PayloadLarge = "large"
)

// Client is the resolved `wsmon client` configuration.
type Client struct {
	Common

	Mode string
	// Labels are free-form name=value pairs from WSMON_LABELS. The binary
	// attaches them to its output and attaches no meaning to them.
	Labels map[string]string

	// URL is WSMON_URL as given. Dial DialURL(), which adds URLParams.
	URL *url.URL
	// URLParams are extra query parameters from WSMON_URL_PARAMS.
	URLParams url.Values
	SNIHost   string
	// HostHeader is the upgrade request's Host: WSMON_HOST_HEADER, or a
	// Host item in WSMON_HEADERS (setting both is an error).
	HostHeader string
	// Headers are extra upgrade-request headers from WSMON_HEADERS, keyed
	// canonically. Host is not in here; it is in HostHeader.
	Headers http.Header
	// UpgradeTraceparent sends a W3C traceparent header, carrying the
	// ws.connect span's trace context, on every upgrade request.
	UpgradeTraceparent bool
	ResolveOverride    string // raw "host:ip"; empty if unset
	ResolveHost        string // host part of ResolveOverride
	ResolveIP          net.IP // IP part of ResolveOverride

	Payloads          []string
	PingpongInterval  time.Duration
	BurstSize         int
	BurstInterval     time.Duration
	IdleLadder        []time.Duration
	MessageTimeout    time.Duration
	ConnectTimeout    time.Duration
	BackoffBase       time.Duration
	BackoffCap        time.Duration
	LargePayloadBytes int
	PushIntervalMs    int64

	InsecureSkipVerify bool

	RemoteWriteURL         string
	RemoteWriteInterval    time.Duration
	RemoteWriteTimeout     time.Duration
	RemoteWriteBearerToken string
	RemoteWriteUsername    string
	RemoteWritePassword    string

	TraceSlowThreshold time.Duration

	ClockOffsetInterval      time.Duration
	ClockOffsetWarnThreshold time.Duration
}

// Client-only variable names, in table order. Shared ones are in
// shared.go.
const (
	EnvURL                      = "WSMON_URL"
	EnvMode                     = "WSMON_MODE"
	EnvSNIHost                  = "WSMON_SNI_HOST"
	EnvHostHeader               = "WSMON_HOST_HEADER"
	EnvHeaders                  = "WSMON_HEADERS"
	EnvUpgradeTraceparent       = "WSMON_UPGRADE_TRACEPARENT"
	EnvURLParams                = "WSMON_URL_PARAMS"
	EnvResolveOverride          = "WSMON_RESOLVE_OVERRIDE"
	EnvPayloads                 = "WSMON_PAYLOADS"
	EnvPingpongInterval         = "WSMON_PINGPONG_INTERVAL"
	EnvBurstSize                = "WSMON_BURST_SIZE"
	EnvBurstInterval            = "WSMON_BURST_INTERVAL"
	EnvIdleLadder               = "WSMON_IDLE_LADDER"
	EnvMessageTimeout           = "WSMON_MESSAGE_TIMEOUT"
	EnvConnectTimeout           = "WSMON_CONNECT_TIMEOUT"
	EnvBackoffBase              = "WSMON_BACKOFF_BASE"
	EnvBackoffCap               = "WSMON_BACKOFF_CAP"
	EnvLargePayloadBytes        = "WSMON_LARGE_PAYLOAD_BYTES"
	EnvPushIntervalMs           = "WSMON_PUSH_INTERVAL_MS"
	EnvInsecureSkipVerify       = "WSMON_INSECURE_SKIP_VERIFY"
	EnvRemoteWriteURL           = "WSMON_REMOTE_WRITE_URL"
	EnvRemoteWriteInterval      = "WSMON_REMOTE_WRITE_INTERVAL"
	EnvRemoteWriteTimeout       = "WSMON_REMOTE_WRITE_TIMEOUT"
	EnvRemoteWriteBearerToken   = "WSMON_REMOTE_WRITE_BEARER_TOKEN"
	EnvRemoteWriteUsername      = "WSMON_REMOTE_WRITE_USERNAME"
	EnvRemoteWritePassword      = "WSMON_REMOTE_WRITE_PASSWORD"
	EnvTraceSlowThreshold       = "WSMON_TRACE_SLOW_THRESHOLD"
	EnvClockOffsetInterval      = "WSMON_CLOCK_OFFSET_INTERVAL"
	EnvClockOffsetWarnThreshold = "WSMON_CLOCK_OFFSET_WARN_THRESHOLD"
)

// Payload size bounds. MaxLargePayloadBytes stays below
// protocol.DefaultMaxFrameBytes so the largest probe always fits under the
// server's default frame limit (asserted in roles_test.go).
const (
	minLargePayloadBytes = 1024
	MaxLargePayloadBytes = 524288
)

// ClientVars lists every `wsmon client` variable with its default, in
// table order. Shared variables come from shared.go.
var ClientVars = []Var{
	{Name: EnvURL, Help: "required: ws:// or wss:// URL of the entry point"},
	{Name: EnvMode, Default: ModePingpong, Help: "pingpong, burst or idle"},
	sharedVar(EnvName, ""),
	sharedVar(EnvLabels, ""),
	{Name: EnvSNIHost, Help: "TLS SNI override"},
	{Name: EnvHostHeader, Help: "Host header override"},
	{Name: EnvHeaders, Help: "extra upgrade headers, Name=value,..."},
	{Name: EnvUpgradeTraceparent, Default: "true", Help: "send the ws.connect traceparent header on the upgrade request"},
	{Name: EnvURLParams, Help: "extra query parameters, name=value,..."},
	{Name: EnvResolveOverride, Help: "host:ip pin, curl --resolve style"},
	sharedVar(EnvAuthToken, ""),
	{Name: EnvPayloads, Default: "small,large", Help: "payload rotation"},
	{Name: EnvPingpongInterval, Default: "1s", Help: "pause between pingpong probes"},
	{Name: EnvBurstSize, Default: "20", Help: "probes per burst (1-100)"},
	{Name: EnvBurstInterval, Default: "10s", Help: "pause between bursts"},
	{Name: EnvIdleLadder, Default: "30s,60s,90s,120s,300s,600s", Help: "idle silences, strictly increasing"},
	{Name: EnvMessageTimeout, Default: "10s", Help: "echo deadline"},
	{Name: EnvConnectTimeout, Default: "15s", Help: "DNS through hello_ack"},
	{Name: EnvBackoffBase, Default: "1s", Help: "reconnect backoff base"},
	{Name: EnvBackoffCap, Default: "30s", Help: "reconnect backoff cap"},
	{Name: EnvLargePayloadBytes, Default: "32768", Help: "size of a large probe frame"},
	{Name: EnvPushIntervalMs, Default: "0", Help: "server push interval to request; 0 disables (pingpong only)"},
	sharedVar(EnvEnablePermessageDeflate, "false"),
	{Name: EnvInsecureSkipVerify, Default: "false", Help: "skip TLS verification (wss:// only)"},
	sharedVar(EnvMetricsAddr, DefaultMetricsAddr),
	{Name: EnvRemoteWriteURL, Help: "Prometheus remote-write URL; empty disables push"},
	{Name: EnvRemoteWriteInterval, Default: "15s", Help: "remote-write interval"},
	{Name: EnvRemoteWriteTimeout, Default: "10s", Help: "remote-write request timeout"},
	{Name: EnvRemoteWriteBearerToken, Secret: true, Help: "remote-write bearer token"},
	{Name: EnvRemoteWriteUsername, Help: "remote-write basic-auth user"},
	{Name: EnvRemoteWritePassword, Secret: true, Help: "remote-write basic-auth password"},
	sharedVar(EnvOTLPEndpoint, ""),
	sharedVar(EnvTraceSampleRatio, DefaultTraceSampleRatio),
	{Name: EnvTraceSlowThreshold, Default: "500ms", Help: "round trips slower than this are always traced"},
	{Name: EnvClockOffsetInterval, Default: "60s", Help: "clock-offset gauge interval"},
	{Name: EnvClockOffsetWarnThreshold, Default: "250ms", Help: "WARN above this clock offset"},
	sharedVar(EnvShutdownTimeout, "10s"),
	sharedVar(EnvLogLevel, "info"),
}

var required = []string{EnvURL}

// LoadClient reads and validates the client configuration. getenv is
// os.Getenv in production. An unset and an empty variable are the same: the
// default applies, or the variable is reported missing if it is required. On
// error, the returned *Error lists every problem found.
func LoadClient(getenv func(string) string) (Client, error) {
	l := newLoader(getenv, ClientVars)
	var c Client
	c.Common = l.loadCommon()

	for _, name := range required {
		if l.raw(name) == "" {
			l.addf(name, "required")
		}
	}

	c.Mode = l.str(EnvMode)
	c.Labels = l.labels(EnvLabels, clientReservedLabels)
	c.SNIHost = l.str(EnvSNIHost)
	c.HostHeader = l.str(EnvHostHeader)
	var host string
	c.Headers, host = l.headers(EnvHeaders)
	if host != "" {
		if c.HostHeader != "" {
			l.addf(EnvHeaders, "Host is also set by WSMON_HOST_HEADER; set only one")
		} else {
			c.HostHeader = host
		}
	}
	c.UpgradeTraceparent = l.boolean(EnvUpgradeTraceparent)
	c.URLParams = l.urlParams(EnvURLParams)
	c.ResolveOverride = l.str(EnvResolveOverride)

	c.Payloads = l.csv(EnvPayloads)
	c.PingpongInterval = l.duration(EnvPingpongInterval)
	c.BurstSize = int(l.integer(EnvBurstSize))
	c.BurstInterval = l.duration(EnvBurstInterval)
	for _, s := range l.csv(EnvIdleLadder) {
		d, err := time.ParseDuration(s)
		switch {
		case err != nil:
			l.addf(EnvIdleLadder, "invalid duration %q", s)
		case d <= 0:
			l.addf(EnvIdleLadder, "rung must be > 0, got %s", s)
		default:
			c.IdleLadder = append(c.IdleLadder, d)
		}
	}
	c.MessageTimeout = l.duration(EnvMessageTimeout)
	c.ConnectTimeout = l.duration(EnvConnectTimeout)
	c.BackoffBase = l.duration(EnvBackoffBase)
	c.BackoffCap = l.duration(EnvBackoffCap)
	c.LargePayloadBytes = int(l.integer(EnvLargePayloadBytes))
	c.PushIntervalMs = l.integer(EnvPushIntervalMs)
	c.InsecureSkipVerify = l.boolean(EnvInsecureSkipVerify)
	c.RemoteWriteURL = l.str(EnvRemoteWriteURL)
	c.RemoteWriteInterval = l.duration(EnvRemoteWriteInterval)
	c.RemoteWriteTimeout = l.duration(EnvRemoteWriteTimeout)
	c.RemoteWriteBearerToken = l.secret(EnvRemoteWriteBearerToken)
	c.RemoteWriteUsername = l.str(EnvRemoteWriteUsername)
	c.RemoteWritePassword = l.secret(EnvRemoteWritePassword)
	c.TraceSlowThreshold = l.duration(EnvTraceSlowThreshold)
	c.ClockOffsetInterval = l.duration(EnvClockOffsetInterval)
	c.ClockOffsetWarnThreshold = l.duration(EnvClockOffsetWarnThreshold)
	l.validateClient(&c)
	if err := l.err(); err != nil {
		return Client{}, err
	}
	return c, nil
}

// validateClient applies the cross-field rules. Field-level parse
// errors are already recorded. Every rule that exists because of what the
// server enforces is expressed with the protocol package's constants.
func (l *loader) validateClient(c *Client) {
	// Rule 2: mode.
	if c.Mode != "" && c.Mode != ModePingpong && c.Mode != ModeBurst && c.Mode != ModeIdle {
		l.addf(EnvMode, "must be one of pingpong, burst, idle; got %q", c.Mode)
	}

	// Rule 3: URL scheme; INSECURE_SKIP_VERIFY only with wss.
	if s := l.raw(EnvURL); s != "" {
		u, err := url.Parse(s)
		switch {
		case err != nil:
			l.addf(EnvURL, "invalid URL")
		case u.Scheme != "ws" && u.Scheme != "wss":
			l.addf(EnvURL, "scheme must be ws or wss, got %q", u.Scheme)
		case u.Hostname() == "":
			l.addf(EnvURL, "missing host")
		default:
			c.URL = u
		}
		if c.URL != nil && c.URL.Scheme == "ws" && c.InsecureSkipVerify {
			l.addf(EnvInsecureSkipVerify, "true is only valid with a wss:// URL")
		}
	}

	// Rule 17: the probe owns traceparent while WSMON_UPGRADE_TRACEPARENT is
	// on; a static one from WSMON_HEADERS would be silently replaced.
	if c.UpgradeTraceparent && c.Headers.Get("Traceparent") != "" {
		l.addf(EnvHeaders, "header \"Traceparent\" is set by the probe while WSMON_UPGRADE_TRACEPARENT=true; set that to false to send your own")
	}

	// Rule 16: a WSMON_URL_PARAMS name must not already be in WSMON_URL's
	// query, or which value wins would be up to the server.
	if c.URL != nil {
		q := c.URL.Query()
		for _, k := range sortedKeys(c.URLParams) {
			if q.Has(k) {
				l.addf(EnvURLParams, "parameter %q is already in the WSMON_URL query", k)
			}
		}
	}

	// Rule 4: push only in pingpong (D4). Rule 11: no negative push interval.
	if c.PushIntervalMs < 0 {
		l.addf(EnvPushIntervalMs, "must be >= 0, got %d", c.PushIntervalMs)
	}
	if c.PushIntervalMs > 0 && c.Mode != ModePingpong {
		l.addf(EnvPushIntervalMs, "push is only allowed with WSMON_MODE=pingpong (mode is %q)", c.Mode)
	}
	// The server adjusts out-of-range intervals and reports the adjusted
	// value in accepted_options, which the client must treat as
	// options_rejected on every connect. Refuse it here instead.
	if req := (protocol.Options{PushIntervalMs: c.PushIntervalMs}); c.PushIntervalMs > 0 && protocol.HonourOptions(req) != req {
		l.addf(EnvPushIntervalMs, "must be 0 or within %d-%d (the server's accepted range), got %d",
			protocol.MinPushInterval.Milliseconds(), protocol.MaxPushInterval.Milliseconds(), c.PushIntervalMs)
	}

	// Rule 5: burst size and large payload bounds (C6, C7).
	if c.BurstSize < 1 || c.BurstSize > 100 {
		l.addf(EnvBurstSize, "must be within 1-100, got %d", c.BurstSize)
	}
	if c.LargePayloadBytes < minLargePayloadBytes || c.LargePayloadBytes > MaxLargePayloadBytes {
		l.addf(EnvLargePayloadBytes, "must be within %d-%d, got %d", minLargePayloadBytes, MaxLargePayloadBytes, c.LargePayloadBytes)
	}

	// Rule 6: payload names; ladder strictly increasing.
	for _, p := range c.Payloads {
		if p != PayloadSmall && p != PayloadLarge {
			l.addf(EnvPayloads, "items must be small or large, got %q", p)
		}
	}
	for i := 1; i < len(c.IdleLadder); i++ {
		if c.IdleLadder[i] <= c.IdleLadder[i-1] {
			l.addf(EnvIdleLadder, "must be strictly increasing (%s then %s)", c.IdleLadder[i-1], c.IdleLadder[i])
			break
		}
	}

	// Rule 7 / 14: pingpong rate and message timeout vs smallest rung, in
	// every mode (pure shape checks).
	if c.PingpongInterval > 0 && c.PingpongInterval < time.Second/protocol.MaxMsgsPerSec {
		l.addf(EnvPingpongInterval, "must be >= %s (%d msg/s cap), got %s", time.Second/protocol.MaxMsgsPerSec, protocol.MaxMsgsPerSec, c.PingpongInterval)
	}
	if len(c.IdleLadder) > 0 && c.MessageTimeout > 0 && c.MessageTimeout >= c.IdleLadder[0] {
		l.addf(EnvMessageTimeout, "must be less than the smallest WSMON_IDLE_LADDER rung (%s), got %s", c.IdleLadder[0], c.MessageTimeout)
	}

	// Rule 9: remote-write auth is bearer or basic, not both.
	if c.RemoteWriteBearerToken != "" && c.RemoteWriteUsername != "" {
		l.addf(EnvRemoteWriteBearerToken, "mutually exclusive with WSMON_REMOTE_WRITE_USERNAME")
	}

	// Rule 12: RESOLVE_OVERRIDE is host:ip (host:[v6]) and its host must be
	// the URL's host, or the pin would silently do nothing.
	if c.ResolveOverride != "" {
		host, ip, ok := parseResolve(c.ResolveOverride)
		switch {
		case !ok:
			l.addf(EnvResolveOverride, "must be host:ip or host:[ipv6], got %q", c.ResolveOverride)
		case c.URL != nil && !strings.EqualFold(host, c.URL.Hostname()):
			l.addf(EnvResolveOverride, "host %q does not match the WSMON_URL host %q", host, c.URL.Hostname())
		default:
			c.ResolveHost, c.ResolveIP = host, ip
		}
	}

	// Rule 13: at most 200 msg/s on the connection.
	if c.BurstSize > 0 && c.BurstInterval > 0 {
		if rate := float64(c.BurstSize) / c.BurstInterval.Seconds(); rate > protocol.MaxMsgsPerSec {
			l.addf(EnvBurstInterval, "WSMON_BURST_SIZE/WSMON_BURST_INTERVAL is %.0f msg/s, above the %d msg/s cap", rate, protocol.MaxMsgsPerSec)
		}
	}
	if c.PushIntervalMs > 0 && c.PingpongInterval > 0 {
		if rate := 1/c.PingpongInterval.Seconds() + 1000/float64(c.PushIntervalMs); rate > protocol.MaxMsgsPerSec {
			l.addf(EnvPushIntervalMs, "probes plus push_acks are %.0f msg/s, above the %d msg/s cap", rate, protocol.MaxMsgsPerSec)
		}
	}

	// Rule 8 (parse): remote-write URL. The metrics address is checked by
	// loadCommon.
	if c.RemoteWriteURL != "" {
		u, err := url.Parse(c.RemoteWriteURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			l.addf(EnvRemoteWriteURL, "must be an http:// or https:// URL")
		}
	}
}

// clientReservedLabels are label names the probe sets itself on its own
// series (mode, and build_info / config_hash's labels).
var clientReservedLabels = map[string]bool{
	"mode": true, "name": true, "version": true, "go_version": true, "hash": true,
}

// pairs splits a "name=value,name=value" variable into its items, recording
// any item without "=" or with an empty name. Empty means none.
func (l *loader) pairs(name string) [][2]string {
	if l.raw(name) == "" {
		return nil
	}
	var out [][2]string
	for _, item := range l.csv(name) {
		k, v, ok := strings.Cut(item, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if !ok || k == "" {
			l.addf(name, "item %q must be name=value", item)
			continue
		}
		out = append(out, [2]string{k, v})
	}
	return out
}

// headerToken is the RFC 9110 field-name grammar.
var headerToken = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")

// reservedHeaders may not be set through WSMON_HEADERS: the WebSocket
// handshake or the HTTP client owns them, or a dedicated variable does.
var reservedHeaders = map[string]string{
	"Authorization":            "use WSMON_AUTH_TOKEN",
	"Connection":               "set by the WebSocket handshake",
	"Upgrade":                  "set by the WebSocket handshake",
	"Sec-Websocket-Key":        "set by the WebSocket handshake",
	"Sec-Websocket-Version":    "set by the WebSocket handshake",
	"Sec-Websocket-Protocol":   "always wsmon.v1",
	"Sec-Websocket-Extensions": "use WSMON_ENABLE_PERMESSAGE_DEFLATE",
	"Content-Length":           "managed by the HTTP client",
	"Transfer-Encoding":        "managed by the HTTP client",
	"Te":                       "managed by the HTTP client",
	"Trailer":                  "managed by the HTTP client",
	"Keep-Alive":               "managed by the HTTP client",
	"Proxy-Connection":         "managed by the HTTP client",
}

// headers parses WSMON_HEADERS: "Name=value,Name=value". Names are matched
// case-insensitively. A Host item is returned separately, because Go sends
// the request's Host field, never a Host header.
func (l *loader) headers(name string) (http.Header, string) {
	var out http.Header
	host := ""
	for _, kv := range l.pairs(name) {
		k, v := http.CanonicalHeaderKey(kv[0]), kv[1]
		switch {
		case !headerToken.MatchString(kv[0]):
			l.addf(name, "invalid header name %q", kv[0])
		case reservedHeaders[k] != "":
			l.addf(name, "header %q may not be set here: %s", k, reservedHeaders[k])
		case v == "" || strings.ContainsFunc(v, func(r rune) bool { return r < ' ' && r != '\t' || r == 0x7f }):
			l.addf(name, "header %q must have a non-empty value without control characters", k)
		case out.Get(k) != "" || (k == "Host" && host != ""):
			l.addf(name, "duplicate header %q", k)
		case k == "Host":
			host = v
		default:
			if out == nil {
				out = http.Header{}
			}
			out.Set(k, v)
		}
	}
	return out, host
}

// urlParams parses WSMON_URL_PARAMS: "name=value,name=value". Names and
// values are opaque; they are URL-encoded when the URL is built. A value may
// be empty ("flag=").
func (l *loader) urlParams(name string) url.Values {
	var out url.Values
	for _, kv := range l.pairs(name) {
		if out.Has(kv[0]) {
			l.addf(name, "duplicate parameter %q", kv[0])
			continue
		}
		if out == nil {
			out = url.Values{}
		}
		out.Set(kv[0], kv[1])
	}
	return out
}

// DialURL is WSMON_URL with WSMON_URL_PARAMS appended to its query. The
// query already in WSMON_URL is kept byte for byte.
func (c Client) DialURL() string {
	if c.URL == nil {
		return ""
	}
	if len(c.URLParams) == 0 {
		return c.URL.String()
	}
	u := *c.URL
	if u.RawQuery != "" {
		u.RawQuery += "&"
	}
	u.RawQuery += c.URLParams.Encode()
	return u.String()
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// LabelsString renders Labels canonically: sorted by name, "a=1,b=2".
func (c Client) LabelsString() string { return labelsString(c.Labels) }

// HeadersString renders Headers canonically: sorted by name, "A=1,B=2".
func (c Client) HeadersString() string {
	parts := make([]string, 0, len(c.Headers))
	for _, k := range sortedKeys(c.Headers) {
		parts = append(parts, k+"="+c.Headers.Get(k))
	}
	return strings.Join(parts, ",")
}

// URLParamsString renders URLParams canonically, unencoded: sorted by name,
// "a=1,b=2".
func (c Client) URLParamsString() string {
	parts := make([]string, 0, len(c.URLParams))
	for _, k := range sortedKeys(c.URLParams) {
		parts = append(parts, k+"="+c.URLParams.Get(k))
	}
	return strings.Join(parts, ",")
}

// parseResolve splits "host:ip" or "host:[ipv6]".
func parseResolve(s string) (host string, ip net.IP, ok bool) {
	host, addr, found := strings.Cut(s, ":")
	if !found || host == "" || addr == "" {
		return "", nil, false
	}
	if strings.HasPrefix(addr, "[") && strings.HasSuffix(addr, "]") {
		ip = net.ParseIP(addr[1 : len(addr)-1])
		if ip == nil || ip.To4() != nil {
			return "", nil, false
		}
		return host, ip, true
	}
	ip = net.ParseIP(addr)
	if ip == nil || ip.To4() == nil {
		return "", nil, false
	}
	return host, ip, true
}

// IdleBucketLabel renders a ladder rung as the idle_bucket label: whole
// seconds for whole-second rungs ("30"), the shortest decimal otherwise
// ("0.5").
func IdleBucketLabel(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64)
}

// values renders every variable's resolved value in ClientVars order. The
// URL is shown with any userinfo password masked.
func (c Client) values() []kv {
	durs := func(ds []time.Duration) string {
		s := make([]string, len(ds))
		for i, d := range ds {
			s[i] = d.String()
		}
		return strings.Join(s, ",")
	}
	u := ""
	if c.URL != nil {
		u = c.URL.Redacted()
	}
	return append([]kv{
		{EnvURL, u},
		{EnvMode, c.Mode},
		{EnvLabels, c.LabelsString()},
		{EnvSNIHost, c.SNIHost},
		{EnvHostHeader, c.HostHeader},
		{EnvHeaders, c.HeadersString()},
		{EnvUpgradeTraceparent, strconv.FormatBool(c.UpgradeTraceparent)},
		{EnvURLParams, c.URLParamsString()},
		{EnvResolveOverride, c.ResolveOverride},
		{EnvPayloads, strings.Join(c.Payloads, ",")},
		{EnvPingpongInterval, c.PingpongInterval.String()},
		{EnvBurstSize, strconv.Itoa(c.BurstSize)},
		{EnvBurstInterval, c.BurstInterval.String()},
		{EnvIdleLadder, durs(c.IdleLadder)},
		{EnvMessageTimeout, c.MessageTimeout.String()},
		{EnvConnectTimeout, c.ConnectTimeout.String()},
		{EnvBackoffBase, c.BackoffBase.String()},
		{EnvBackoffCap, c.BackoffCap.String()},
		{EnvLargePayloadBytes, strconv.Itoa(c.LargePayloadBytes)},
		{EnvPushIntervalMs, strconv.FormatInt(c.PushIntervalMs, 10)},
		{EnvInsecureSkipVerify, strconv.FormatBool(c.InsecureSkipVerify)},
		{EnvRemoteWriteURL, redactURL(c.RemoteWriteURL)},
		{EnvRemoteWriteInterval, c.RemoteWriteInterval.String()},
		{EnvRemoteWriteTimeout, c.RemoteWriteTimeout.String()},
		{EnvRemoteWriteBearerToken, c.RemoteWriteBearerToken},
		{EnvRemoteWriteUsername, c.RemoteWriteUsername},
		{EnvRemoteWritePassword, c.RemoteWritePassword},
		{EnvTraceSlowThreshold, c.TraceSlowThreshold.String()},
		{EnvClockOffsetInterval, c.ClockOffsetInterval.String()},
		{EnvClockOffsetWarnThreshold, c.ClockOffsetWarnThreshold.String()},
	}, c.Common.values()...)
}

func redactURL(s string) string {
	if u, err := url.Parse(s); err == nil {
		return u.Redacted()
	}
	return s
}

// Redacted returns every variable as a log attribute, keyed by its env name,
// with secrets redacted.
func (c Client) Redacted() []slog.Attr { return redactedAttrs(ClientVars, c.values()) }

// HashInput is the canonical, secret-free serialisation behind
// wsmon_client_config_hash.
func (c Client) HashInput() []byte { return hashInput(ClientVars, c.values()) }
