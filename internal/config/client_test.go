package config

import (
	"bytes"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

func minimal() map[string]string {
	return map[string]string{
		EnvURL: "wss://relay.example.com/__synthetic/ws",
	}
}

func getenv(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func with(kv ...string) map[string]string {
	m := minimal()
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

func TestDefaults(t *testing.T) {
	c, err := LoadClient(getenv(minimal()))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name      string
		got, want any
	}{
		{EnvURL, c.URL.String(), "wss://relay.example.com/__synthetic/ws"},
		{EnvMode, c.Mode, "pingpong"},
		{EnvName, c.Name, defaultName()},
		{EnvLabels, c.Labels, map[string]string(nil)},
		{EnvSNIHost, c.SNIHost, ""},
		{EnvHostHeader, c.HostHeader, ""},
		{EnvHeaders, c.Headers, http.Header(nil)},
		{EnvUpgradeTraceparent, c.UpgradeTraceparent, true},
		{EnvURLParams, c.URLParams, url.Values(nil)},
		{EnvResolveOverride, c.ResolveOverride, ""},
		{EnvAuthToken, c.AuthToken, ""},
		{EnvPayloads, c.Payloads, []string{"small", "large"}},
		{EnvPingpongInterval, c.PingpongInterval, time.Second},
		{EnvBurstSize, c.BurstSize, 20},
		{EnvBurstInterval, c.BurstInterval, 10 * time.Second},
		{EnvIdleLadder, c.IdleLadder, []time.Duration{30 * time.Second, 60 * time.Second, 90 * time.Second, 120 * time.Second, 300 * time.Second, 600 * time.Second}},
		{EnvMessageTimeout, c.MessageTimeout, 10 * time.Second},
		{EnvConnectTimeout, c.ConnectTimeout, 15 * time.Second},
		{EnvBackoffBase, c.BackoffBase, time.Second},
		{EnvBackoffCap, c.BackoffCap, 30 * time.Second},
		{EnvLargePayloadBytes, c.LargePayloadBytes, 32768},
		{EnvPushIntervalMs, c.PushIntervalMs, int64(0)},
		{EnvEnablePermessageDeflate, c.EnablePermessageDeflate, false},
		{EnvInsecureSkipVerify, c.InsecureSkipVerify, false},
		{EnvMetricsAddr, c.MetricsAddr, ":9090"},
		{EnvRemoteWriteURL, c.RemoteWriteURL, ""},
		{EnvRemoteWriteInterval, c.RemoteWriteInterval, 15 * time.Second},
		{EnvRemoteWriteTimeout, c.RemoteWriteTimeout, 10 * time.Second},
		{EnvRemoteWriteBearerToken, c.RemoteWriteBearerToken, ""},
		{EnvRemoteWriteUsername, c.RemoteWriteUsername, ""},
		{EnvRemoteWritePassword, c.RemoteWritePassword, ""},
		{EnvOTLPEndpoint, c.OTLPEndpoint, ""},
		{EnvTraceSampleRatio, c.TraceSampleRatio, 0.02},
		{EnvTraceSlowThreshold, c.TraceSlowThreshold, 500 * time.Millisecond},
		{EnvClockOffsetInterval, c.ClockOffsetInterval, 60 * time.Second},
		{EnvClockOffsetWarnThreshold, c.ClockOffsetWarnThreshold, 250 * time.Millisecond},
		{EnvShutdownTimeout, c.ShutdownTimeout, 10 * time.Second},
		{EnvLogLevel, c.LogLevel, slog.LevelInfo},
	}
	if len(cases) != len(ClientVars) {
		t.Errorf("%d default rows for %d variables", len(cases), len(ClientVars))
	}
	for i, tc := range cases {
		if ClientVars[i].Name != tc.name {
			t.Errorf("row %d: table order %s vs %s", i, ClientVars[i].Name, tc.name)
		}
		if !reflect.DeepEqual(tc.got, tc.want) {
			t.Errorf("%s: got %#v, want %#v", tc.name, tc.got, tc.want)
		}
	}
}

func TestLabels(t *testing.T) {
	c, err := LoadClient(getenv(with(EnvLabels, " team = netops , site=nimbus-east1,env=prod ")))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"team": "netops", "site": "nimbus-east1", "env": "prod"}
	if !reflect.DeepEqual(c.Labels, want) {
		t.Errorf("labels %v", c.Labels)
	}
	if got := c.LabelsString(); got != "env=prod,site=nimbus-east1,team=netops" {
		t.Errorf("canonical %q", got)
	}
}

func TestHeaders(t *testing.T) {
	c, err := LoadClient(getenv(with(EnvHeaders, " x-synthetic = wsmon , User-Agent=wsmon-probe/1 (synthetic),X-Token=a=b==")))
	if err != nil {
		t.Fatal(err)
	}
	want := http.Header{"X-Synthetic": {"wsmon"}, "User-Agent": {"wsmon-probe/1 (synthetic)"}, "X-Token": {"a=b=="}}
	if !reflect.DeepEqual(c.Headers, want) || c.HostHeader != "" {
		t.Errorf("headers %v, host %q", c.Headers, c.HostHeader)
	}
	if got := c.HeadersString(); got != "User-Agent=wsmon-probe/1 (synthetic),X-Synthetic=wsmon,X-Token=a=b==" {
		t.Errorf("canonical %q", got)
	}
	// Host in WSMON_HEADERS becomes the Host override, not a header.
	c, err = LoadClient(getenv(with(EnvHeaders, "host=relay.example.com,X-A=1")))
	if err != nil {
		t.Fatal(err)
	}
	if c.HostHeader != "relay.example.com" || c.Headers.Get("Host") != "" || c.HeadersString() != "X-A=1" {
		t.Errorf("host %q, headers %v", c.HostHeader, c.Headers)
	}
}

func TestURLParams(t *testing.T) {
	c, err := LoadClient(getenv(with(EnvURL, "wss://relay.example.com/__synthetic/ws?a=1&z=%2F",
		EnvURLParams, "src=wsmon, flag= ,q=a b&c")))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.URLParamsString(); got != "flag=,q=a b&c,src=wsmon" {
		t.Errorf("canonical %q", got)
	}
	// The URL's own query is kept verbatim; params are appended, encoded.
	if got, want := c.DialURL(), "wss://relay.example.com/__synthetic/ws?a=1&z=%2F&flag=&q=a+b%26c&src=wsmon"; got != want {
		t.Errorf("DialURL %q, want %q", got, want)
	}
	if c.URL.RawQuery != "a=1&z=%2F" {
		t.Errorf("URL mutated: %q", c.URL.RawQuery)
	}
	c, _ = LoadClient(getenv(with(EnvURLParams, "a=1")))
	if got := c.DialURL(); got != "wss://relay.example.com/__synthetic/ws?a=1" {
		t.Errorf("DialURL without query %q", got)
	}
	c, _ = LoadClient(getenv(minimal()))
	if got := c.DialURL(); got != "wss://relay.example.com/__synthetic/ws" {
		t.Errorf("DialURL without params %q", got)
	}
}

func TestParsesValues(t *testing.T) {
	c, err := LoadClient(getenv(with(
		EnvMode, "idle", EnvPayloads, " large , small ", EnvIdleLadder, "500ms,1s,1.5s", EnvMessageTimeout, "200ms",
		EnvURL, "ws://wsmon-server.wsmon.svc.cluster.local:8080/__synthetic/ws",
		EnvResolveOverride, "wsmon-server.wsmon.svc.cluster.local:[::1]",
		EnvEnablePermessageDeflate, "true", EnvLogLevel, "DEBUG", EnvTraceSampleRatio, "1",
	)))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Payloads, []string{"large", "small"}) || len(c.IdleLadder) != 3 || !c.EnablePermessageDeflate ||
		c.LogLevel != slog.LevelDebug || c.TraceSampleRatio != 1 || !c.ResolveIP.Equal(net.ParseIP("::1")) ||
		c.ResolveHost != "wsmon-server.wsmon.svc.cluster.local" {
		t.Errorf("%+v", c)
	}
}

// One row per validation rule (in validateClient order).
var invalid = []struct {
	rule int
	name string
	env  map[string]string
	want string // variable that must be named
}{
	{1, "url unset", with(EnvURL, ""), EnvURL},
	{1, "url blank", with(EnvURL, "  "), EnvURL},
	{2, "bad mode", with(EnvMode, "push"), EnvMode},
	{3, "https url", with(EnvURL, "https://relay.example.com/__synthetic/ws"), EnvURL},
	{3, "no host", with(EnvURL, "wss:///__synthetic/ws"), EnvURL},
	{3, "insecure with ws", with(EnvURL, "ws://h/x", EnvInsecureSkipVerify, "true"), EnvInsecureSkipVerify},
	{15, "label without value", with(EnvLabels, "site"), EnvLabels},
	{15, "label empty value", with(EnvLabels, "site="), EnvLabels},
	{15, "label bad name", with(EnvLabels, "my-site=x"), EnvLabels},
	{15, "label reserved prefix", with(EnvLabels, "__name__=x"), EnvLabels},
	{15, "label reserved", with(EnvLabels, "mode=idle"), EnvLabels},
	{15, "label duplicate", with(EnvLabels, "a=1,a=2"), EnvLabels},
	{15, "label empty item", with(EnvLabels, "a=1,,b=2"), EnvLabels},
	{16, "header without value", with(EnvHeaders, "X-A"), EnvHeaders},
	{16, "header empty value", with(EnvHeaders, "X-A="), EnvHeaders},
	{16, "header bad name", with(EnvHeaders, "X A=1"), EnvHeaders},
	{16, "header duplicate", with(EnvHeaders, "X-A=1,x-a=2"), EnvHeaders},
	{16, "header authorization", with(EnvHeaders, "authorization=Bearer x"), EnvHeaders},
	{16, "header subprotocol", with(EnvHeaders, "Sec-WebSocket-Protocol=other"), EnvHeaders},
	{16, "header connection", with(EnvHeaders, "Connection=close"), EnvHeaders},
	{16, "header host twice", with(EnvHeaders, "Host=a,host=b"), EnvHeaders},
	{16, "host header and Host", with(EnvHostHeader, "a", EnvHeaders, "Host=b"), EnvHeaders},
	{17, "traceparent header while probe sends it", with(EnvHeaders, "traceparent=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"), EnvUpgradeTraceparent},
	{17, "upgrade traceparent not bool", with(EnvUpgradeTraceparent, "maybe"), EnvUpgradeTraceparent},
	{16, "param without =", with(EnvURLParams, "flag"), EnvURLParams},
	{16, "param empty name", with(EnvURLParams, "=1"), EnvURLParams},
	{16, "param duplicate", with(EnvURLParams, "a=1,a=2"), EnvURLParams},
	{16, "param already in url", with(EnvURL, "wss://h/x?a=0", EnvURLParams, "a=1"), EnvURLParams},
	{4, "push in burst", with(EnvMode, "burst", EnvPushIntervalMs, "1000"), EnvPushIntervalMs},
	{4, "push in idle", with(EnvMode, "idle", EnvPushIntervalMs, "1000"), EnvPushIntervalMs},
	{5, "burst 0", with(EnvBurstSize, "0"), EnvBurstSize},
	{5, "burst 101", with(EnvBurstSize, "101"), EnvBurstSize},
	{5, "large 1023", with(EnvLargePayloadBytes, "1023"), EnvLargePayloadBytes},
	{5, "large 524289", with(EnvLargePayloadBytes, "524289"), EnvLargePayloadBytes},
	{6, "bad payload", with(EnvPayloads, "small,huge"), EnvPayloads},
	{6, "ladder not increasing", with(EnvIdleLadder, "30s,30s"), EnvIdleLadder},
	{6, "ladder decreasing", with(EnvIdleLadder, "60s,30s"), EnvIdleLadder},
	{7, "pingpong too fast", with(EnvPingpongInterval, "4ms"), EnvPingpongInterval},
	{7, "timeout >= rung", with(EnvMessageTimeout, "30s"), EnvMessageTimeout},
	{8, "bad duration", with(EnvConnectTimeout, "15"), EnvConnectTimeout},
	{8, "bad int", with(EnvBurstSize, "twenty"), EnvBurstSize},
	{8, "bad bool", with(EnvEnablePermessageDeflate, "yes"), EnvEnablePermessageDeflate},
	{8, "bad float", with(EnvTraceSampleRatio, "2%"), EnvTraceSampleRatio},
	{8, "empty csv item", with(EnvPayloads, "small,,large"), EnvPayloads},
	{8, "bad ladder item", with(EnvIdleLadder, "30s,1m,x"), EnvIdleLadder},
	{8, "bad metrics addr", with(EnvMetricsAddr, "9090"), EnvMetricsAddr},
	{8, "bad remote write url", with(EnvRemoteWriteURL, "prom:9090/api/v1/write"), EnvRemoteWriteURL},
	{9, "bearer and basic", with(EnvRemoteWriteBearerToken, "tok", EnvRemoteWriteUsername, "u"), EnvRemoteWriteBearerToken},
	{10, "ratio > 1", with(EnvTraceSampleRatio, "1.5"), EnvTraceSampleRatio},
	{10, "ratio < 0", with(EnvTraceSampleRatio, "-0.1"), EnvTraceSampleRatio},
	{10, "log level", with(EnvLogLevel, "trace"), EnvLogLevel},
	{11, "zero duration", with(EnvMessageTimeout, "0s"), EnvMessageTimeout},
	{11, "negative duration", with(EnvBackoffCap, "-1s"), EnvBackoffCap},
	{11, "zero rung", with(EnvIdleLadder, "0s,30s"), EnvIdleLadder},
	{11, "negative push", with(EnvPushIntervalMs, "-1"), EnvPushIntervalMs},
	{12, "resolve no ip", with(EnvResolveOverride, "relay.example.com"), EnvResolveOverride},
	{12, "resolve bad ip", with(EnvResolveOverride, "relay.example.com:10.0.0"), EnvResolveOverride},
	{12, "resolve v6 without brackets", with(EnvResolveOverride, "relay.example.com:::1"), EnvResolveOverride},
	{12, "resolve host mismatch", with(EnvResolveOverride, "other.example.com:10.0.0.1"), EnvResolveOverride},
	{13, "burst rate", with(EnvMode, "burst", EnvBurstSize, "100", EnvBurstInterval, "400ms"), EnvBurstInterval},
	{13, "push rate", with(EnvPingpongInterval, "10ms", EnvPushIntervalMs, "9"), EnvPushIntervalMs},
	{14, "pingpong interval in idle", with(EnvMode, "idle", EnvPingpongInterval, "1ms"), EnvPingpongInterval},
	{14, "timeout >= rung in burst", with(EnvMode, "burst", EnvIdleLadder, "5s,10s"), EnvMessageTimeout},
}

func TestValidation(t *testing.T) {
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadClient(getenv(tc.env))
			if err == nil {
				t.Fatalf("rule %d: accepted", tc.rule)
			}
			var ce *Error
			if !errors.As(err, &ce) || len(ce.Problems) == 0 {
				t.Fatalf("not a *Error: %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("rule %d: error does not name %s: %v", tc.rule, tc.want, err)
			}
		})
	}
}

func TestValidEdges(t *testing.T) {
	for _, env := range []map[string]string{
		with(EnvPingpongInterval, "5ms"),
		with(EnvBurstSize, "1"), with(EnvBurstSize, "100"),
		with(EnvLargePayloadBytes, "1024"), with(EnvLargePayloadBytes, "524288"),
		with(EnvPushIntervalMs, "100"),
		with(EnvMessageTimeout, "29s"),
		with(EnvMode, "burst", EnvBurstSize, "100", EnvBurstInterval, "500ms"),
		with(EnvResolveOverride, "RELAY.example.com:203.0.113.10"),
		with(EnvURL, "ws://h/x", EnvInsecureSkipVerify, "false"),
		with(EnvRemoteWriteUsername, "u", EnvRemoteWritePassword, "p", EnvRemoteWriteURL, "https://prom/api/v1/write"),
		with(EnvTraceSampleRatio, "0"),
		// A static traceparent is allowed once the probe stops sending one.
		with(EnvUpgradeTraceparent, "false", EnvHeaders, "Traceparent=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
		with(EnvMetricsAddr, "127.0.0.1:19090"),
		// Label values are opaque.
		with(EnvLabels, "entrypoint=anything_at_all,provider=my-own-dc,region=mars-north-1"),
	} {
		if _, err := LoadClient(getenv(env)); err != nil {
			t.Errorf("rejected: %v", err)
		}
	}
}

func TestAllProblemsReported(t *testing.T) {
	_, err := LoadClient(getenv(map[string]string{EnvBurstSize: "0", EnvLogLevel: "loud"}))
	probs, ok := IsError(err)
	if !ok || len(probs) != len(required)+2 {
		t.Fatalf("want %d problems, got %v", len(required)+2, probs)
	}
}

func TestIdleBucketLabel(t *testing.T) {
	for d, want := range map[time.Duration]string{
		30 * time.Second: "30", 600 * time.Second: "600", 500 * time.Millisecond: "0.5",
		1500 * time.Millisecond: "1.5", 250 * time.Millisecond: "0.25",
	} {
		if got := IdleBucketLabel(d); got != want {
			t.Errorf("%s: %q, want %q", d, got, want)
		}
	}
}

const canary = "s3cr3t-canary"

func TestRedacted(t *testing.T) {
	env := with(EnvAuthToken, canary, EnvRemoteWritePassword, canary+"2", EnvRemoteWriteUsername, "user",
		EnvURL, "wss://u:"+canary+"3@relay.example.com/__synthetic/ws")
	c, err := LoadClient(getenv(env))
	if err != nil {
		t.Fatal(err)
	}
	attrs := c.Redacted()
	if len(attrs) != len(ClientVars) {
		t.Errorf("%d attrs for %d variables", len(attrs), len(ClientVars))
	}
	got := map[string]string{}
	for _, a := range attrs {
		got[a.Key] = a.Value.String()
		if strings.Contains(a.Value.String(), canary) {
			t.Errorf("%s leaks a secret: %q", a.Key, a.Value)
		}
	}
	if got[EnvAuthToken] != "[redacted]" || got[EnvRemoteWritePassword] != "[redacted]" || got[EnvRemoteWriteBearerToken] != "" {
		t.Errorf("secret rendering: %v", got)
	}
	if got[EnvRemoteWriteUsername] != "user" || got[EnvURL] != "wss://u:xxxxx@relay.example.com/__synthetic/ws" {
		t.Errorf("non-secrets: %v", got)
	}
}

func TestHashInput(t *testing.T) {
	base, _ := LoadClient(getenv(minimal()))
	h := base.HashInput()
	if bytes.Contains(h, []byte(canary)) {
		t.Fatal("secret in hash input")
	}
	for _, s := range []string{EnvAuthToken, EnvRemoteWriteBearerToken, EnvRemoteWritePassword} {
		c, err := LoadClient(getenv(with(s, canary)))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(c.HashInput(), h) {
			t.Errorf("changing %s changed the hash input", s)
		}
	}
	// Same value, different spelling: same hash input.
	c, _ := LoadClient(getenv(with(EnvPingpongInterval, "1000ms")))
	if !bytes.Equal(c.HashInput(), h) {
		t.Error("1000ms and 1s hash differently")
	}
	// Changing any non-secret field changes it.
	changes := map[string]string{
		EnvMode: "burst", EnvLabels: "site=x", EnvHeaders: "X-A=1", EnvURLParams: "a=1", EnvURL: "wss://relay.example.com/other", EnvSNIHost: "sni", EnvHostHeader: "hh", EnvUpgradeTraceparent: "false",
		EnvResolveOverride: "relay.example.com:10.0.0.1", EnvPayloads: "small", EnvPingpongInterval: "2s",
		EnvBurstSize: "21", EnvBurstInterval: "11s", EnvIdleLadder: "30s,60s", EnvMessageTimeout: "9s",
		EnvConnectTimeout: "14s", EnvBackoffBase: "2s", EnvBackoffCap: "31s", EnvLargePayloadBytes: "2048",
		EnvPushIntervalMs: "1000", EnvEnablePermessageDeflate: "true", EnvInsecureSkipVerify: "true",
		EnvMetricsAddr: ":9091", EnvRemoteWriteURL: "https://p/w", EnvRemoteWriteInterval: "16s",
		EnvRemoteWriteTimeout: "9s", EnvRemoteWriteUsername: "u", EnvOTLPEndpoint: "otel:4317",
		EnvTraceSampleRatio: "0.5", EnvTraceSlowThreshold: "1s", EnvClockOffsetInterval: "61s",
		EnvClockOffsetWarnThreshold: "1s", EnvShutdownTimeout: "9s", EnvLogLevel: "debug",
	}
	nsecret := 0
	for _, v := range ClientVars {
		if v.Secret {
			nsecret++
		}
	}
	// WSMON_NAME is identity, not configuration: it is excluded from the
	// hash (TestNameDefaultsToHostname).
	if len(changes)+nsecret+1 != len(ClientVars) {
		t.Errorf("hash test covers %d of %d non-secret variables", len(changes), len(ClientVars)-nsecret-1)
	}
	for k, v := range changes {
		c, err := LoadClient(getenv(with(k, v)))
		if err != nil {
			t.Fatalf("%s=%s: %v", k, v, err)
		}
		if bytes.Equal(c.HashInput(), h) {
			t.Errorf("changing %s did not change the hash input", k)
		}
	}
}
