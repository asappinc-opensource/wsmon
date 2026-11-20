package config

import (
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

func TestServerDefaultsMatchSpec(t *testing.T) {
	c, err := LoadServer(getenv(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := Server{
		Common: Common{
			MetricsAddr: ":9090", ShutdownTimeout: 30 * time.Second,
			EnablePermessageDeflate: true, TraceSampleRatio: 0.02, LogLevel: slog.LevelInfo,
		},
		ListenAddr: ":8080", WSPath: "/__synthetic/ws", MetricsPath: "/metrics",
		MaxFrameBytes: 1048576, MaxConnections: 2000, MaxMsgsPerSec: 200,
		HelloTimeout: 10 * time.Second, WriteTimeout: 5 * time.Second, DrainDelay: 5 * time.Second,
		ServerPingInterval: 30 * time.Second,
	}
	want.Name, want.NameDefaulted = defaultName(), true
	if !reflect.DeepEqual(c, want) {
		t.Errorf("defaults\n got %+v\nwant %+v", c, want)
	}
}

func TestServerLoadFromEnv(t *testing.T) {
	c, err := LoadServer(getenv(map[string]string{
		"WSMON_MAX_CONNECTIONS":           "10",
		"WSMON_HELLO_TIMEOUT":             "1500ms",
		"WSMON_ENABLE_PERMESSAGE_DEFLATE": "false",
		"WSMON_TRACE_SAMPLE_RATIO":        "1",
		"WSMON_AUTH_TOKEN":                "tok",
		"WSMON_DRAIN_DELAY":               "0s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxConnections != 10 || c.HelloTimeout != 1500*time.Millisecond || c.EnablePermessageDeflate || c.TraceSampleRatio != 1 || c.AuthToken != "tok" || c.DrainDelay != 0 {
		t.Errorf("got %+v", c)
	}
	for _, a := range c.Redacted() {
		if a.Key == EnvAuthToken && a.Value.String() != "[redacted]" {
			t.Errorf("token not redacted: %s", a.Value)
		}
	}
}

func TestServerRejectsInvalid(t *testing.T) {
	for k, v := range map[string]string{
		"WSMON_MAX_CONNECTIONS":    "x",
		"WSMON_MAX_FRAME_BYTES":    "0",
		"WSMON_HELLO_TIMEOUT":      "10",
		"WSMON_TRACE_SAMPLE_RATIO": "1.5",
		"WSMON_WS_PATH":            "no-slash",
		"WSMON_METRICS_ADDR":       ":8080",
		"WSMON_METRICS_PATH":       "/healthz",
		"WSMON_LOG_LEVEL":          "loud",
		"WSMON_DRAIN_DELAY":        "31s",
		"WSMON_OTLP_ENDPOINT":      "grpc://collector:4317",
		"WSMON_LABELS":             "version=x",
	} {
		_, err := LoadServer(getenv(map[string]string{k: v}))
		if err == nil {
			t.Errorf("%s=%s accepted", k, v)
			continue
		}
		if !strings.Contains(err.Error(), "WSMON_") {
			t.Errorf("%s=%s: error does not name a variable: %v", k, v, err)
		}
	}
}

func TestServerIdentity(t *testing.T) {
	c, _ := LoadServer(getenv(map[string]string{EnvName: "relay-1"}))
	if id := c.Identity("1.2.3"); id != (protocol.ServerInfo{Name: "relay-1", Version: "1.2.3"}) || c.NameDefaulted {
		t.Errorf("identity %+v defaulted=%v", id, c.NameDefaulted)
	}
}

// Both roles: an unset WSMON_NAME falls back to the host name, then
// "unknown"; WSMON_NAME never affects the client's config hash.
func TestNameDefaultsToHostname(t *testing.T) {
	defer func(h func() (string, error)) { hostname = h }(hostname)
	for _, tc := range []struct {
		host func() (string, error)
		want string
	}{
		{func() (string, error) { return "wsmon-7d9f8c6b5-xk2mn", nil }, "wsmon-7d9f8c6b5-xk2mn"},
		{func() (string, error) { return "", errors.New("no host name") }, "unknown"},
	} {
		hostname = tc.host
		s, _ := LoadServer(getenv(nil))
		c, _ := LoadClient(getenv(minimal()))
		if s.Name != tc.want || !s.NameDefaulted || c.Name != tc.want || !c.NameDefaulted {
			t.Errorf("server %q/%v, client %q/%v, want %q", s.Name, s.NameDefaulted, c.Name, c.NameDefaulted, tc.want)
		}
	}
	named := minimal()
	named[EnvName] = "probe-1"
	c, _ := LoadClient(getenv(named))
	d, _ := LoadClient(getenv(minimal()))
	if c.Name != "probe-1" || c.NameDefaulted || string(c.HashInput()) != string(d.HashInput()) {
		t.Errorf("named client %q/%v; hash depends on name: %v", c.Name, c.NameDefaulted, string(c.HashInput()) != string(d.HashInput()))
	}
}

func TestServerLabels(t *testing.T) {
	c, err := LoadServer(getenv(map[string]string{EnvLabels: "zone=east-1a, node=n1"}))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Labels, map[string]string{"zone": "east-1a", "node": "n1"}) {
		t.Errorf("labels %v", c.Labels)
	}
	if got := c.LabelsString(); got != "node=n1,zone=east-1a" {
		t.Errorf("LabelsString %q", got)
	}
	for _, name := range []string{"name", "version", "type", "code", "close_code"} {
		if _, err := LoadServer(getenv(map[string]string{EnvLabels: name + "=x"})); err == nil {
			t.Errorf("reserved label %q accepted", name)
		}
	}
}
