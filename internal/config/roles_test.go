package config

import (
	"strconv"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// These tests hold the two roles' configurations to each other: a client
// configuration that loads must be one the default server accepts unchanged.

func TestSharedVarsAreIdenticalInBothRoles(t *testing.T) {
	server := map[string]Var{}
	for _, v := range ServerVars {
		server[v.Name] = v
	}
	n := 0
	for _, c := range ClientVars {
		s, ok := server[c.Name]
		if !ok {
			continue
		}
		n++
		if s.Help != c.Help || s.Secret != c.Secret {
			t.Errorf("%s is defined differently by the two roles: %+v vs %+v", c.Name, s, c)
		}
	}
	// Plus WSMON_LABELS: same syntax in both roles, but each role reserves
	// the label names it sets itself, so it is parsed per role, not in Common.
	if want := len(Common{}.values()) + 1; n != want {
		t.Errorf("%d variables shared, want %d (Common + %s)", n, want, EnvLabels)
	}
}

func TestVarTablesComplete(t *testing.T) {
	for role, tc := range map[string]struct {
		vars   []Var
		values func() []kv
	}{
		"server": {ServerVars, func() []kv { c, _ := LoadServer(getenv(nil)); return c.values() }},
		"client": {ClientVars, func() []kv { c, _ := LoadClient(getenv(minimal())); return c.values() }},
	} {
		names := map[string]bool{}
		for _, v := range tc.vars {
			if names[v.Name] {
				t.Errorf("%s: %s listed twice", role, v.Name)
			}
			names[v.Name] = true
			if v.Help == "" {
				t.Errorf("%s: %s has no help text", role, v.Name)
			}
		}
		vals := tc.values()
		if len(vals) != len(tc.vars) {
			t.Errorf("%s: %d values rendered for %d variables", role, len(vals), len(tc.vars))
		}
		for _, v := range vals {
			if !names[v.name] {
				t.Errorf("%s: value for %s, which is not in the Var table", role, v.name)
			}
		}
	}
}

// TestClientPushIntervalMatchesServer: the client accepts exactly the push
// intervals the server honours as-is.
func TestClientPushIntervalMatchesServer(t *testing.T) {
	for _, ms := range []int64{1, 50, 99, 100, 101, 1000, 3600000, 3600001} {
		req := protocol.Options{PushIntervalMs: ms}
		honoured := protocol.HonourOptions(req) == req
		_, err := LoadClient(getenv(with(EnvPushIntervalMs, strconv.FormatInt(ms, 10), EnvPingpongInterval, "1s")))
		if honoured != (err == nil) {
			t.Errorf("push_interval_ms=%d: server honours as-is %v, client config error %v", ms, honoured, err)
		}
	}
}

// TestClientRateWithinServerCap: the fastest client the config allows stays
// within the default server's per-connection cap.
func TestClientRateWithinServerCap(t *testing.T) {
	srv, err := LoadServer(getenv(nil))
	if err != nil {
		t.Fatal(err)
	}
	cap := float64(srv.MaxMsgsPerSec)
	fastest := time.Second / protocol.MaxMsgsPerSec
	if _, err := LoadClient(getenv(with(EnvPingpongInterval, fastest.String()))); err != nil {
		t.Fatalf("fastest pingpong rejected: %v", err)
	}
	if rate := float64(time.Second) / float64(fastest); rate > cap {
		t.Errorf("fastest pingpong is %v msg/s, server cap %v", rate, cap)
	}
	if _, err := LoadClient(getenv(with(EnvPingpongInterval, (fastest - time.Microsecond).String()))); err == nil {
		t.Error("pingpong faster than the server cap accepted")
	}
	if _, err := LoadClient(getenv(with(EnvBurstSize, "100", EnvBurstInterval, "499ms"))); err == nil {
		t.Error("burst above the server cap accepted")
	}
}

// TestLargestProbeFitsServerFrameLimit: a probe at the largest allowed size
// is under the default server's max_frame_bytes.
func TestLargestProbeFitsServerFrameLimit(t *testing.T) {
	srv, _ := LoadServer(getenv(nil))
	if int64(MaxLargePayloadBytes) >= srv.MaxFrameBytes {
		t.Errorf("largest probe %d bytes, server limit %d", MaxLargePayloadBytes, srv.MaxFrameBytes)
	}
	if _, err := LoadClient(getenv(with(EnvLargePayloadBytes, strconv.Itoa(MaxLargePayloadBytes)))); err != nil {
		t.Errorf("largest payload rejected: %v", err)
	}
}

// TestClientDefaultsMatchServerDefaults: the client's deflate default is
// "off" and the server's is "on"; that is fine because the server only
// negotiates when offered. Every other shared default must agree.
func TestClientDefaultsMatchServerDefaults(t *testing.T) {
	srv, _ := LoadServer(getenv(nil))
	cli, _ := LoadClient(getenv(minimal()))
	if srv.MetricsAddr != cli.MetricsAddr || srv.TraceSampleRatio != cli.TraceSampleRatio || srv.LogLevel != cli.LogLevel || srv.OTLPEndpoint != cli.OTLPEndpoint {
		t.Errorf("shared defaults differ:\nserver %+v\nclient %+v", srv.Common, cli.Common)
	}
	if cli.URL.Path != protocol.DefaultWSPath || srv.WSPath != protocol.DefaultWSPath {
		t.Errorf("ws path: client test URL %q, server %q", cli.URL.Path, srv.WSPath)
	}
}
