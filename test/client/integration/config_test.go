package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// probeBin is the real wsmon-probe binary, built once in TestMain.
var probeBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "wsmon-probe-it")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	probeBin = filepath.Join(dir, "wsmon-probe")
	build := exec.Command("go", "build", "-o", probeBin, "github.com/asappinc-opensource/wsmon/cmd/wsmon")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building wsmon-probe: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

const (
	canary   = "s3cr3t-canary"
	canaryRW = "s3cr3t-canary-rw"
)

// baseEnv is a minimal valid configuration. Secrets are always set so every
// row can assert that none of them leaks.
func baseEnv() map[string]string {
	return map[string]string{
		"WSMON_LABELS":                "site=local",
		"WSMON_URL":                   "ws://127.0.0.1:1/__synthetic/ws",
		"WSMON_METRICS_ADDR":          "127.0.0.1:0",
		"WSMON_AUTH_TOKEN":            canary,
		"WSMON_REMOTE_WRITE_PASSWORD": canaryRW,
	}
}

func envList(m map[string]string) []string {
	out := []string{"PATH=/usr/bin:/bin"}
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return out
}

func mutate(kv ...string) map[string]string {
	m := baseEnv()
	for i := 0; i < len(kv); i += 2 {
		if kv[i+1] == "<unset>" {
			delete(m, kv[i])
		} else {
			m[kv[i]] = kv[i+1]
		}
	}
	return m
}

// One row per validation rule plus the derived rules.
var invalidConfigs = []struct {
	name string
	env  map[string]string
	want string
}{
	{"1 required unset", mutate("WSMON_URL", "<unset>"), "WSMON_URL"},
	{"1 required empty", mutate("WSMON_URL", ""), "WSMON_URL"},
	{"2 mode", mutate("WSMON_MODE", "push"), "WSMON_MODE"},
	{"3 scheme", mutate("WSMON_URL", "https://127.0.0.1/__synthetic/ws"), "WSMON_URL"},
	{"labels", mutate("WSMON_LABELS", "site"), "WSMON_LABELS"},
	{"3 insecure with ws", mutate("WSMON_INSECURE_SKIP_VERIFY", "true"), "WSMON_INSECURE_SKIP_VERIFY"},
	{"4 push outside pingpong", mutate("WSMON_MODE", "idle", "WSMON_PUSH_INTERVAL_MS", "1000"), "WSMON_PUSH_INTERVAL_MS"},
	{"5 burst size", mutate("WSMON_BURST_SIZE", "101"), "WSMON_BURST_SIZE"},
	{"5 large payload", mutate("WSMON_LARGE_PAYLOAD_BYTES", "524289"), "WSMON_LARGE_PAYLOAD_BYTES"},
	{"6 payloads", mutate("WSMON_PAYLOADS", "medium"), "WSMON_PAYLOADS"},
	{"6 ladder", mutate("WSMON_IDLE_LADDER", "60s,30s"), "WSMON_IDLE_LADDER"},
	{"7 pingpong rate", mutate("WSMON_PINGPONG_INTERVAL", "1ms"), "WSMON_PINGPONG_INTERVAL"},
	{"7 message timeout", mutate("WSMON_MESSAGE_TIMEOUT", "30s"), "WSMON_MESSAGE_TIMEOUT"},
	{"8 parse", mutate("WSMON_CONNECT_TIMEOUT", "fifteen"), "WSMON_CONNECT_TIMEOUT"},
	{"9 rw auth exclusive", mutate("WSMON_REMOTE_WRITE_BEARER_TOKEN", canary, "WSMON_REMOTE_WRITE_USERNAME", "u"), "WSMON_REMOTE_WRITE_BEARER_TOKEN"},
	{"10 sample ratio", mutate("WSMON_TRACE_SAMPLE_RATIO", "2"), "WSMON_TRACE_SAMPLE_RATIO"},
	{"10 log level", mutate("WSMON_LOG_LEVEL", "verbose"), "WSMON_LOG_LEVEL"},
	{"11 zero duration", mutate("WSMON_BACKOFF_BASE", "0s"), "WSMON_BACKOFF_BASE"},
	{"11 negative push", mutate("WSMON_PUSH_INTERVAL_MS", "-5"), "WSMON_PUSH_INTERVAL_MS"},
	{"12 resolve override", mutate("WSMON_RESOLVE_OVERRIDE", "other.example.com:10.0.0.1"), "WSMON_RESOLVE_OVERRIDE"},
	{"13 burst rate", mutate("WSMON_MODE", "burst", "WSMON_BURST_SIZE", "100", "WSMON_BURST_INTERVAL", "100ms"), "WSMON_BURST_INTERVAL"},
	{"16 reserved header", mutate("WSMON_HEADERS", "Sec-WebSocket-Protocol=x"), "WSMON_HEADERS"},
	{"16 url param duplicate", mutate("WSMON_URL_PARAMS", "a=1,a=2"), "WSMON_URL_PARAMS"},
	{"14 interval in idle", mutate("WSMON_MODE", "idle", "WSMON_PINGPONG_INTERVAL", "1ms"), "WSMON_PINGPONG_INTERVAL"},
}

// TC-INT-16: each invalid configuration exits non-zero within 2 s with
// exactly one clear JSON log line at ERROR naming the variable, and no
// secret in it.
func TestTCINT16_InvalidConfig(t *testing.T) {
	for _, tc := range invalidConfigs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, probeBin, "client")
			cmd.Env = envList(tc.env)
			out, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("still running after 2 s; output:\n%s", out)
			}
			var ee *exec.ExitError
			if !errors.As(err, &ee) || ee.ExitCode() == 0 {
				t.Fatalf("exit: %v (want non-zero); output:\n%s", err, out)
			}
			lines := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
			if len(lines) != 1 {
				t.Fatalf("want exactly one line, got %d:\n%s", len(lines), out)
			}
			var rec map[string]any
			if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
				t.Fatalf("not JSON: %v\n%s", err, lines[0])
			}
			if rec["level"] != "ERROR" {
				t.Errorf("level %v", rec["level"])
			}
			if !strings.Contains(lines[0], tc.want) {
				t.Errorf("line does not name %s: %s", tc.want, lines[0])
			}
			if strings.Contains(lines[0], canary) {
				t.Errorf("secret leaked: %s", lines[0])
			}
		})
	}
}

// startProbe runs the binary with env, lets it run for d, then SIGTERMs it.
// It returns the combined output and requires a clean exit 0.
func startProbe(t *testing.T, env map[string]string, d time.Duration) string {
	t.Helper()
	var buf bytes.Buffer
	cmd := exec.Command(probeBin, "client")
	cmd.Env = envList(env)
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		t.Fatalf("exited early (%v):\n%s", err, buf.String())
	case <-time.After(d):
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("exit after SIGTERM: %v\n%s", err, buf.String())
		}
	case <-time.After(5 * time.Second):
		cmd.Process.Kill()
		t.Fatalf("did not exit after SIGTERM:\n%s", buf.String())
	}
	return buf.String()
}

// TC-INT-16 valid row, and TC-WIRE-16's "token never appears in logs" half.
func TestTCINT16_ValidConfigRuns(t *testing.T) {
	out := startProbe(t, baseEnv(), 500*time.Millisecond)
	if strings.Contains(out, canary) {
		t.Errorf("secret leaked into startup output:\n%s", out)
	}
	var sawConfig bool
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("non-JSON log line: %s", line)
		}
		if rec["msg"] == "configuration" {
			sawConfig = true
			if rec["WSMON_AUTH_TOKEN"] != "[redacted]" || rec["WSMON_REMOTE_WRITE_PASSWORD"] != "[redacted]" {
				t.Errorf("secrets not redacted: %s", line)
			}
			if rec["WSMON_LABELS"] != "site=local" || rec["WSMON_MODE"] != "pingpong" || rec["WSMON_BURST_SIZE"] != "20" {
				t.Errorf("resolved config missing: %s", line)
			}
		}
	}
	if !sawConfig {
		t.Errorf("no configuration log line:\n%s", out)
	}
}
