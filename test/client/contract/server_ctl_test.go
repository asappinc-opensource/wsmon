package contract

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// serverControl is how the suite reads the logs of, stops and starts the
// server under test. Two implementations:
//
//   - localServer (default): `wsmon server` built from this very commit and
//     run as a subprocess on loopback. Client and server come from one
//     module, so this is the contract at HEAD.
//   - dockerServer: a released server image driven through the Docker API,
//     selected by WSMON_CONTRACT_* (`make test-contract-image`). It tests
//     this client against a deployed server version, e.g. before a
//     mixed-version rollout.
type serverControl interface {
	Logs(n int) string
	// Stop is a graceful stop: SIGTERM, drain, exit. It returns once the
	// process has exited.
	Stop(t *testing.T)
	Start(t *testing.T)
}

var srv serverControl

// contractToken is the bearer token the local server requires.
const contractToken = "contract-test-token"

// setupServer selects the server under test. It reports false when the suite
// should be skipped.
func setupServer(t *testing.T) bool {
	if loadEnv() {
		srv = dockerServer{container: env.Container}
		return true
	}
	if _, ok := testExtra(); !ok {
		// TC-CONTRACT-11 needs the test-only probe field; without the tag it
		// would fail by design. `make test` runs this package with it.
		return false
	}
	srv = startLocalServer(t)
	return true
}

// ---------------------------------------------------------------------------
// Local `wsmon server` subprocess.

type localServer struct {
	bin  string
	env  []string
	mu   sync.Mutex
	logs *syncBuf
	cmd  *exec.Cmd
	done chan error
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func startLocalServer(t *testing.T) *localServer {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "wsmon")
	build := exec.Command("go", "build", "-o", bin, "github.com/asappinc-opensource/wsmon/cmd/wsmon")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	wsAddr, adminAddr := freeAddr(t), freeAddr(t)
	// The same settings `make test-contract-image` gives the server container.
	s := &localServer{bin: bin, logs: &syncBuf{}, env: []string{
		"WSMON_LISTEN_ADDR=" + wsAddr,
		"WSMON_METRICS_ADDR=" + adminAddr,
		"WSMON_AUTH_TOKEN=" + contractToken,
		"WSMON_DRAIN_DELAY=1s",
		"WSMON_SHUTDOWN_TIMEOUT=5s",
		"WSMON_NAME=contract-server",
		"WSMON_ENABLE_PERMESSAGE_DEFLATE=true",
		"WSMON_OTLP_ENDPOINT=" + otlpLocal,
		"WSMON_TRACE_SAMPLE_RATIO=1",
	}}
	env = contractEnv{
		URL:        "ws://" + wsAddr + "/__synthetic/ws",
		MetricsURL: "http://" + adminAddr + "/metrics",
		AdminBase:  "http://" + adminAddr,
		Token:      contractToken,
	}
	s.Start(t)
	t.Cleanup(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.cmd != nil {
			_ = s.cmd.Process.Kill()
			<-s.done
		}
	})
	return s
}

func (s *localServer) Start(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd := exec.Command(s.bin, "server")
	cmd.Env, cmd.Stdout, cmd.Stderr = s.env, s.logs, s.logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start wsmon server: %v", err)
	}
	s.cmd, s.done = cmd, make(chan error, 1)
	go func(done chan<- error) { done <- cmd.Wait() }(s.done)
	for deadline := time.Now().Add(20 * time.Second); !serverReady(); {
		if time.Now().After(deadline) {
			t.Fatalf("wsmon server not ready after 20s\n%s", s.Logs(30))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (s *localServer) Stop(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd == nil {
		return
	}
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-s.done:
		if err != nil {
			t.Errorf("wsmon server exit after SIGTERM: %v", err)
		}
	case <-time.After(20 * time.Second):
		_ = s.cmd.Process.Kill()
		<-s.done
		t.Errorf("wsmon server did not exit within 20s of SIGTERM")
	}
	s.cmd = nil
}

func (s *localServer) Logs(n int) string { return tail(s.logs.String(), n) }

// ---------------------------------------------------------------------------
// Released server image via the Docker Engine API over the mounted socket.

type dockerServer struct{ container string }

var dockerHTTP = &http.Client{
	Timeout: 90 * time.Second,
	Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", "/var/run/docker.sock")
	}},
}

func docker(method, path string) (int, []byte, error) {
	req, err := http.NewRequest(method, "http://docker"+path, nil)
	if err != nil {
		return 0, nil, err
	}
	resp, err := dockerHTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	var b bytes.Buffer
	_, err = b.ReadFrom(resp.Body)
	return resp.StatusCode, b.Bytes(), err
}

func (d dockerServer) Stop(t *testing.T) {
	t.Helper()
	if code, body, err := docker(http.MethodPost, "/containers/"+d.container+"/stop?t=20"); err != nil || code != http.StatusNoContent {
		t.Fatalf("docker stop: %d %v %s", code, err, body)
	}
}

func (d dockerServer) Start(t *testing.T) {
	t.Helper()
	if code, body, err := docker(http.MethodPost, "/containers/"+d.container+"/start"); err != nil || code != http.StatusNoContent {
		t.Fatalf("docker start: %d %v %s", code, err, body)
	}
}

// Logs returns the last n lines of the container's output, demultiplexing
// the Docker log stream (8-byte frame headers).
func (d dockerServer) Logs(n int) string {
	code, b, err := docker(http.MethodGet, fmt.Sprintf("/containers/%s/logs?stdout=1&stderr=1&tail=%d", d.container, n))
	if err != nil || code != 200 {
		return fmt.Sprintf("(server logs unavailable: %d %v)", code, err)
	}
	var out strings.Builder
	for len(b) >= 8 {
		size := int(binary.BigEndian.Uint32(b[4:8]))
		if 8+size > len(b) {
			break
		}
		out.Write(b[8 : 8+size])
		b = b[8+size:]
	}
	return out.String()
}

// serverLogs is the evidence hook used throughout the suite.
func serverLogs(n int) string {
	if srv == nil {
		return "(no server under test)"
	}
	return srv.Logs(n)
}
