package integration

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/test/server/wstest"
)

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

// serverBinary builds cmd/wsmon once per test run.
func serverBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		goBin, err := exec.LookPath("go")
		if err != nil {
			buildErr = err
			return
		}
		dir, err := os.MkdirTemp("", "wsmon-server-bin")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(dir, "wsmon-server")
		out, err := exec.Command(goBin, "build", "-o", binPath, "github.com/asappinc-opensource/wsmon/cmd/wsmon").CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("%v: %s", err, out)
		}
	})
	if buildErr != nil {
		t.Skipf("cannot build server binary: %v", buildErr)
	}
	return binPath
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

type proc struct {
	cmd      *exec.Cmd
	wsURL    string
	adminURL string
	logs     *bytes.Buffer
	exited   chan error
}

func startProcess(t *testing.T, extraEnv ...string) *proc {
	t.Helper()
	bin := serverBinary(t)
	wsAddr, adminAddr := freePort(t), freePort(t)
	cmd := exec.Command(bin, "server")
	cmd.Env = append([]string{
		"WSMON_LISTEN_ADDR=" + wsAddr,
		"WSMON_METRICS_ADDR=" + adminAddr,
		"WSMON_NAME=proc-test",
	}, extraEnv...)
	logs := &bytes.Buffer{}
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{cmd: cmd, wsURL: "ws://" + wsAddr + "/__synthetic/ws", adminURL: "http://" + adminAddr, logs: logs, exited: make(chan error, 1)}
	go func() { p.exited <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		if t.Failed() {
			t.Logf("server logs:\n%s", logs)
		}
	})
	eventually(t, 10*time.Second, func() bool {
		resp, err := http.Get(p.adminURL + "/readyz")
		return err == nil && resp.StatusCode == http.StatusOK
	}, "server process never became ready")
	return p
}

func (p *proc) connect(t *testing.T) *wstest.Client {
	t.Helper()
	c, err := wstest.Dial(ctxT(t, 5*time.Second), p.wsURL, wstest.DialOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Hello(ctxT(t, 5*time.Second), wstest.ClientSkyEdge, protocol.Options{}); err != nil {
		t.Fatal(err)
	}
	return c
}

// TC-INT-04 against the real binary: SIGTERM fails /readyz at once, then
// after drain_delay every connection gets error{shutting_down} followed by a
// 1001 close frame, and the process exits cleanly. No 1006.
func TestInt04SIGTERMDrain(t *testing.T) {
	p := startProcess(t, "WSMON_DRAIN_DELAY=500ms", "WSMON_SHUTDOWN_TIMEOUT=10s")
	clients := []*wstest.Client{p.connect(t), p.connect(t), p.connect(t)}

	sent := time.Now()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	eventually(t, 400*time.Millisecond, func() bool {
		resp, err := http.Get(p.adminURL + "/readyz")
		return err == nil && resp.StatusCode == http.StatusServiceUnavailable
	}, "/readyz still passing after SIGTERM")

	// Connections keep working during drain_delay.
	expectOpen(t, clients[0])

	for _, c := range clients {
		expectError(t, c, protocol.ErrShuttingDown)
		expectClose(t, c, protocol.CloseGoingAway)
		assertErrorBeforeClose(t, c, protocol.CloseGoingAway)
	}
	if el := time.Since(sent); el < 500*time.Millisecond {
		t.Errorf("connections closed after %v, before drain_delay", el)
	}
	select {
	case err := <-p.exited:
		if err != nil {
			t.Errorf("exit: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("process did not exit within shutdown_timeout")
	}
}

// TC-INT-05: a SIGKILLed server sends no close frame, so the client sees an
// abnormal closure (1006). This is the failure the server must otherwise
// never manufacture.
func TestInt05SIGKILLIsAbnormal(t *testing.T) {
	p := startProcess(t)
	c := p.connect(t)
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, err := c.Recv(ctxT(t, 5*time.Second))
	if err == nil {
		t.Fatal("read succeeded after SIGKILL")
	}
	if code := websocket.CloseStatus(err); code != -1 {
		t.Fatalf("got close frame %d; want abnormal closure (1006, no close frame)", code)
	}
	frames, _ := c.Capture.FromServer()
	for _, f := range frames {
		if f.Opcode == wstest.OpClose {
			t.Fatal("close frame on the wire after SIGKILL")
		}
	}
}
