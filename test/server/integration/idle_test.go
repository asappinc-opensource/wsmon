package integration

import (
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/test/server/wstest"
)

// controlFrames counts ping, pong and close frames in each direction.
func controlFrames(t *testing.T, c *wstest.Client) (fromServer, toServer map[byte]int) {
	t.Helper()
	count := func(frames []wstest.Frame, err error) map[byte]int {
		if err != nil {
			t.Fatal(err)
		}
		m := map[byte]int{}
		for _, f := range frames {
			if f.Opcode >= wstest.OpClose {
				m[f.Opcode]++
			}
		}
		return m
	}
	return count(c.Capture.FromServer()), count(c.Capture.ToServer())
}

// TC-IDLE-03 — the highest-priority test in the suite. It inspects the raw
// byte stream, not library configuration: zero ping or pong frames in either
// direction during the silence, and the connection still works afterwards.
//
// The ping interval is shortened far below the silence so that any leaked
// keepalive would fire many times over.
func TestIdle03NoControlFramesDuringSilence(t *testing.T) {
	t.Parallel()
	silence := dur(120*time.Second, 3*time.Second)
	e := start(t, func(c *config.Server) { c.ServerPingInterval = 200 * time.Millisecond })
	c, ack := e.connect(wstest.ClientIdle, protocol.Options{ServerKeepalive: false})
	if ack.AcceptedOptions.ServerKeepalive {
		t.Fatal("server accepted keepalive that was not requested")
	}

	time.Sleep(silence)

	from, to := controlFrames(t, c)
	if from[wstest.OpPing]+from[wstest.OpPong]+to[wstest.OpPing]+to[wstest.OpPong] != 0 {
		t.Fatalf("control frames during %v of silence: server→client %v, client→server %v", silence, from, to)
	}
	if from[wstest.OpClose] != 0 {
		t.Fatalf("server closed an idle connection")
	}
	expectOpen(t, c)
}

// TC-IDLE-04: with server_keepalive=false the server sends no ping and
// applies no read deadline — not hello_timeout, not write_timeout, not the
// HTTP server's header timeout — over a long hold.
func TestIdle04NoPingNoReadDeadline(t *testing.T) {
	t.Parallel()
	hold := dur(10*time.Minute, 5*time.Second)
	e := start(t, func(c *config.Server) {
		c.HelloTimeout = 200 * time.Millisecond
		c.WriteTimeout = 200 * time.Millisecond
		c.ServerPingInterval = 200 * time.Millisecond
	})
	c, _ := e.connect(wstest.ClientIdle, protocol.Options{ServerKeepalive: false})

	time.Sleep(hold)

	from, _ := controlFrames(t, c)
	if from[wstest.OpPing] != 0 || from[wstest.OpClose] != 0 {
		t.Fatalf("server sent control frames during %v hold: %v", hold, from)
	}
	if got := e.srv.ActiveConnections(); got != 1 {
		t.Fatalf("active connections = %d after hold", got)
	}
	expectOpen(t, c)
}

// Positive control for the two tests above: when keepalive is requested the
// capture does see pings, so a zero count there is meaningful.
func TestIdleKeepaliveTrueDoesPing(t *testing.T) {
	e := start(t, func(c *config.Server) { c.ServerPingInterval = 100 * time.Millisecond })
	c, ack := e.connect(wstest.ClientIdle, protocol.Options{ServerKeepalive: true})
	if !ack.AcceptedOptions.ServerKeepalive {
		t.Fatal("keepalive not accepted")
	}
	// The client library answers pings only while reading.
	go func() { _, _ = c.Recv(ctxT(t, 2*time.Second)) }()
	eventually(t, 2*time.Second, func() bool {
		from, to := controlFrames(t, c)
		return from[wstest.OpPing] >= 3 && to[wstest.OpPong] >= 3
	}, "no keepalive pings observed on the wire")
}
