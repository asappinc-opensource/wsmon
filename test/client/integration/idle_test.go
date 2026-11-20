package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/test/client/fakeserver"
	"github.com/asappinc-opensource/wsmon/test/client/wirecap"
)

// The spec ladder 30,60,90,120,300,600 s scaled by 1/30 (acceptance) and a
// further 1/10 (-short), preserving the ratios.
func ladder() string {
	if testing.Short() {
		return "100ms,200ms,300ms,400ms,1s,2s"
	}
	return "1s,2s,3s,4s,10s,20s"
}

func idleEnv(url string, kv ...string) map[string]string {
	return probeEnv(url, append([]string{
		"WSMON_MODE", "idle",
		"WSMON_IDLE_LADDER", ladder(),
		"WSMON_MESSAGE_TIMEOUT", D(500*time.Millisecond, 80*time.Millisecond).String(),
	}, kv...)...)
}

func buckets(h *harness) []string {
	var out []string
	for _, r := range h.cfg.IdleLadder {
		out = append(out, config.IdleBucketLabel(r))
	}
	return out
}

func idleKey(bucket, result string) string { return "idle_probe{" + bucket + "," + result + "}" }

// TC-IDLE-01: no intermediary → every rung survives and the ladder restarts
// on the same connection.
func TestTCIDLE01_LadderSurvives(t *testing.T) {
	parallelLong(t)
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	h := newHarness(t, idleEnv(srv.URL))
	b := buckets(h)
	h.WaitCount(idleKey(b[0], "survived"), 2, D(90*time.Second, 15*time.Second))
	h.Stop()
	for _, bk := range b {
		if h.Count(idleKey(bk, "survived")) < 1 {
			t.Errorf("rung %s never survived", bk)
		}
	}
	if h.rec.SumPrefix("idle_close") != 0 || len(srv.Upgraded()) != 1 || h.Count("reconnects") != 0 {
		t.Errorf("ladder did not restart on the same connection\n%s", h.rec.Dump())
	}
	ps := srv.Upgraded()[0].Probes()
	if len(ps) < len(b)+1 || ps[len(b)].Seq != int64(len(b)+1) {
		t.Errorf("probes on the connection: %d", len(ps))
	}
}

// TC-IDLE-06: the full ladder survives repeatedly → the top bucket keeps
// incrementing.
func TestTCIDLE06_TopRungRepeats(t *testing.T) {
	parallelLong(t)
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	h := newHarness(t, idleEnv(srv.URL))
	b := buckets(h)
	top := idleKey(b[len(b)-1], "survived")
	h.WaitCount(top, 2, D(150*time.Second, 25*time.Second))
	if len(srv.Upgraded()) != 1 {
		t.Errorf("reconnected: %d connections", len(srv.Upgraded()))
	}
}

// TC-IDLE-02: a proxy idle timeout between rungs 0 and 1 → rung 0 survives,
// rung 1 drops with 1006, and higher rungs get zero observations.
func TestTCIDLE02_ProxyIdleTimeout(t *testing.T) {
	parallelLong(t)
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	px := fakeserver.NewIdleProxy(t, srv.Addr, D(1500*time.Millisecond, 150*time.Millisecond))
	h := newHarness(t, idleEnv("ws://"+px.Addr+fakeserver.Path))
	b := buckets(h)
	h.WaitCount(idleKey(b[1], "dropped"), 2, D(60*time.Second, 10*time.Second))
	h.Stop()
	if h.Count(idleKey(b[0], "survived")) < 2 || h.Count(idleKey(b[1], "survived")) != 0 || h.Count(idleKey(b[0], "dropped")) != 0 {
		t.Errorf("recorder:\n%s", h.rec.Dump())
	}
	if h.Count("idle_close{"+b[1]+",1006}") != h.Count(idleKey(b[1], "dropped")) {
		t.Errorf("close code not recorded as 1006:\n%s", h.rec.Dump())
	}
	for _, bk := range b[2:] {
		for _, k := range []string{idleKey(bk, "survived"), idleKey(bk, "dropped")} {
			if h.Count(k) != 0 {
				t.Errorf("%s = %v: rungs above the drop must have zero observations", k, h.Count(k))
			}
		}
	}
	if px.Drops() < 2 {
		t.Errorf("proxy drops %d", px.Drops())
	}
}

// TC-IDLE-03: across ≥ 1 reconnect and a full 120 s rung of silence (2 s
// under -short), zero ping or pong frames from either end on every
// connection, captured on the wire at both the client and the server, and
// zero client writes of any kind during each silence.
func TestTCIDLE03_NoPingOnTheWire(t *testing.T) {
	parallelLong(t)
	rungs := []time.Duration{D(5*time.Second, 300*time.Millisecond), D(120*time.Second, 2*time.Second)}
	srv := fakeserver.New(t, fakeserver.Behaviour{OnProbe: func(c *fakeserver.Conn, p fakeserver.Probe) {
		_ = c.Echo(p)
		if c.Index == 0 {
			// Force one reconnect after the first rung.
			go func() { time.Sleep(50 * time.Millisecond); c.Kill() }()
		}
	}})
	h := newHarness(t, probeEnv(srv.URL,
		"WSMON_MODE", "idle",
		"WSMON_IDLE_LADDER", rungs[0].String()+","+rungs[1].String(),
		"WSMON_MESSAGE_TIMEOUT", D(2*time.Second, 200*time.Millisecond).String()))
	b := buckets(h)
	h.WaitCount(idleKey(b[1], "survived"), 1, D(200*time.Second, 20*time.Second))
	h.Stop()

	caps := h.Caps()
	conns := srv.Upgraded()
	if len(caps) < 2 || len(conns) < 2 {
		t.Fatalf("want ≥ 2 connections, got client %d server %d", len(caps), len(conns))
	}
	check := func(side string, i int, cp *wirecap.Conn) {
		to, from, err := cp.Frames()
		if err != nil {
			t.Fatalf("%s conn %d: %v", side, i, err)
		}
		if p := append(wirecap.Pings(to), wirecap.Pings(from)...); len(p) > 0 {
			t.Errorf("%s conn %d: %d ping/pong frames on the wire\nto peer:\n%s\nfrom peer:\n%s", side, i, len(p), dump(to), dump(from))
		}
		if len(to) == 0 || len(from) == 0 {
			t.Errorf("%s conn %d: empty capture", side, i)
		}
	}
	for i, cp := range caps {
		check("client", i, cp)
	}
	for _, c := range conns {
		check("server", c.Index, c.Cap)
	}

	// Silence: per connection, the k-th probe follows the previous client
	// write by at least rung k, and the client wrote nothing but hello,
	// probes and its final close.
	silence := time.Duration(0)
	for i, cp := range caps {
		wf, err := cp.WrittenFrames()
		if err != nil {
			t.Fatal(err)
		}
		k := 0
		for j, f := range wf {
			switch {
			case f.Opcode == wirecap.OpClose:
				continue
			case f.Opcode != wirecap.OpText:
				t.Errorf("client conn %d: unexpected opcode 0x%x", i, f.Opcode)
				continue
			}
			switch typ := frameType(f.Frame); typ {
			case protocol.TypeHello:
			case protocol.TypeProbe:
				rung := rungs[k%len(rungs)]
				gap := f.At.Sub(wf[j-1].At)
				if gap < rung-time.Millisecond {
					t.Errorf("client conn %d probe %d: previous write %v earlier, rung %v", i, k, gap, rung)
				}
				silence = max(silence, gap)
				k++
			default:
				t.Errorf("client conn %d: wrote a %q frame", i, typ)
			}
		}
	}
	if silence < rungs[1] {
		t.Errorf("longest observed silence %v < %v", silence, rungs[1])
	}
	t.Logf("TC-IDLE-03: %d client + %d server connections captured, longest silence %v, 0 ping/pong", len(caps), len(conns), silence)
}

func dump(fs []wirecap.Frame) string {
	var b strings.Builder
	for _, f := range fs {
		fmt.Fprintln(&b, f.String())
	}
	return b.String()
}

// TC-IDLE-05: a drop at the "120" rung (index 3) → the ladder restarts at
// rung 0 on a fresh conn_id.
func TestTCIDLE05_RestartOnFreshConn(t *testing.T) {
	parallelLong(t)
	srv := fakeserver.New(t, fakeserver.Behaviour{
		IdleTimeout:   D(3500*time.Millisecond, 350*time.Millisecond),
		IdleCloseCode: protocol.CloseInternalError,
	})
	h := newHarness(t, idleEnv(srv.URL))
	b := buckets(h)
	h.WaitCount(idleKey(b[3], "dropped"), 1, D(60*time.Second, 10*time.Second))
	h.WaitCount(idleKey(b[0], "survived"), 2, D(30*time.Second, 5*time.Second))
	h.Stop()
	if h.Count("idle_close{"+b[3]+",1011}") < 1 {
		t.Errorf("close code not recorded:\n%s", h.rec.Dump())
	}
	for _, bk := range b[4:] {
		if h.Count(idleKey(bk, "survived"))+h.Count(idleKey(bk, "dropped")) != 0 {
			t.Errorf("bucket %s observed above the drop", bk)
		}
	}
	cs := srv.Upgraded()
	if len(cs) < 2 || cs[0].ConnID() == cs[1].ConnID() {
		t.Fatalf("conn_ids %v", connIDs(cs))
	}
	ps := cs[1].Probes()
	if len(ps) == 0 || ps[0].Seq != 1 {
		t.Fatalf("second connection probes %v", ps)
	}
	var first time.Time
	for _, f := range cs[1].Frames() {
		if f.Type == protocol.TypeProbe {
			first = f.At
			break
		}
	}
	if gap := first.Sub(cs[1].HelloAckAt()); gap < h.cfg.IdleLadder[0] {
		t.Errorf("first probe on the new connection after %v, want ≥ rung 0 (%v)", gap, h.cfg.IdleLadder[0])
	}
}

// TC-IDLE-07: the idle hello's options are exactly {false, 0}.
func TestTCIDLE07_HelloOptions(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	h := newHarness(t, idleEnv(srv.URL))
	h.Wait("connected", func() bool { return h.rec.Gauge("connection_up") == 1 }, 5*time.Second)
	h.Stop()
	to, _, err := h.Caps()[0].Frames()
	if err != nil || len(to) == 0 {
		t.Fatalf("capture: %v", err)
	}
	var hello struct {
		Type    string                     `json:"type"`
		Client  protocol.ClientInfo        `json:"client"`
		Options map[string]json.RawMessage `json:"options"`
	}
	if err := json.Unmarshal(to[0].Payload, &hello); err != nil || hello.Type != protocol.TypeHello {
		t.Fatalf("first frame %s (%v)", to[0].Payload, err)
	}
	if len(hello.Options) != 2 || string(hello.Options["server_keepalive"]) != "false" || string(hello.Options["push_interval_ms"]) != "0" {
		t.Errorf("options = %s", to[0].Payload)
	}
	if hello.Client.Mode != "idle" {
		t.Errorf("client.mode %q", hello.Client.Mode)
	}
}

// TC-IDLE-08: pingpong and burst, 120 s of capture (2 s under -short): the
// client sends zero ping frames, and none cross the wire.
func TestTCIDLE08_NoPingOtherModes(t *testing.T) {
	parallelLong(t)
	for _, mode := range []string{"pingpong", "burst"} {
		t.Run(mode, func(t *testing.T) {
			parallelLong(t)
			srv := fakeserver.New(t, fakeserver.Behaviour{})
			h := newHarness(t, probeEnv(srv.URL, "WSMON_MODE", mode, "WSMON_PINGPONG_INTERVAL", "1s", "WSMON_BURST_INTERVAL", "5s"))
			time.Sleep(D(120*time.Second, 2*time.Second))
			h.Stop()
			if h.Count("echoed{small}") == 0 {
				t.Fatalf("no traffic:\n%s", h.rec.Dump())
			}
			for i, cp := range h.Caps() {
				to, from, err := cp.Frames()
				if err != nil {
					t.Fatal(err)
				}
				if p := append(wirecap.Pings(to), wirecap.Pings(from)...); len(p) > 0 {
					t.Errorf("conn %d: %d ping/pong frames\n%s", i, len(p), dump(to))
				}
			}
		})
	}
}

// Negative control for TC-IDLE-03: a server that pings is caught by the
// same wire check (ping from the peer, pong back), and the client WARNs.
// Proves the zero-ping assertions above can fail.
func TestTCIDLE03_DetectorSeesPings(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{OnHello: func(c *fakeserver.Conn) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.WS().Ping(ctx) // test-only misbehaviour: the real server must never do this
	}})
	h := newHarness(t, idleEnv(srv.URL))
	h.Wait("ping WARN", func() bool { return len(h.logs.Find("WARN", "received WebSocket ping")) > 0 }, 5*time.Second)
	time.Sleep(50 * time.Millisecond)
	h.Stop()
	to, from, err := h.Caps()[0].Frames()
	if err != nil {
		t.Fatal(err)
	}
	if len(wirecap.Pings(from)) != 1 || len(wirecap.Pings(to)) != 1 {
		t.Fatalf("detector missed the ping/pong: from=%d to=%d", len(wirecap.Pings(from)), len(wirecap.Pings(to)))
	}
}

// TCP keepalive is off on the probe's socket, so no L4 hop sees keepalive
// segments during an idle rung.
func TestNoTCPKeepalive(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	var mu sync.Mutex
	vals := []int{}
	h := newHarness(t, idleEnv(srv.URL), func(o *probe.Options) {
		inner := o.WrapConn
		o.WrapConn = func(c net.Conn) net.Conn {
			if tc, ok := c.(*net.TCPConn); ok {
				rc, _ := tc.SyscallConn()
				_ = rc.Control(func(fd uintptr) {
					v, err := syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_KEEPALIVE)
					if err == nil {
						mu.Lock()
						vals = append(vals, v)
						mu.Unlock()
					}
				})
			}
			return inner(c)
		}
	})
	h.Wait("connected", func() bool { return h.rec.Gauge("connection_up") == 1 }, 5*time.Second)
	mu.Lock()
	defer mu.Unlock()
	if len(vals) != 1 || vals[0] != 0 {
		t.Errorf("SO_KEEPALIVE = %v, want [0]", vals)
	}
}
