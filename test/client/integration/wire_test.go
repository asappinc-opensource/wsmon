package integration

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/test/client/fakeserver"
	"github.com/asappinc-opensource/wsmon/test/client/wirecap"
)

// TC-WIRE-10: upgrade completes without wsmon.v1 → reason subprotocol, and the
// client closes with a 1002 close frame (never a bare drop).
func TestTCWIRE10_Subprotocol(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{OmitSubprotocol: true})
	h := newHarness(t, probeEnv(srv.URL))
	h.WaitCount("connect_failures{subprotocol}", 1, 5*time.Second)
	h.WaitCount("closes{1002,client}", 1, 5*time.Second)
	c := srv.Upgraded()[0]
	<-c.Done()
	if got := c.CloseCodeReceived(); got != protocol.CloseProtocolError {
		t.Errorf("server received close code %d, want 1002", got)
	}
	if h.rec.Gauge("connection_up") != 0 || h.Count("connect_failures{other}") != 0 {
		t.Errorf("recorder:\n%s", h.rec.Dump())
	}
	if c.Hello() != nil {
		t.Error("hello sent on a connection without wsmon.v1")
	}
}

// Not a TC: 101 with a bad Sec-WebSocket-Accept → upgrade_rejected.
func TestUpgradeRejected(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{BadAccept: true})
	h := newHarness(t, probeEnv(srv.URL))
	h.WaitCount("connect_failures{upgrade_rejected}", 1, 5*time.Second)
}

// TC-WIRE-12: the large probe on the wire is exactly LARGE_PAYLOAD_BYTES; its
// echo is processed and labelled large.
func TestTCWIRE12_LargePayload(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	h := newHarness(t, probeEnv(srv.URL, "WSMON_PAYLOADS", "large"))
	h.WaitCount("echoed{large}", 3, 5*time.Second)
	h.Stop()
	n := 0
	for _, cp := range h.Caps() {
		to, _, err := cp.Frames()
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range to {
			if f.Opcode == wirecap.OpText && frameType(f) == protocol.TypeProbe {
				n++
				if len(f.Payload) != h.cfg.LargePayloadBytes || f.RSV1 {
					t.Errorf("probe frame %d bytes (rsv1=%v), want %d", len(f.Payload), f.RSV1, h.cfg.LargePayloadBytes)
				}
			}
		}
	}
	if n < 3 || h.Count("sent{small}") != 0 || len(h.rec.Obs("rtt{large}")) < 3 {
		t.Errorf("probes on wire=%d\n%s", n, h.rec.Dump())
	}
}

// TC-WIRE-16: Authorization on every (re)connect; the token never appears in
// logs.
func TestTCWIRE16_AuthHeader(t *testing.T) {
	const token = "tok-CANARY-9f8e7d"
	srv := fakeserver.New(t, fakeserver.Behaviour{
		Token:   token,
		OnHello: func(c *fakeserver.Conn) { time.Sleep(20 * time.Millisecond); c.Kill() },
	})
	h := newHarness(t, probeEnv(srv.URL, "WSMON_AUTH_TOKEN", token, "WSMON_LOG_LEVEL", "debug"))
	h.Wait("3 upgraded connections", func() bool { return len(srv.Upgraded()) >= 3 }, 10*time.Second)
	h.Stop()
	for _, c := range srv.Conns() {
		if got := c.Header.Get("Authorization"); got != "Bearer "+token {
			t.Errorf("conn %d: Authorization %q", c.Index, got)
		}
	}
	if strings.Contains(h.logs.String(), token) {
		t.Errorf("token leaked into logs")
	}
	if h.Count("connect_failures{auth}") != 0 {
		t.Errorf("auth failures: %s", h.rec.Dump())
	}
}

// TC-WIRE-17: accepted_options differ → close 1002, options_rejected, WARN.
func TestTCWIRE17_OptionsRejected(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{
		AcceptedOptions: func(r protocol.Options) protocol.Options { r.ServerKeepalive = true; return r },
	})
	h := newHarness(t, probeEnv(srv.URL))
	h.WaitCount("connect_failures{options_rejected}", 1, 5*time.Second)
	c := srv.Upgraded()[0]
	<-c.Done()
	if got := c.CloseCodeReceived(); got != protocol.CloseProtocolError {
		t.Errorf("close code %d, want 1002", got)
	}
	warns := h.logs.Find("WARN", "did not accept the requested options")
	if len(warns) == 0 || warns[0]["accepted_server_keepalive"] != true || warns[0]["requested_server_keepalive"] != false {
		t.Errorf("WARN missing or incomplete: %v", warns)
	}
	if h.Count("closes{1002,client}") < 1 || len(h.rec.Obs("connect_phase{hello}")) < 1 {
		t.Errorf("recorder:\n%s", h.rec.Dump())
	}
	if h.Count("sent{small}")+h.Count("sent{large}") != 0 {
		t.Error("probe sent on a rejected connection")
	}
}

// TC-WIRE-18: every error frame → WARN with code and message, counted by
// code; unknown codes pass through. The connection stays open.
func TestTCWIRE18_ErrorFrames(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{
		OnHello: func(c *fakeserver.Conn) {
			_ = c.SendError(protocol.ErrRateLimited, 7, "slow down")
			_ = c.SendError("brand_new_code", 0, "from the future")
		},
	})
	h := newHarness(t, probeEnv(srv.URL))
	h.WaitCount("server_errors{rate_limited}", 1, 5*time.Second)
	h.WaitCount("server_errors{brand_new_code}", 1, 5*time.Second)
	h.WaitCount("echoed{small}", 2, 5*time.Second)
	warns := h.logs.Find("WARN", "error frame")
	var sawKnown, sawUnknown bool
	for _, w := range warns {
		sawKnown = sawKnown || (w["code"] == "rate_limited" && w["message"] == "slow down")
		sawUnknown = sawUnknown || (w["code"] == "brand_new_code" && w["message"] == "from the future")
	}
	if !sawKnown || !sawUnknown {
		t.Errorf("WARNs: %v", warns)
	}
	if h.Count("reconnects") != 0 {
		t.Error("error frame closed the connection")
	}
}

// An error frame (or anything else) before hello_ack → reason other.
func TestErrorBeforeHelloAck(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{
		BeforeHelloAck: func(c *fakeserver.Conn) { _ = c.SendError(protocol.ErrRateLimited, 0, "early") },
		NoHelloAck:     true,
	})
	h := newHarness(t, probeEnv(srv.URL))
	h.WaitCount("connect_failures{other}", 1, 5*time.Second)
	h.WaitCount("server_errors{rate_limited}", 1, time.Second)
}

// hello_ack with the wrong conn_id → other (not covered by the spec).
func TestHelloAckConnIDMismatch(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{HelloAckConnID: "01AAAAAAAAAAAAAAAAAAAAAAAA"})
	h := newHarness(t, probeEnv(srv.URL))
	h.WaitCount("connect_failures{other}", 1, 5*time.Second)
	if len(h.logs.Find("WARN", "conn_id does not match")) == 0 {
		t.Error("no WARN")
	}
}

// No hello_ack within CONNECT_TIMEOUT → timeout, closed with 1000.
func TestHelloAckTimeout(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{NoHelloAck: true})
	h := newHarness(t, probeEnv(srv.URL, "WSMON_CONNECT_TIMEOUT", "300ms"))
	h.WaitCount("connect_failures{timeout}", 1, 5*time.Second)
	c := srv.Upgraded()[0]
	<-c.Done()
	if got := c.CloseCodeReceived(); got != protocol.CloseNormal {
		t.Errorf("close code %d, want 1000", got)
	}
}

// TC-WIRE-21: hello is the first frame; no probe before hello_ack; seq
// starts at 1 and increments by one.
func TestTCWIRE21_HelloFirst(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{HelloAckDelay: 300 * time.Millisecond})
	h := newHarness(t, probeEnv(srv.URL))
	h.WaitCount("echoed{small}", 2, 5*time.Second)
	h.Stop()
	c := srv.Upgraded()[0]
	frames := c.Frames()
	if len(frames) == 0 || frames[0].Type != protocol.TypeHello {
		t.Fatalf("first frame: %v", frames)
	}
	ackAt := c.HelloAckAt()
	for _, f := range frames[1:] {
		if f.Type == protocol.TypeHello {
			t.Error("second hello")
		}
		if f.Type == protocol.TypeProbe && f.At.Before(ackAt) {
			t.Errorf("probe at %v before hello_ack at %v", f.At, ackAt)
		}
	}
	for i, p := range c.Probes() {
		if p.Seq != int64(i+1) || p.ConnID != c.ConnID() {
			t.Errorf("probe %d: seq %d conn_id %s", i, p.Seq, p.ConnID)
		}
	}
	to, _, err := h.Caps()[0].Frames()
	if err != nil || len(to) == 0 || frameType(to[0]) != protocol.TypeHello {
		t.Errorf("client capture: first frame %v (%v)", to, err)
	}
	var hello map[string]json.RawMessage
	_ = json.Unmarshal(to[0].Payload, &hello)
	if !protocol.ValidULID(strings.Trim(string(hello["conn_id"]), `"`)) {
		t.Errorf("conn_id not a ULID: %s", hello["conn_id"])
	}
}
