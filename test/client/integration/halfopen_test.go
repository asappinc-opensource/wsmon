package integration

import (
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/test/client/fakeserver"
)

// Half-open detection: DEAD_AFTER = 4 × MESSAGE_TIMEOUT.
// These are its fake-server acceptance tests: (a) a
// half-open socket reconnects, (b) a stall shorter than DEAD_AFTER recovers
// on the same connection. TC-FAULT-05/10 repeat them against the real server.

const deadAfterFactor = 4

// closeTimeout mirrors probe.closeTimeout: a client close to a peer that no
// longer reads is abandoned after it.
const closeTimeout = 2 * time.Second

// TestHalfOpen_Reconnects is test (a): the server stops reading and writing
// but keeps the TCP connection open.
func TestHalfOpen_Reconnects(t *testing.T) {
	for _, mode := range []string{"pingpong", "burst"} {
		t.Run(mode, func(t *testing.T) {
			mt := D(1*time.Second, 500*time.Millisecond)
			deadAfter := deadAfterFactor * mt
			srv := fakeserver.New(t, fakeserver.Behaviour{})
			h := newHarness(t, probeEnv(srv.URL,
				"WSMON_MODE", mode,
				"WSMON_MESSAGE_TIMEOUT", mt.String(),
				"WSMON_BURST_SIZE", "5",
				"WSMON_BURST_INTERVAL", "100ms",
			))
			c1, err := srv.WaitConn(1, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			h.Wait("first echoes", func() bool { return h.rec.SumPrefix("echoed") >= 3 }, 5*time.Second)
			c1.Stall()
			t.Cleanup(c1.Resume)
			stalledAt := time.Now()

			h.Wait("a loss", func() bool { return h.rec.SumPrefix("lost") >= 1 }, deadAfter)
			bound := deadAfter + mt + closeTimeout + h.cfg.BackoffCap + time.Second
			c2, err := srv.WaitConn(2, bound)
			if err != nil {
				t.Fatalf("no reconnect within %v of the stall\nrecorder:\n%s\nlogs:\n%s", bound, h.rec.Dump(), tail(h.logs.String(), 40))
			}
			h.Wait("hello_ack on the new connection", func() bool { return !c2.HelloAckAt().IsZero() }, 5*time.Second)

			warns := h.logs.Find("WARN", "presumed half-open")
			if len(warns) != 1 {
				t.Fatalf("want exactly one half-open WARN, got %d\nlogs:\n%s", len(warns), tail(h.logs.String(), 40))
			}
			// Detection: within DEAD_AFTER + MESSAGE_TIMEOUT of the last
			// inbound frame, which is at or just after the stall.
			if at, err := time.Parse(time.RFC3339Nano, warns[0]["time"].(string)); err == nil {
				if d := at.Sub(stalledAt); d < deadAfter-mt || d > deadAfter+mt {
					t.Errorf("half-open detected %v after the stall; want within [%v, %v]", d, deadAfter-mt, deadAfter+mt)
				}
			}
			if c1.ConnID() == c2.ConnID() || c2.ConnID() == "" {
				t.Errorf("conn_id not renewed: %q → %q", c1.ConnID(), c2.ConnID())
			}
			if got := h.Count("closes{1000,client}"); got != 1 {
				t.Errorf("closes{1000,client} = %v, want 1\n%s", got, h.rec.Dump())
			}
			if got := h.Count("closes{1006,remote}"); got != 0 {
				t.Errorf("closes{1006,remote} = %v, want 0", got)
			}
			if got := h.Count("reconnects"); got < 1 {
				t.Errorf("reconnects = %v, want ≥ 1", got)
			}
			// The new connection works.
			before := h.rec.SumPrefix("echoed")
			h.Wait("echoes on the new connection", func() bool { return h.rec.SumPrefix("echoed") > before }, 5*time.Second)
		})
	}
}

// TestHalfOpen_StallRecovers is test (b): the server stalls for
// 3 × MESSAGE_TIMEOUT, then releases the queued probes. No reconnect, the
// same conn_id, losses then late echoes, then normal echoes.
func TestHalfOpen_StallRecovers(t *testing.T) {
	mt := D(1*time.Second, 500*time.Millisecond)
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	h := newHarness(t, probeEnv(srv.URL, "WSMON_MESSAGE_TIMEOUT", mt.String()))
	c1, err := srv.WaitConn(1, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h.Wait("first echoes", func() bool { return h.rec.SumPrefix("echoed") >= 3 }, 5*time.Second)
	c1.Stall()
	time.Sleep(3 * mt)
	if h.rec.SumPrefix("lost") < 1 {
		t.Errorf("no loss during a %v stall", 3*mt)
	}
	echoedAtResume := h.rec.SumPrefix("echoed")
	c1.Resume()

	h.Wait("late echoes", func() bool { return h.rec.SumPrefix("late") >= 1 }, 3*time.Second)
	h.Wait("normal echoes resume", func() bool { return h.rec.SumPrefix("echoed") >= echoedAtResume+5 }, 5*time.Second)
	// Hold past DEAD_AFTER measured from the stall to prove nothing fires late.
	time.Sleep(2 * mt)

	if n := len(srv.Upgraded()); n != 1 {
		t.Errorf("server saw %d connections, want 1 (no reconnect)", n)
	}
	if got := h.Count("reconnects"); got != 0 {
		t.Errorf("reconnects = %v, want 0", got)
	}
	if w := h.logs.Find("WARN", "presumed half-open"); len(w) != 0 {
		t.Errorf("unexpected half-open WARN: %v", w)
	}
	if got := h.rec.SumPrefix("closes"); got != 0 {
		t.Errorf("closes = %v, want 0\n%s", got, h.rec.Dump())
	}
}
