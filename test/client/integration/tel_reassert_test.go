package integration

import (
	"strings"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/test/client/fakeserver"
)

// The fake-recorder scenarios re-run with the real telemetry, asserting the exact
// series and labels expected on /metrics.
func TestReassertViaMetrics(t *testing.T) {
	const wait = 10 * time.Second
	pp := L{"mode": "pingpong", "site": "tel-site", "path": "direct"}
	with := func(base L, kv ...string) L {
		out := L{}
		for k, v := range base {
			out[k] = v
		}
		for i := 0; i+1 < len(kv); i += 2 {
			out[kv[i]] = kv[i+1]
		}
		return out
	}
	env := func(url string, kv ...string) map[string]string {
		return probeEnv(url, append([]string{"WSMON_LABELS", telLabels}, kv...)...)
	}

	t.Run("TC-INT-01", func(t *testing.T) {
		srv := fakeserver.New(t, fakeserver.Behaviour{})
		h := newTelHarness(t, env(srv.URL), false)
		s := h.Wait("3 small and 3 large echoes", func(s *Scrape) bool {
			return s.Counter("wsmon_client_messages_echoed_total", with(pp, "payload", "small")) >= 3 &&
				s.Counter("wsmon_client_messages_echoed_total", with(pp, "payload", "large")) >= 3
		}, wait)
		for _, p := range []string{"small", "large"} {
			sent, echoed := s.Counter("wsmon_client_messages_sent_total", with(pp, "payload", p)), s.Counter("wsmon_client_messages_echoed_total", with(pp, "payload", p))
			if d := sent - echoed; d < 0 || d > 1 || s.Counter("wsmon_client_messages_lost_total", with(pp, "payload", p)) != 0 {
				t.Errorf("%s: sent %v echoed %v", p, sent, echoed)
			}
		}
		if s.Value("wsmon_client_connection_up", pp) != 1 || s.Value("wsmon_client_connections_active", pp) != 1 || s.Value("wsmon_client_connection_age_seconds", pp) <= 0 {
			t.Errorf("connection gauges:\n%s", grepWS(s.Raw))
		}
		if _, ok := s.Fams["wsmon_client_closes_total"]; ok {
			t.Error("closes before any close")
		}
	})

	t.Run("TC-INT-04", func(t *testing.T) {
		srv := fakeserver.New(t, fakeserver.Behaviour{OnHello: func(c *fakeserver.Conn) {
			if c.Index == 0 {
				time.Sleep(200 * time.Millisecond)
				c.Drain()
			}
		}})
		h := newTelHarness(t, env(srv.URL), false)
		s := h.Wait("drain then reconnect", func(s *Scrape) bool {
			return s.Counter("wsmon_client_closes_total", with(pp, "close_code", "1001", "initiator", "remote")) == 1 &&
				s.Counter("wsmon_client_reconnects_total", pp) >= 1 && s.Value("wsmon_client_connection_up", pp) == 1
		}, wait)
		if s.Counter("wsmon_client_closes_total", L{"close_code": "1006"}) != 0 ||
			s.Counter("wsmon_client_server_errors_received_total", L{"code": "shutting_down", "site": "tel-site"}) != 1 ||
			s.HistCount("wsmon_client_connection_lifetime_seconds", with(pp, "close_code", "1001")) != 1 {
			t.Errorf("/metrics:\n%s", grepWS(s.Raw))
		}
	})

	t.Run("TC-INT-05", func(t *testing.T) {
		srv := fakeserver.New(t, fakeserver.Behaviour{OnHello: func(c *fakeserver.Conn) {
			if c.Index == 0 {
				time.Sleep(200 * time.Millisecond)
				c.Kill()
			}
		}})
		h := newTelHarness(t, env(srv.URL), false)
		h.Wait("1006 remote", func(s *Scrape) bool {
			return s.Counter("wsmon_client_closes_total", with(pp, "close_code", "1006", "initiator", "remote")) == 1 &&
				s.HistCount("wsmon_client_connection_lifetime_seconds", with(pp, "close_code", "1006")) == 1 &&
				s.Counter("wsmon_client_reconnects_total", pp) >= 1
		}, wait)
	})

	t.Run("TC-INT-07", func(t *testing.T) {
		srv := fakeserver.New(t, fakeserver.Behaviour{Token: "right"})
		h := newTelHarness(t, env(srv.URL, "WSMON_AUTH_TOKEN", "wrong"), false)
		s := h.Wait("auth failures", func(s *Scrape) bool {
			return s.Counter("wsmon_client_connect_failures_total", with(pp, "reason", "auth")) >= 2
		}, wait)
		if s.Counter("wsmon_client_connect_attempts_total", pp) < 2 || s.Counter("wsmon_client_connect_failures_total", with(pp, "reason", "http_status")) != 0 {
			t.Errorf("/metrics:\n%s", grepWS(s.Raw))
		}
	})

	t.Run("TC-INT-08", func(t *testing.T) {
		srv := fakeserver.New(t, fakeserver.Behaviour{HTTPStatus: 503})
		h := newTelHarness(t, env(srv.URL), false)
		s := h.Wait("http_status failures", func(s *Scrape) bool {
			return s.Counter("wsmon_client_connect_failures_total", with(pp, "reason", "http_status")) >= 2
		}, wait)
		if _, ok := s.Fams["wsmon_client_closes_total"]; ok {
			t.Error("a non-101 was counted as a close")
		}
	})

	t.Run("TC-WIRE-10", func(t *testing.T) {
		srv := fakeserver.New(t, fakeserver.Behaviour{OmitSubprotocol: true})
		h := newTelHarness(t, env(srv.URL), false)
		h.Wait("subprotocol", func(s *Scrape) bool {
			return s.Counter("wsmon_client_connect_failures_total", with(pp, "reason", "subprotocol")) >= 1 &&
				s.Counter("wsmon_client_closes_total", with(pp, "close_code", "1002", "initiator", "client")) >= 1
		}, wait)
	})

	t.Run("TC-WIRE-17", func(t *testing.T) {
		srv := fakeserver.New(t, fakeserver.Behaviour{
			AcceptedOptions: func(r protocol.Options) protocol.Options { r.ServerKeepalive = true; return r },
		})
		h := newTelHarness(t, env(srv.URL), false)
		s := h.Wait("options_rejected", func(s *Scrape) bool {
			return s.Counter("wsmon_client_connect_failures_total", with(pp, "reason", "options_rejected")) >= 1 &&
				s.Counter("wsmon_client_closes_total", with(pp, "close_code", "1002", "initiator", "client")) >= 1
		}, wait)
		if s.HistCount("wsmon_client_connect_duration_seconds", with(pp, "phase", "hello")) < 1 || s.Counter("wsmon_client_messages_sent_total", pp) != 0 {
			t.Errorf("/metrics:\n%s", grepWS(s.Raw))
		}
	})

	t.Run("TC-WIRE-18", func(t *testing.T) {
		srv := fakeserver.New(t, fakeserver.Behaviour{OnHello: func(c *fakeserver.Conn) {
			_ = c.SendError(protocol.ErrRateLimited, 7, "slow down")
			_ = c.SendError("brand_new_code", 0, "from the future")
		}})
		h := newTelHarness(t, env(srv.URL), false)
		s := h.Wait("error frames counted", func(s *Scrape) bool {
			return s.Counter("wsmon_client_server_errors_received_total", L{"code": "rate_limited", "site": "tel-site"}) == 1 &&
				s.Counter("wsmon_client_server_errors_received_total", L{"code": "other", "site": "tel-site"}) == 1
		}, wait)
		if strings.Contains(s.Raw, "brand_new_code") || s.Counter("wsmon_client_reconnects_total", pp) != 0 {
			t.Errorf("/metrics:\n%s", grepWS(s.Raw))
		}
	})

	t.Run("TC-CORR-05", func(t *testing.T) {
		srv := fakeserver.New(t, fakeserver.Behaviour{OnProbe: func(c *fakeserver.Conn, p fakeserver.Probe) {
			if p.Seq != 1 {
				_ = c.Echo(p)
			}
		}})
		h := newTelHarness(t, env(srv.URL, "WSMON_PAYLOADS", "small", "WSMON_MESSAGE_TIMEOUT", "300ms"), false)
		h.Wait("one lost", func(s *Scrape) bool {
			return s.Counter("wsmon_client_messages_lost_total", with(pp, "payload", "small")) == 1 &&
				s.Counter("wsmon_client_messages_echoed_total", with(pp, "payload", "small")) >= 1
		}, wait)
	})

	t.Run("TC-CORR-06", func(t *testing.T) {
		const mt = 300 * time.Millisecond
		srv := fakeserver.New(t, fakeserver.Behaviour{OnProbe: func(c *fakeserver.Conn, p fakeserver.Probe) {
			if p.Seq == 1 {
				e := c.MakeEcho(p)
				go func() { time.Sleep(3 * mt); _ = c.SendJSON(e) }()
				return
			}
			_ = c.Echo(p)
		}})
		h := newTelHarness(t, env(srv.URL, "WSMON_PAYLOADS", "small", "WSMON_MESSAGE_TIMEOUT", mt.String()), false)
		h.Wait("one late, one lost", func(s *Scrape) bool {
			return s.Counter("wsmon_client_messages_late_total", with(pp, "payload", "small")) == 1 &&
				s.Counter("wsmon_client_messages_lost_total", with(pp, "payload", "small")) == 1
		}, wait)
	})

	t.Run("TC-IDLE-02", func(t *testing.T) {
		srv := fakeserver.New(t, fakeserver.Behaviour{})
		px := fakeserver.NewIdleProxy(t, srv.Addr, D(1500*time.Millisecond, 150*time.Millisecond))
		h := newTelHarness(t, idleEnv("ws://"+px.Addr+fakeserver.Path, "WSMON_LABELS", telLabels), false)
		b0, b1 := config.IdleBucketLabel(h.cfg.IdleLadder[0]), config.IdleBucketLabel(h.cfg.IdleLadder[1])
		u := L{"site": "tel-site", "path": "direct"}
		s := h.Wait("rung 2 dropped by the proxy", func(s *Scrape) bool {
			return s.Counter("wsmon_client_idle_probe_total", with(u, "idle_bucket", b1, "result", "dropped")) >= 1
		}, D(60*time.Second, 10*time.Second))
		dropped := s.Counter("wsmon_client_idle_probe_total", with(u, "idle_bucket", b1, "result", "dropped"))
		if s.Counter("wsmon_client_idle_probe_close_total", with(u, "idle_bucket", b1, "close_code", "1006")) != dropped ||
			s.Counter("wsmon_client_idle_probe_total", with(u, "idle_bucket", b0, "result", "survived")) < 1 ||
			s.Counter("wsmon_client_idle_probe_total", with(u, "idle_bucket", b0, "result", "dropped")) != 0 ||
			s.Counter("wsmon_client_idle_probe_total", with(u, "idle_bucket", b1, "result", "survived")) != 0 {
			t.Errorf("/metrics:\n%s", grepWS(s.Raw))
		}
		for _, r := range h.cfg.IdleLadder[2:] {
			bk := config.IdleBucketLabel(r)
			if s.Counter("wsmon_client_idle_probe_total", with(u, "idle_bucket", bk)) != 0 {
				t.Errorf("rung %s above the drop has observations", bk)
			}
		}
	})
}
