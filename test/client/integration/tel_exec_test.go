package integration

import (
	"errors"
	"net"
	"net/http"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/test/client/fakeserver"
)

// metricsBase waits for the binary's "metrics listening" log line and
// returns http://addr.
func (e *execProbe) metricsBase(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, l := range e.out.Lines() {
			if l["msg"] == "metrics listening" {
				return "http://" + l["addr"].(string)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no metrics listening line:\n%s", e.out.String())
	return ""
}

func (e *execProbe) stop(t *testing.T) {
	t.Helper()
	_ = e.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-e.done:
		if err != nil {
			t.Errorf("exit: %v\n%s", err, tail(e.out.String(), 20))
		}
	case <-time.After(10 * time.Second):
		t.Errorf("did not exit")
	}
}

func status(url string) int {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

// TC-INT-10 (metrics half): two processes with different WSMON_LABELS each
// expose only their own label values on /metrics.
func TestTCINT10_MetricsHalf(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{})
	procs := map[string]*execProbe{
		"entry_a": startBinary(t, binEnv(srv.URL, "entry_a")),
		"entry_b": startBinary(t, binEnv(srv.URL, "entry_b")),
	}
	for site, e := range procs {
		base := e.metricsBase(t)
		var s *Scrape
		for i := 0; i < 200; i++ {
			s = scrape(t, base)
			if s.HistCount("wsmon_client_rtt_seconds", nil) > 0 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if s.HistCount("wsmon_client_rtt_seconds", nil) == 0 {
			t.Errorf("%s: no round trips", site)
		}
		n := 0
		for name, mf := range s.Fams {
			for _, m := range mf.GetMetric() {
				n++
				if got := labelMap(m)["site"]; got != site {
					t.Errorf("%s: %s has site=%q", site, name, got)
				}
			}
		}
		if n == 0 {
			t.Errorf("%s: empty /metrics", site)
		}
	}
	for _, e := range procs {
		e.stop(t)
	}
}

// TC-INT-17 (/readyz half): on SIGTERM /readyz turns 503 while the probe is
// still draining, before its close frame reaches the server; exit 0 within
// SHUTDOWN_TIMEOUT.
func TestTCINT17_ReadyzOnSIGTERM(t *testing.T) {
	const hold = 700 * time.Millisecond // < MESSAGE_TIMEOUT (1 s), so drain waits for it
	srv := fakeserver.New(t, fakeserver.Behaviour{OnProbe: func(c *fakeserver.Conn, p fakeserver.Probe) {
		if p.Seq == 3 {
			go func() { time.Sleep(hold); _ = c.Echo(p) }()
			return
		}
		_ = c.Echo(p)
	}})
	e := startBinary(t, binEnv(srv.URL, "pod_direct"))
	base := e.metricsBase(t)
	if got := status(base + "/readyz"); got != 200 {
		t.Fatalf("/readyz %d before shutdown", got)
	}
	c, err := srv.WaitConn(1, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500 && len(c.Probes()) < 3; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	start := time.Now()
	if err := e.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	var notReadyAt time.Time
	for time.Since(start) < hold {
		if status(base+"/readyz") == http.StatusServiceUnavailable {
			notReadyAt = time.Now()
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	closedBefore := c.CloseCodeReceived() != -1
	if notReadyAt.IsZero() {
		t.Fatal("/readyz never returned 503 during the drain")
	}
	if closedBefore {
		t.Error("close frame reached the server before /readyz turned 503")
	}
	select {
	case err := <-e.done:
		if err != nil {
			t.Errorf("exit: %v", err)
		}
		if el := time.Since(start); el > 3*time.Second {
			t.Errorf("exit after %v > SHUTDOWN_TIMEOUT", el)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("did not exit")
	}
	<-c.Done()
	if got := c.CloseCodeReceived(); got != 1000 {
		t.Errorf("close code %d", got)
	}
	t.Logf("/readyz 503 after %v; close frame after the drain (≈%v)", notReadyAt.Sub(start), hold)
}

// TC-INT-18: an unreachable target (closed port, unresolvable host) never
// fails /healthz or /readyz, while wsmon_client_connect_failures_total rises.
func TestTCINT18_HealthIgnoresTarget(t *testing.T) {
	for name, url := range map[string]string{
		"closed port":       "ws://127.0.0.1:1/__synthetic/ws",
		"unresolvable host": "ws://no-such-host.invalid:8080/__synthetic/ws",
	} {
		t.Run(name, func(t *testing.T) {
			e := startBinary(t, binEnv(url, "unreachable"))
			base := e.metricsBase(t)
			first := scrape(t, base).Counter("wsmon_client_connect_failures_total", nil)
			end := time.Now().Add(D(10*time.Second, 2*time.Second))
			checks := 0
			for time.Now().Before(end) {
				for _, p := range []string{"/healthz", "/readyz"} {
					if got := status(base + p); got != 200 {
						t.Fatalf("%s = %d while the target is unreachable", p, got)
					}
				}
				checks++
				time.Sleep(100 * time.Millisecond)
			}
			s := scrape(t, base)
			last := s.Counter("wsmon_client_connect_failures_total", nil)
			if last <= first || last < 2 || s.Value("wsmon_client_connection_up", nil) != 0 {
				t.Errorf("failures %v → %v, up %v", first, last, s.Value("wsmon_client_connection_up", nil))
			}
			t.Logf("%d health checks all 200; wsmon_client_connect_failures_total %v → %v", checks, first, last)
			e.stop(t)
		})
	}
}

func runHealthcheck(env map[string]string) int {
	cmd := exec.Command(probeBin, "healthcheck")
	cmd.Env = envList(env)
	err := cmd.Run()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &ee):
		return ee.ExitCode()
	}
	return -1
}

// healthcheck subcommand: exit 0 against a running probe with only
// WSMON_METRICS_ADDR set; 1 when nothing listens.
func TestHealthcheckSubcommand(t *testing.T) {
	e := startBinary(t, binEnv("ws://127.0.0.1:1/__synthetic/ws", "hc"))
	_, port, _ := net.SplitHostPort(e.metricsBase(t)[len("http://"):])
	if got := runHealthcheck(map[string]string{"WSMON_METRICS_ADDR": ":" + port}); got != 0 {
		t.Errorf("healthcheck against a running probe: exit %d", got)
	}
	if got := runHealthcheck(map[string]string{"WSMON_METRICS_ADDR": "0.0.0.0:" + port, "WSMON_MODE": "nonsense"}); got != 0 {
		t.Errorf("healthcheck must ignore the rest of the config: exit %d", got)
	}
	e.stop(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, free, _ := net.SplitHostPort(ln.Addr().String())
	ln.Close()
	if got := runHealthcheck(map[string]string{"WSMON_METRICS_ADDR": ":" + free}); got != 1 {
		t.Errorf("healthcheck with nothing listening: exit %d, want 1", got)
	}
}

func configHash(t *testing.T, env map[string]string) string {
	t.Helper()
	e := startBinary(t, env)
	s := scrape(t, e.metricsBase(t))
	e.stop(t)
	mf := s.Fams["wsmon_client_config_hash"]
	if mf == nil || len(mf.GetMetric()) != 1 {
		t.Fatalf("wsmon_client_config_hash: %v", mf)
	}
	return labelMap(mf.GetMetric()[0])["hash"]
}

// TC-TEL-08: changing a non-secret variable changes wsmon_client_config_hash;
// changing only secrets does not.
func TestTCTEL08_ConfigHash(t *testing.T) {
	base := func(kv ...string) map[string]string {
		m := binEnv("ws://127.0.0.1:1/__synthetic/ws", "hash")
		for i := 0; i+1 < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return m
	}
	h0 := configHash(t, base())
	h1 := configHash(t, base("WSMON_PINGPONG_INTERVAL", "70ms"))
	h2 := configHash(t, base("WSMON_AUTH_TOKEN", "another-token",
		"WSMON_REMOTE_WRITE_PASSWORD", "pw2", "WSMON_REMOTE_WRITE_BEARER_TOKEN", ""))
	h3 := configHash(t, base("WSMON_REMOTE_WRITE_BEARER_TOKEN", "bearer-2"))
	if len(h0) != 16 {
		t.Fatalf("hash %q", h0)
	}
	if h1 == h0 {
		t.Error("non-secret change kept the hash")
	}
	if h2 != h0 || h3 != h0 {
		t.Errorf("secret-only changes changed the hash: %s %s %s", h0, h2, h3)
	}
}
