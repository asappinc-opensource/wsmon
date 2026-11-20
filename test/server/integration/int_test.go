package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/test/server/wstest"
)

func TestInt01PingPong(t *testing.T) {
	t.Parallel()
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	total := dur(30*time.Second, 3*time.Second)
	interval := dur(time.Second, 100*time.Millisecond)
	n := 0
	for deadline := time.Now().Add(total); time.Now().Before(deadline); time.Sleep(interval) {
		p := c.NextProbe(wstest.ClientSkyEdge)
		echo, _, err := c.RoundTrip(ctxT(t, 5*time.Second), p)
		if err != nil {
			t.Fatalf("probe %d: %v", p.Seq, err)
		}
		if echo.Seq != p.Seq || echo.TraceID != p.TraceID || echo.ConnID != p.ConnID {
			t.Fatalf("echo does not match probe: %+v vs %+v", echo, p)
		}
		n++
	}
	if got := e.srv.ActiveConnections(); got != 1 {
		t.Errorf("active connections = %d", got)
	}
	t.Logf("%d round trips", n)
}

func TestInt02Burst(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	ctx := ctxT(t, 5*time.Second)
	for range 20 {
		if err := c.Send(ctx, c.NextProbe(wstest.ClientSkyEdge)); err != nil {
			t.Fatal(err)
		}
	}
	for want := int64(1); want <= 20; want++ {
		typ, b, err := c.RecvType(ctx)
		if err != nil || typ != protocol.TypeEcho {
			t.Fatalf("echo %d: %v %s", want, err, b)
		}
		var echo protocol.Echo
		_ = json.Unmarshal(b, &echo)
		if echo.Seq != want {
			t.Fatalf("echo seq %d, want %d", echo.Seq, want)
		}
	}
}

func TestInt03WSSUpgradeNegotiatesSubprotocol(t *testing.T) {
	e := start(t, nil)
	ts := httptest.NewUnstartedServer(e.srv.Handler())
	ts.StartTLS()
	t.Cleanup(ts.Close)

	url := "wss" + strings.TrimPrefix(ts.URL, "https") + e.cfg.WSPath
	c, err := wstest.Dial(ctxT(t, 5*time.Second), url, wstest.DialOptions{HTTPClient: ts.Client()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.WS.Subprotocol() != protocol.Subprotocol {
		t.Errorf("subprotocol %q", c.WS.Subprotocol())
	}
	if _, err := c.Hello(ctxT(t, 5*time.Second), wstest.ClientSkyEdge, protocol.Options{}); err != nil {
		t.Fatal(err)
	}
	expectOpen(t, c)
}

// In-process variant of TC-INT-04; the SIGTERM variant is in process_test.go.
func TestInt04DrainSendsShuttingDownThen1001(t *testing.T) {
	e := start(t, nil)
	clients := make([]*wstest.Client, 3)
	for i := range clients {
		clients[i], _ = e.connect(wstest.ClientSkyEdge, protocol.Options{})
	}
	// A connection still waiting for hello is drained too.
	pending := e.dial(wstest.DialOptions{})

	e.srv.BeginDrain()
	if resp, err := http.Get(e.adminURL + "/readyz"); err != nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/readyz while draining: %v %v", resp, err)
	}
	done := make(chan error, 1)
	go func() { done <- e.srv.CloseConnections(ctxT(t, 15*time.Second)) }()

	for _, c := range append(clients, pending) {
		expectError(t, c, protocol.ErrShuttingDown)
		expectClose(t, c, protocol.CloseGoingAway)
		assertErrorBeforeClose(t, c, protocol.CloseGoingAway)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	body := e.scrape()
	if v, _ := metric(body, `wsmon_server_closes_total{close_code="1001"}`); v != 4 {
		t.Errorf("closes 1001 = %v, want 4", v)
	}
	if v, _ := metric(body, `wsmon_server_closes_total{close_code="1006"}`); v != 0 {
		t.Errorf("drain produced %v 1006 closes", v)
	}

	// New upgrades are refused once connections are being closed.
	if _, err := wstest.Dial(ctxT(t, 5*time.Second), e.wsURL, wstest.DialOptions{}); err == nil {
		t.Error("upgrade accepted after shutdown began")
	}
}

// assertErrorBeforeClose checks the wire order: the error frame, then the
// server's close frame with code.
func assertErrorBeforeClose(t *testing.T, c *wstest.Client, code int) {
	t.Helper()
	frames, err := c.Capture.FromServer()
	if err != nil {
		t.Fatal(err)
	}
	errIdx, closeIdx := -1, -1
	for i, f := range frames {
		switch {
		case f.Opcode == wstest.OpText && strings.Contains(string(f.Payload), `"type":"error"`):
			errIdx = i
		case f.Opcode == wstest.OpClose && closeIdx < 0:
			closeIdx = i
			if f.CloseCode() != code {
				t.Errorf("close frame code %d, want %d", f.CloseCode(), code)
			}
		}
	}
	if errIdx < 0 || closeIdx < 0 || errIdx > closeIdx {
		t.Errorf("error frame at %d, close frame at %d: want error first", errIdx, closeIdx)
	}
}

func TestInt07WrongTokenIs401BeforeUpgrade(t *testing.T) {
	e := start(t, func(c *config.Server) { c.AuthToken = "s3cret" })
	for _, tok := range []string{"", "wrong", "s3cret-but-longer"} {
		c, err := wstest.Dial(ctxT(t, 5*time.Second), e.wsURL, wstest.DialOptions{Token: tok})
		if err == nil {
			t.Fatalf("token %q: upgrade succeeded", tok)
		}
		if c.Resp == nil || c.Resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("token %q: response %+v, want 401", tok, c.Resp)
		}
	}
}

func TestInt08MaxConnectionsIs503(t *testing.T) {
	e := start(t, func(c *config.Server) { c.MaxConnections = 2 })
	e.connect(wstest.ClientSkyEdge, protocol.Options{})
	e.connect(wstest.ClientSkyEdge, protocol.Options{})
	c, err := wstest.Dial(ctxT(t, 5*time.Second), e.wsURL, wstest.DialOptions{})
	if err == nil {
		t.Fatal("third connection accepted")
	}
	if c.Resp == nil || c.Resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("response %+v, want 503", c.Resp)
	}
}

func TestInt09PushAndPushAck(t *testing.T) {
	t.Parallel()
	e := start(t, nil)
	interval := dur(time.Second, 200*time.Millisecond)
	c, ack := e.connect(wstest.ClientSkyEdge, protocol.Options{PushIntervalMs: interval.Milliseconds()})
	if ack.AcceptedOptions.PushIntervalMs != interval.Milliseconds() {
		t.Fatalf("push not accepted: %+v", ack.AcceptedOptions)
	}

	var got []protocol.Push
	ctx := ctxT(t, 10*interval+5*time.Second)
	for len(got) < 10 {
		typ, b, err := c.RecvType(ctx)
		if err != nil || typ != protocol.TypePush {
			t.Fatalf("push %d: %v %s", len(got)+1, err, b)
		}
		var p protocol.Push
		if err := json.Unmarshal(b, &p); err != nil {
			t.Fatal(err)
		}
		got = append(got, p)
		if err := c.Send(ctx, protocol.PushAck{V: 1, Type: protocol.TypePushAck, TraceID: p.TraceID, ConnID: p.ConnID, PushSeq: p.PushSeq, ClientRecvEpochNs: time.Now().UnixNano()}); err != nil {
			t.Fatal(err)
		}
	}
	hex32, hex16 := regexp.MustCompile(`^[0-9a-f]{32}$`), regexp.MustCompile(`^[0-9a-f]{16}$`)
	for i, p := range got {
		if p.PushSeq != int64(i+1) {
			t.Errorf("push_seq %d at position %d: pushes must start at 1 and never skip", p.PushSeq, i)
		}
		if p.ConnID != c.ConnID || p.Server != testIdentity || p.ServerSendEpochNs == 0 {
			t.Errorf("bad push: %+v", p)
		}
		if !hex32.MatchString(p.TraceID) || !hex16.MatchString(p.SpanID) || p.Traceparent != "00-"+p.TraceID+"-"+p.SpanID+"-01" {
			t.Errorf("bad push trace context: %+v", p)
		}
	}
	eventually(t, 2*time.Second, func() bool {
		v, _ := metric(e.scrape(), "wsmon_server_push_ack_seconds_count")
		return v >= 10
	}, "wsmon_server_push_ack_seconds not observed for every ack")
	body := e.scrape()
	if v, _ := metric(body, `wsmon_server_messages_total{type="push_ack"}`); v < 10 {
		t.Errorf("push_ack messages = %v", v)
	}
	if v, _ := metric(body, `wsmon_server_messages_total{type="push"}`); v < 10 {
		t.Errorf("push messages = %v", v)
	}
}

// The server is layer-agnostic: the entry point a client claims changes
// nothing about how it is served or what the server records.
func TestInt10EntrypointChangesNothing(t *testing.T) {
	e := start(t, nil)
	normalise := func(b []byte) map[string]any {
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		for _, k := range []string{"trace_id", "conn_id", "seq", "client_send_epoch_ns", "server_recv_epoch_ns", "server_send_epoch_ns"} {
			if _, ok := m[k]; !ok {
				t.Fatalf("echo missing %s: %s", k, b)
			}
			m[k] = "*"
		}
		return m
	}
	var shapes []string
	for _, info := range []protocol.ClientInfo{wstest.ClientSkyEdge, wstest.ClientPodDirect} {
		c, ack := e.connect(info, protocol.Options{})
		ack.ConnID = ""
		_, b, err := c.RoundTrip(ctxT(t, 5*time.Second), c.NextProbe(info))
		if err != nil {
			t.Fatal(err)
		}
		s, _ := json.Marshal([]any{ack, normalise(b)})
		shapes = append(shapes, string(s))
	}
	if shapes[0] != shapes[1] {
		t.Errorf("behaviour differs by entrypoint:\n%s\n%s", shapes[0], shapes[1])
	}
	body := e.scrape()
	for _, forbidden := range []string{"entrypoint", "skyedge", "pod_direct", "vantage", "relay-prod-east"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("/metrics mentions %q", forbidden)
		}
	}
}

func TestInt13PlaintextWS(t *testing.T) {
	e := start(t, nil)
	if !strings.HasPrefix(e.wsURL, "ws://") {
		t.Fatal(e.wsURL)
	}
	c, _ := e.connect(wstest.ClientPodDirect, protocol.Options{})
	expectOpen(t, c)
}

func TestHealthEndpointsOnAdminPortOnly(t *testing.T) {
	e := start(t, nil)
	for _, p := range []string{"/healthz", "/readyz", e.cfg.MetricsPath} {
		resp, err := http.Get(e.adminURL + p)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Errorf("admin %s: %v %v", p, resp, err)
		}
		wsBase := "http" + strings.TrimSuffix(strings.TrimPrefix(e.wsURL, "ws"), e.cfg.WSPath)
		resp, err = http.Get(wsBase + p)
		if err != nil || resp.StatusCode != http.StatusNotFound {
			t.Errorf("ws port %s: %v %v, want 404", p, resp, err)
		}
	}
}

func TestClientCloseIsRecorded(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	c.Close()
	eventually(t, 5*time.Second, func() bool {
		v, _ := metric(e.scrape(), `wsmon_server_closes_total{close_code="1000"}`)
		return v == 1
	}, "client 1000 close not recorded")
	if v, _ := metric(e.scrape(), "wsmon_server_connections_active"); v != 0 {
		t.Errorf("connections_active = %v", v)
	}
}
