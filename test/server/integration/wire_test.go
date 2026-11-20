package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/test/server/wstest"
)

// rawProbe renders a probe by hand so tests control every byte.
func rawProbe(connID string, seq int64, extra string) []byte {
	return fmt.Appendf(nil, `{"v":1,"type":"probe","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736",`+
		`"span_id":"00f067aa0ba902b7","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",`+
		`"conn_id":%q,"seq":%d,"client_send_epoch_ns":1758537600123456789,`+
		`"client":{"target":"t","entrypoint":"skyedge","vantage":"v","mode":"pingpong","env":"e","version":"1"}%s}`,
		connID, seq, extra)
}

func TestWire01EchoCopiesFieldsByteIdentically(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	ctx := ctxT(t, 5*time.Second)

	if err := c.SendRaw(ctx, rawProbe(c.ConnID, 1427, "")); err != nil {
		t.Fatal(err)
	}
	b, err := c.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`,
		`"conn_id":"` + c.ConnID + `"`,
		`"seq":1427`,
		`"client_send_epoch_ns":1758537600123456789`,
	} {
		if !bytes.Contains(b, []byte(want)) {
			t.Errorf("echo does not carry %s verbatim: %s", want, b)
		}
	}
	var echo protocol.Echo
	if err := json.Unmarshal(b, &echo); err != nil {
		t.Fatal(err)
	}
	if echo.Server.Name == "" || echo.Server.Version == "" {
		t.Errorf("server identity incomplete: %+v", echo.Server)
	}
}

func TestWire02PaddingIsNotEchoed(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	p := c.NextProbe(wstest.ClientSkyEdge)
	p.Padding = strings.Repeat("x", 400)
	_, b, err := c.RoundTrip(ctxT(t, 5*time.Second), p)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte("padding")) || bytes.Contains(b, []byte("xxxx")) {
		t.Errorf("echo contains padding: %s", b)
	}
}

func TestWire03BadVersionClosesWith1002(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	if err := c.SendRaw(ctxT(t, 5*time.Second), bytes.Replace(rawProbe(c.ConnID, 1, ""), []byte(`"v":1`), []byte(`"v":2`), 1)); err != nil {
		t.Fatal(err)
	}
	ef := expectError(t, c, protocol.ErrBadVersion)
	if ef.ConnID != c.ConnID {
		t.Errorf("error conn_id %q, want %q", ef.ConnID, c.ConnID)
	}
	expectClose(t, c, protocol.CloseProtocolError)
}

func TestWire04MalformedJSONClosesWith1002(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	if err := c.SendRaw(ctxT(t, 5*time.Second), []byte(`{"v":1,"type":"probe",`)); err != nil {
		t.Fatal(err)
	}
	expectError(t, c, protocol.ErrBadJSON)
	expectClose(t, c, protocol.CloseProtocolError)
}

func TestWire05UnknownFieldsAreIgnored(t *testing.T) {
	e := start(t, nil)
	c := e.dial(wstest.DialOptions{})
	ctx := ctxT(t, 5*time.Second)
	hello := fmt.Appendf(nil, `{"v":1,"type":"hello","conn_id":%q,"client":{},"options":{"server_keepalive":false,"push_interval_ms":0,"new_opt":7},"new_top":"x"}`, c.ConnID)
	if err := c.SendRaw(ctx, hello); err != nil {
		t.Fatal(err)
	}
	if typ, b, err := c.RecvType(ctx); err != nil || typ != protocol.TypeHelloAck {
		t.Fatalf("hello with unknown fields rejected: %v %s", err, b)
	}
	if err := c.SendRaw(ctx, rawProbe(c.ConnID, 1, `,"brand_new":{"nested":[1,2,3]}`)); err != nil {
		t.Fatal(err)
	}
	if typ, b, err := c.RecvType(ctx); err != nil || typ != protocol.TypeEcho {
		t.Fatalf("probe with unknown field not echoed: %v %s", err, b)
	}
}

func TestWire06ProbeBeforeHelloClosesWith1002(t *testing.T) {
	e := start(t, nil)
	c := e.dial(wstest.DialOptions{})
	if err := c.Send(ctxT(t, 5*time.Second), c.NextProbe(wstest.ClientSkyEdge)); err != nil {
		t.Fatal(err)
	}
	expectError(t, c, protocol.ErrNoHello)
	expectClose(t, c, protocol.CloseProtocolError)
}

func TestWire07SeqRegressionKeepsConnectionOpen(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	ctx := ctxT(t, 5*time.Second)
	for _, seq := range []int64{5} {
		if err := c.SendRaw(ctx, rawProbe(c.ConnID, seq, "")); err != nil {
			t.Fatal(err)
		}
		if typ, b, err := c.RecvType(ctx); err != nil || typ != protocol.TypeEcho {
			t.Fatalf("seq %d: %v %s", seq, err, b)
		}
	}
	if err := c.SendRaw(ctx, rawProbe(c.ConnID, 4, "")); err != nil {
		t.Fatal(err)
	}
	if ef := expectError(t, c, protocol.ErrSeqRegression); ef.Seq != 4 {
		t.Errorf("error seq = %d, want 4", ef.Seq)
	}
	if err := c.SendRaw(ctx, rawProbe(c.ConnID, 6, "")); err != nil {
		t.Fatal(err)
	}
	if typ, b, err := c.RecvType(ctx); err != nil || typ != protocol.TypeEcho {
		t.Fatalf("connection unusable after seq_regression: %v %s", err, b)
	}
}

func TestWire08TooLargeClosesWith1009(t *testing.T) {
	e := start(t, func(c *config.Server) { c.MaxFrameBytes = 64 << 10 })
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	p := c.NextProbe(wstest.ClientSkyEdge)
	p.Padding = strings.Repeat("x", 70<<10)
	if err := c.Send(ctxT(t, 5*time.Second), p); err != nil {
		t.Fatal(err)
	}
	expectError(t, c, protocol.ErrTooLarge)
	expectClose(t, c, protocol.CloseTooBig)
}

func TestWire09SubprotocolIsEchoed(t *testing.T) {
	e := start(t, nil)
	c := e.dial(wstest.DialOptions{})
	if got := c.WS.Subprotocol(); got != protocol.Subprotocol {
		t.Errorf("negotiated %q, want %q", got, protocol.Subprotocol)
	}
	if got := c.Resp.Header.Get("Sec-WebSocket-Protocol"); got != protocol.Subprotocol {
		t.Errorf("Sec-WebSocket-Protocol response header %q", got)
	}
}

func TestWire11HelloTimeoutClosesWith1002(t *testing.T) {
	e := start(t, func(c *config.Server) { c.HelloTimeout = 300 * time.Millisecond })
	c := e.dial(wstest.DialOptions{})
	start := time.Now()
	expectClose(t, c, protocol.CloseProtocolError)
	if el := time.Since(start); el < 250*time.Millisecond {
		t.Errorf("closed after %v, before hello_timeout", el)
	}
}

func TestWire12LargeFrameRoundTrip(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	p := c.NextProbe(wstest.ClientSkyEdge)
	b, _ := json.Marshal(p)
	p.Padding = strings.Repeat("x", 32<<10-len(b)-len(`,"padding":""`))
	if b, _ := json.Marshal(p); len(b) != 32<<10 {
		t.Fatalf("frame is %d bytes, want 32 KiB", len(b))
	}
	echo, _, err := c.RoundTrip(ctxT(t, 5*time.Second), p)
	if err != nil {
		t.Fatal(err)
	}
	if echo.Seq != p.Seq || echo.TraceID != p.TraceID || echo.ClientSendEpochNs != p.ClientSendEpochNs {
		t.Errorf("echo mismatch: %+v", echo)
	}
}

func TestWire13UnknownTypeKeepsConnectionOpen(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	if err := c.SendRaw(ctxT(t, 5*time.Second), []byte(`{"v":1,"type":"banana"}`)); err != nil {
		t.Fatal(err)
	}
	expectError(t, c, protocol.ErrBadType)
	expectOpen(t, c)
}

func TestWire14MissingSeqKeepsConnectionOpen(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	b := bytes.Replace(rawProbe(c.ConnID, 1, ""), []byte(`"seq":1,`), nil, 1)
	if err := c.SendRaw(ctxT(t, 5*time.Second), b); err != nil {
		t.Fatal(err)
	}
	if ef := expectError(t, c, protocol.ErrMissingField); !strings.Contains(ef.Message, "seq") {
		t.Errorf("message %q does not name the field", ef.Message)
	}
	expectOpen(t, c)
}

func TestWire15RateLimitDropsExcessAndStaysOpen(t *testing.T) {
	e := start(t, nil) // cap 200/s
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	ctx := ctxT(t, 10*time.Second)

	const sent = 300
	for range sent {
		if err := c.Send(ctx, c.NextProbe(wstest.ClientSkyEdge)); err != nil {
			t.Fatal(err)
		}
	}
	echoes, limited := 0, 0
	for range sent {
		typ, b, err := c.RecvType(ctx)
		if err != nil {
			t.Fatal(err)
		}
		switch typ {
		case protocol.TypeEcho:
			echoes++
		case protocol.TypeError:
			var ef protocol.Error
			_ = json.Unmarshal(b, &ef)
			if ef.Code != protocol.ErrRateLimited {
				t.Fatalf("unexpected error %s", b)
			}
			limited++
		}
	}
	if limited == 0 || echoes > 200 {
		t.Errorf("echoes=%d rate_limited=%d; want the excess over 200/s dropped", echoes, limited)
	}
	time.Sleep(1100 * time.Millisecond)
	expectOpen(t, c)
}

func TestWire16ValidBearerTokenUpgrades(t *testing.T) {
	e := start(t, func(c *config.Server) { c.AuthToken = "s3cret" })
	c := e.dial(wstest.DialOptions{Token: "s3cret"})
	if c.Resp.StatusCode != http.StatusSwitchingProtocols {
		t.Errorf("status %d", c.Resp.StatusCode)
	}
	if _, err := c.Hello(ctxT(t, 5*time.Second), wstest.ClientSkyEdge, protocol.Options{}); err != nil {
		t.Fatal(err)
	}
}

func TestWire18EveryErrorFrameIsCounted(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	for range 3 {
		if err := c.SendRaw(ctxT(t, 5*time.Second), []byte(`{"v":1,"type":"banana"}`)); err != nil {
			t.Fatal(err)
		}
		expectError(t, c, protocol.ErrBadType)
	}
	c2 := e.dial(wstest.DialOptions{})
	_ = c2.SendRaw(ctxT(t, 5*time.Second), []byte(`{"v":1,"type":"probe"}`))
	expectError(t, c2, protocol.ErrNoHello)

	body := e.scrape()
	for series, want := range map[string]float64{
		`wsmon_server_errors_total{code="bad_type"}`:    3,
		`wsmon_server_errors_total{code="no_hello"}`:    1,
		`wsmon_server_errors_total{code="bad_version"}`: 0,
	} {
		if got, ok := metric(body, series); !ok || got != want {
			t.Errorf("%s = %v (present=%v), want %v", series, got, ok, want)
		}
	}
}

// accepted_options must report what the server does, never what was asked
// for if it differs.
func TestAcceptedOptionsReflectWhatWasHonoured(t *testing.T) {
	e := start(t, nil)
	for _, tc := range []struct{ req, want protocol.Options }{
		{protocol.Options{}, protocol.Options{}},
		{protocol.Options{ServerKeepalive: true}, protocol.Options{ServerKeepalive: true}},
		{protocol.Options{PushIntervalMs: 1000}, protocol.Options{PushIntervalMs: 1000}},
		{protocol.Options{PushIntervalMs: 5}, protocol.Options{PushIntervalMs: 100}},
		{protocol.Options{PushIntervalMs: -3}, protocol.Options{}},
	} {
		c, ack := e.connect(wstest.ClientSkyEdge, tc.req)
		if ack.AcceptedOptions != tc.want {
			t.Errorf("requested %+v: accepted %+v, want %+v", tc.req, ack.AcceptedOptions, tc.want)
		}
		if ack.ConnID != c.ConnID || ack.Server != testIdentity {
			t.Errorf("hello_ack identity: %+v", ack)
		}
		c.Close()
	}
}

// t2 before t3, both on the server clock, neither clamped.
func TestTimestampsBracketHandling(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	for range 20 {
		p := c.NextProbe(wstest.ClientSkyEdge)
		echo, _, err := c.RoundTrip(ctxT(t, 5*time.Second), p)
		t4 := time.Now().UnixNano()
		if err != nil {
			t.Fatal(err)
		}
		t2, t3 := echo.ServerRecvEpochNs, echo.ServerSendEpochNs
		// Same host, same clock: t1 <= t2 <= t3 <= t4.
		if !(p.ClientSendEpochNs <= t2 && t2 <= t3 && t3 <= t4) {
			t.Fatalf("timestamps out of order: t1=%d t2=%d t3=%d t4=%d", p.ClientSendEpochNs, t2, t3, t4)
		}
		if t3-t2 > int64(10*time.Millisecond) {
			t.Errorf("t3 - t2 = %v", time.Duration(t3-t2))
		}
	}
}

func TestPermessageDeflateHonouredWhenOffered(t *testing.T) {
	e := start(t, func(c *config.Server) { c.EnablePermessageDeflate = true })
	c := e.dial(wstest.DialOptions{Compression: true})
	if ext := c.Resp.Header.Get("Sec-WebSocket-Extensions"); !strings.Contains(ext, "permessage-deflate") {
		t.Fatalf("permessage-deflate not negotiated: %q", ext)
	}
	if _, err := c.Hello(ctxT(t, 5*time.Second), wstest.ClientSkyEdge, protocol.Options{}); err != nil {
		t.Fatal(err)
	}
	p := c.NextProbe(wstest.ClientSkyEdge)
	p.Padding = strings.Repeat("x", 32<<10)
	if _, _, err := c.RoundTrip(ctxT(t, 5*time.Second), p); err != nil {
		t.Fatal(err)
	}

	off := start(t, nil)
	c2 := off.dial(wstest.DialOptions{Compression: true})
	if ext := c2.Resp.Header.Get("Sec-WebSocket-Extensions"); ext != "" {
		t.Errorf("deflate negotiated while disabled: %q", ext)
	}
}
