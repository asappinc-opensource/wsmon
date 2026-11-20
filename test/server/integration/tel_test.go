package integration

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/internal/server/servertel"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
	"github.com/asappinc-opensource/wsmon/test/server/wstest"
)

func spansNamed(e *env, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range e.spans.Ended() {
		if s.Name() == name {
			out = append(out, s)
		}
	}
	return out
}

func attrMap(s sdktrace.ReadOnlySpan) map[attribute.Key]attribute.Value {
	m := map[attribute.Key]attribute.Value{}
	for _, a := range s.Attributes() {
		m[a.Key] = a.Value
	}
	return m
}

// No span or metric may carry entry-point information.
func assertNoEntrypointAttrs(t *testing.T, s sdktrace.ReadOnlySpan) {
	t.Helper()
	for _, a := range s.Attributes() {
		k := string(a.Key)
		if strings.Contains(k, "entrypoint") || strings.Contains(k, "target") || strings.Contains(k, "vantage") {
			t.Errorf("%s carries attribute %s", s.Name(), k)
		}
	}
}

// TC-TEL-02: no series carries trace_id, conn_id or seq as a label, nor any
// client-side universal label. Every series carries the server's own
// WSMON_LABELS.
func TestTel02NoHighCardinalityLabels(t *testing.T) {
	e := start(t, withTestLabels)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{PushIntervalMs: 100})
	for range 5 {
		_ = c.Send(ctxT(t, 5*time.Second), c.NextProbe(wstest.ClientSkyEdge))
	}
	_ = c.SendRaw(ctxT(t, 5*time.Second), []byte(`{"v":1,"type":"banana"}`))
	time.Sleep(300 * time.Millisecond)

	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(e.scrape()))
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]bool{
		"trace_id": true, "conn_id": true, "seq": true, "push_seq": true,
		"entrypoint": true, "target": true, "vantage": true, "env": true, "mode": true,
		"client_ip": true, "url": true, "server_pod": true, "server_name": true,
	}
	seen := 0
	for name, mf := range families {
		if strings.HasPrefix(name, "wsmon_server_") {
			seen++
		}
		for _, m := range mf.GetMetric() {
			zone := ""
			for _, lp := range m.GetLabel() {
				if forbidden[lp.GetName()] {
					t.Errorf("%s has forbidden label %s=%q", name, lp.GetName(), lp.GetValue())
				}
				if lp.GetName() == "zone" {
					zone = lp.GetValue()
				}
			}
			if zone != testLabels["zone"] {
				t.Errorf("%s: WSMON_LABELS zone=%q, want %q", name, zone, testLabels["zone"])
			}
		}
	}
	if seen < 8 {
		t.Errorf("only %d wsmon_server_* families exposed", seen)
	}
	for _, want := range []string{"go_gc_duration_seconds", "go_goroutines"} {
		if _, ok := families[want]; !ok {
			t.Errorf("Go runtime metric %s missing", want)
		}
	}
}

// TC-TEL-05: ws.echo is a child of the client's span via the body's
// traceparent, and carries the expected attributes.
func TestTel05EchoSpanIsChildOfClientSpan(t *testing.T) {
	e := start(t, withTestLabels)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	p := c.NextProbe(wstest.ClientSkyEdge)
	echo, _, err := c.RoundTrip(ctxT(t, 5*time.Second), p)
	if err != nil {
		t.Fatal(err)
	}

	spans := spansNamed(e, telemetry.SpanEcho)
	if len(spans) != 1 {
		t.Fatalf("got %d ws.echo spans", len(spans))
	}
	s := spans[0]
	if s.SpanContext().TraceID().String() != p.TraceID {
		t.Errorf("trace id %s, want %s", s.SpanContext().TraceID(), p.TraceID)
	}
	if s.Parent().SpanID().String() != p.SpanID || !s.Parent().IsRemote() {
		t.Errorf("parent %s (remote=%v), want remote %s", s.Parent().SpanID(), s.Parent().IsRemote(), p.SpanID)
	}
	a := attrMap(s)
	for _, k := range []attribute.Key{
		telemetry.AttrConnID, telemetry.AttrMsgSeq, telemetry.AttrServerName,
		telemetry.AttrServerVersion, servertel.AttrHandleMs,
	} {
		if _, ok := a[k]; !ok {
			t.Errorf("ws.echo missing attribute %s", k)
		}
	}
	if a[telemetry.AttrConnID].AsString() != c.ConnID || a[telemetry.AttrMsgSeq].AsInt64() != p.Seq {
		t.Errorf("wrong conn.id/msg.seq: %v", a)
	}
	if a[telemetry.AttrServerName].AsString() != testIdentity.Name || a["wsmon.zone"].AsString() != testLabels["zone"] {
		t.Errorf("wrong server.name/wsmon.zone: %v", a)
	}
	// The span covers exactly t2..t3.
	if s.StartTime().UnixNano() != echo.ServerRecvEpochNs || s.EndTime().UnixNano() != echo.ServerSendEpochNs {
		t.Errorf("span %d..%d, echo t2..t3 %d..%d", s.StartTime().UnixNano(), s.EndTime().UnixNano(), echo.ServerRecvEpochNs, echo.ServerSendEpochNs)
	}
	assertNoEntrypointAttrs(t, s)
}

// The client's sampling decision is respected: an unsampled parent yields no
// server span.
func TestTelUnsampledParentIsRespected(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	p := c.NextProbe(wstest.ClientSkyEdge)
	p.Traceparent = strings.TrimSuffix(p.Traceparent, "-01") + "-00"
	if _, _, err := c.RoundTrip(ctxT(t, 5*time.Second), p); err != nil {
		t.Fatal(err)
	}
	if n := len(spansNamed(e, telemetry.SpanEcho)); n != 0 {
		t.Errorf("%d spans recorded for an unsampled parent", n)
	}
}

// Error paths are always sampled.
func TestTelErrorPathIsSampled(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{})
	p := c.NextProbe(wstest.ClientSkyEdge)
	p.Traceparent = strings.TrimSuffix(p.Traceparent, "-01") + "-00"
	if _, _, err := c.RoundTrip(ctxT(t, 5*time.Second), p); err != nil {
		t.Fatal(err)
	}
	if err := c.Send(ctxT(t, 5*time.Second), p); err != nil { // same seq again
		t.Fatal(err)
	}
	expectError(t, c, protocol.ErrSeqRegression)
	spans := spansNamed(e, telemetry.SpanEcho)
	if len(spans) != 1 || attrMap(spans[0])[servertel.AttrErrorCode].AsString() != string(protocol.ErrSeqRegression) {
		t.Fatalf("want one seq_regression span, got %d", len(spans))
	}
}

// TC-TEL-06: a connection is an attribute, never a span, and no span lasts
// longer than one round trip.
func TestTel06NoConnectionLongSpans(t *testing.T) {
	t.Parallel()
	hold := dur(2*time.Minute, 2*time.Second)
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{PushIntervalMs: 200})
	stop := time.Now().Add(hold)
	for time.Now().Before(stop) {
		typ, b, err := c.RecvType(ctxT(t, 5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if typ == protocol.TypePush {
			var p protocol.Push
			_ = json.Unmarshal(b, &p)
			_ = c.Send(ctxT(t, 5*time.Second), protocol.PushAck{V: 1, Type: protocol.TypePushAck, TraceID: p.TraceID, ConnID: p.ConnID, PushSeq: p.PushSeq})
			_ = c.Send(ctxT(t, 5*time.Second), c.NextProbe(wstest.ClientSkyEdge))
		}
	}
	c.Close()
	time.Sleep(100 * time.Millisecond)

	ended := e.spans.Ended()
	if len(ended) == 0 {
		t.Fatal("no spans")
	}
	for _, s := range ended {
		if s.Name() != telemetry.SpanEcho && s.Name() != telemetry.SpanPush {
			t.Errorf("unexpected span %q", s.Name())
		}
		if d := s.EndTime().Sub(s.StartTime()); d > time.Second {
			t.Errorf("%s lasted %v", s.Name(), d)
		}
		if _, ok := attrMap(s)[telemetry.AttrConnID]; !ok {
			t.Errorf("%s lacks conn.id", s.Name())
		}
	}
	if n := len(e.spans.Started()) - len(ended); n != 0 {
		t.Errorf("%d spans never ended", n)
	}
}

// TC-TEL-10: the push frame's traceparent is the ws.push span's own context,
// so the client's ws.push_ack becomes its child.
func TestTel10PushSpanRootsPushTrace(t *testing.T) {
	e := start(t, nil)
	c, _ := e.connect(wstest.ClientSkyEdge, protocol.Options{PushIntervalMs: 100})
	typ, b, err := c.RecvType(ctxT(t, 5*time.Second))
	if err != nil || typ != protocol.TypePush {
		t.Fatalf("%v %s", err, b)
	}
	var p protocol.Push
	_ = json.Unmarshal(b, &p)
	if err := c.Send(ctxT(t, 5*time.Second), protocol.PushAck{V: 1, Type: protocol.TypePushAck, TraceID: p.TraceID, ConnID: p.ConnID, PushSeq: p.PushSeq, ClientRecvEpochNs: 1}); err != nil {
		t.Fatal(err)
	}
	var span sdktrace.ReadOnlySpan
	eventually(t, 2*time.Second, func() bool {
		for _, s := range spansNamed(e, telemetry.SpanPush) {
			if attrMap(s)[telemetry.AttrPushSeq].AsInt64() == p.PushSeq {
				span = s
				return true
			}
		}
		return false
	}, "ws.push span for push_seq 1 never ended")

	sc := span.SpanContext()
	if sc.TraceID().String() != p.TraceID || sc.SpanID().String() != p.SpanID {
		t.Errorf("push carries %s/%s, span is %s/%s", p.TraceID, p.SpanID, sc.TraceID(), sc.SpanID())
	}
	if want := "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-01"; p.Traceparent != want {
		t.Errorf("traceparent %s, want %s", p.Traceparent, want)
	}
	if span.Parent().IsValid() {
		t.Error("ws.push must be a root span")
	}
	a := attrMap(span)
	for _, k := range []attribute.Key{
		telemetry.AttrConnID, telemetry.AttrPushSeq, telemetry.AttrServerName,
		telemetry.AttrServerVersion, servertel.AttrPushAckMs,
	} {
		if _, ok := a[k]; !ok {
			t.Errorf("ws.push missing attribute %s", k)
		}
	}
	assertNoEntrypointAttrs(t, span)
}
