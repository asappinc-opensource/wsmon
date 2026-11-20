package otlpsink_test

import (
	"context"
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/client/probetel"
	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/test/client/otlpsink"
)

// The sink receives what the probe's own OTLP gRPC exporter sends: the
// ws.roundtrip span carries exactly the wire IDs, and ws.push_ack is a child
// of the push frame's traceparent. This is the client half of TC-TEL-05/10.
func TestSinkReceivesProbeExporter(t *testing.T) {
	sink := otlpsink.Start(t, "127.0.0.1:0")
	cfg, err := config.LoadClient(func(k string) string {
		return map[string]string{
			"WSMON_URL":                "ws://127.0.0.1:1/__synthetic/ws",
			"WSMON_LABELS":             "site=sink,path=direct",
			"WSMON_METRICS_ADDR":       "127.0.0.1:0",
			"WSMON_OTLP_ENDPOINT":      sink.Addr,
			"WSMON_TRACE_SAMPLE_RATIO": "1",
		}[k]
	})
	if err != nil {
		t.Fatal(err)
	}
	tel, err := probetel.New(context.Background(), cfg, probetel.Options{Version: "9.9.9"})
	if err != nil {
		t.Fatal(err)
	}
	tr := tel.Tracer()
	traceID, spanID := tr.NewIDs()
	if !tr.HeadDecision(traceID) {
		t.Fatal("ratio 1 must head-sample")
	}
	t1 := time.Now()
	ex := tr.RoundTrip(probe.RoundTripInfo{
		ConnID: "01J9ZQ8K3T4X5Y6Z7A8B9C0D1E", Seq: 7, Payload: probe.PayloadSmall,
		TraceID: traceID, SpanID: spanID, Sampled: true,
		Start: t1, End: t1.Add(3 * time.Millisecond), Outcome: probe.OutcomeOK,
		HaveRTT: true, RTT: 3 * time.Millisecond, RTTNet: 2 * time.Millisecond, ServerTime: time.Millisecond,
		Server: protocol.ServerInfo{Name: "p", Version: "2.0.0"},
	})
	if ex.TraceID != traceID {
		t.Errorf("exemplar %q, want %q", ex.TraceID, traceID)
	}
	const pushTrace, pushSpan = "9c1e0000000000000000000000000001", "b7ad6b7169203331"
	tr.PushAck(probe.PushAckInfo{
		Traceparent: protocol.FormatTraceparent(pushTrace, pushSpan, true), TraceID: pushTrace,
		ConnID: "c", PushSeq: 3, ClientRecvEpochNs: t1.UnixNano(), Start: t1, End: t1.Add(time.Microsecond),
	})
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !sink.WaitFor(func(s []otlpsink.Span) bool { return len(s) >= 2 }, 10*time.Second) {
		t.Fatalf("spans: %+v", sink.Spans())
	}
	rt, ok := sink.ByID(traceID, spanID)
	if !ok || rt.Name != probetel.SpanRoundTrip || rt.ParentSpanID != "" || rt.Service != "wsmon-probe" {
		t.Fatalf("ws.roundtrip with the wire IDs: %+v (found %v)", rt, ok)
	}
	if rt.Attrs["msg.seq"] != int64(7) || rt.Attrs["wsmon.site"] != "sink" || rt.Resource["wsmon.path"] != "direct" ||
		rt.Resource["service.version"] != "9.9.9" || rt.Duration() != 3*time.Millisecond {
		t.Errorf("roundtrip span %+v", rt)
	}
	pa := sink.Named(probetel.SpanPushAck)
	if len(pa) != 1 || pa[0].TraceID != pushTrace || pa[0].ParentSpanID != pushSpan || pa[0].Attrs["push.seq"] != int64(3) {
		t.Errorf("ws.push_ack %+v", pa)
	}
}
