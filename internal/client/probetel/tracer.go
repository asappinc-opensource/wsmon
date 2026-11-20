package probetel

import (
	"context"
	crand "crypto/rand"
	"sort"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
)

// Span names, from internal/telemetry. There is deliberately no span
// for a connection's lifetime, an idle silence, or a close.
const (
	SpanRoundTrip = telemetry.SpanRoundTrip
	SpanConnect   = telemetry.SpanConnect
	SpanPushAck   = telemetry.SpanPushAck
)

// Client-only span attribute keys. Universal labels become
// wsmon.<name>. Keys shared with the server are in internal/telemetry.
const (
	AttrMode             = attribute.Key("wsmon.mode")
	AttrPayload          = attribute.Key("wsmon.payload")
	AttrIdleBucket       = attribute.Key("wsmon.idle_bucket")
	AttrRTTMs            = attribute.Key("wsmon.rtt_ms")
	AttrRTTNetMs         = attribute.Key("wsmon.rtt_net_ms")
	AttrServerProcMs     = attribute.Key("wsmon.server_processing_ms")
	AttrPeerIP           = attribute.Key("net.peer.ip")
	AttrHostPort         = attribute.Key("net.host.port")
	AttrTLSVersion       = attribute.Key("tls.protocol.version")
	AttrTLSCipher        = attribute.Key("tls.cipher")
	AttrFailureReason    = attribute.Key("wsmon.connect.failure_reason")
	AttrClientRecvEpochN = attribute.Key("client.recv_epoch_ns")
	// Client additions (client-specific; span attributes cost no cardinality).
	AttrMismatched = attribute.Key("wsmon.mismatched_fields")
	AttrCloseCode  = attribute.Key("wsmon.close_code")
)

// correlationHeaders maps the 101 response headers the probe captures to
// their span attribute names.
var correlationHeaders = map[string]attribute.Key{
	"cf-ray":               "http.response.header.cf_ray",
	"x-amzn-trace-id":      "http.response.header.x_amzn_trace_id",
	"x-haproxy-request-id": "http.response.header.x_haproxy_request_id",
	"x-request-id":         "http.response.header.x_request_id",
}

// Tracer is the OpenTelemetry probe.Tracer.
//
// Sampling. Spans are created after the fact, when the outcome is
// already known, so the export decision is made before a span exists:
// a span is created only if it is head-sampled or upgraded by its outcome.
// The SDK sampler is AlwaysSample and there is no filtering span processor.
// The head decision is TraceIDRatioBased(TRACE_SAMPLE_RATIO) applied to the
// trace ID before the probe frame is serialised; it sets the traceparent
// flag.
type Tracer struct {
	enabled bool
	tracer  trace.Tracer
	tp      *sdktrace.TracerProvider
	head    sdktrace.Sampler
	slow    time.Duration
	attrs   []attribute.KeyValue // wsmon.mode + universal labels
}

var _ probe.Tracer = (*Tracer)(nil)

// idsKey carries pre-generated IDs to the IDGenerator, so ws.roundtrip gets
// exactly the IDs that went on the wire.
type idsKey struct{}

type fixedIDs struct {
	tid trace.TraceID
	sid trace.SpanID
}

type idGenerator struct{}

func (idGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	if v, ok := ctx.Value(idsKey{}).(fixedIDs); ok {
		return v.tid, v.sid
	}
	return randomTraceID(), randomSpanID()
}

func (idGenerator) NewSpanID(context.Context, trace.TraceID) trace.SpanID { return randomSpanID() }

func randomTraceID() trace.TraceID {
	var t trace.TraceID
	for !t.IsValid() {
		_, _ = crand.Read(t[:])
	}
	return t
}

func randomSpanID() trace.SpanID {
	var s trace.SpanID
	for !s.IsValid() {
		_, _ = crand.Read(s[:])
	}
	return s
}

// newTracer exports over OTLP gRPC to OTLP_ENDPOINT, or to exp when given
// (tests). With neither, tracing is disabled: IDs still come from
// crypto/rand and every traceparent carries -00.
func newTracer(ctx context.Context, cfg config.Client, version string, exp sdktrace.SpanExporter) (*Tracer, error) {
	t := &Tracer{
		head:  sdktrace.TraceIDRatioBased(cfg.TraceSampleRatio),
		slow:  cfg.TraceSlowThreshold,
		attrs: append([]attribute.KeyValue{AttrMode.String(cfg.Mode)}, telemetry.LabelAttrs(cfg.Labels)...),
	}
	var proc sdktrace.SpanProcessor
	switch {
	case exp != nil:
		proc = sdktrace.NewSimpleSpanProcessor(exp)
	case cfg.OTLPEndpoint != "":
		// Same exporter as the server (internal/telemetry).
		e, err := telemetry.NewOTLPExporter(ctx, cfg.OTLPEndpoint)
		if err != nil {
			return nil, err
		}
		proc = sdktrace.NewBatchSpanProcessor(e)
	default:
		return t, nil
	}
	res := append([]attribute.KeyValue{
		attribute.String("service.name", telemetry.ServiceClient),
		attribute.String("service.version", version),
		attribute.String("service.instance.id", cfg.Name),
	}, telemetry.LabelAttrs(cfg.Labels)...)
	t.tp = sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(proc),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithIDGenerator(idGenerator{}),
		sdktrace.WithResource(resource.NewSchemaless(res...)),
	)
	t.tracer = t.tp.Tracer(telemetry.ServiceClient)
	t.enabled = true
	return t, nil
}

// Shutdown flushes buffered spans.
func (t *Tracer) Shutdown(ctx context.Context) error {
	if t.tp == nil {
		return nil
	}
	return t.tp.Shutdown(ctx)
}

func (t *Tracer) NewIDs() (string, string) {
	return randomTraceID().String(), randomSpanID().String()
}

// HeadDecision is false whenever tracing is disabled, so the server skips
// ws.echo too.
func (t *Tracer) HeadDecision(traceID string) bool {
	if !t.enabled {
		return false
	}
	tid, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		return false
	}
	return t.headSampled(tid)
}

func (t *Tracer) headSampled(tid trace.TraceID) bool {
	return t.head.ShouldSample(sdktrace.SamplingParameters{TraceID: tid}).Decision == sdktrace.RecordAndSample
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// forcedCloseCodes are closes whose in-flight round trips are always
// exported.
var forcedCloseCodes = map[int]bool{
	protocol.CloseAbnormal: true, protocol.CloseTooBig: true, protocol.CloseInternalError: true,
}

// RoundTrip emits ws.roundtrip from t1 to t4 (or to the loss deadline /
// disconnect) when the round trip is head-sampled, lost, mismatched, slower
// than TRACE_SLOW_THRESHOLD, or in flight at a 1006/1009/1011 close.
// The returned exemplar is set exactly when the span is exported.
func (t *Tracer) RoundTrip(info probe.RoundTripInfo) probe.Exemplar {
	if !t.enabled {
		return probe.Exemplar{}
	}
	export := info.Sampled || info.Outcome != probe.OutcomeOK ||
		(info.HaveRTT && info.RTT > t.slow) || forcedCloseCodes[info.CloseCode]
	if !export {
		return probe.Exemplar{}
	}
	tid, err1 := trace.TraceIDFromHex(info.TraceID)
	sid, err2 := trace.SpanIDFromHex(info.SpanID)
	if err1 != nil || err2 != nil {
		return probe.Exemplar{}
	}
	attrs := make([]attribute.KeyValue, 0, len(t.attrs)+14)
	attrs = append(attrs, t.attrs...)
	attrs = append(attrs, AttrPayload.String(string(info.Payload)),
		telemetry.AttrConnID.String(info.ConnID), telemetry.AttrMsgSeq.Int64(info.Seq),
		telemetry.AttrOutcome.String(string(info.Outcome)))
	if info.IdleBucket != "" {
		attrs = append(attrs, AttrIdleBucket.String(info.IdleBucket))
	}
	if info.HaveRTT {
		attrs = append(attrs, AttrRTTMs.Float64(ms(info.RTT)), AttrRTTNetMs.Float64(ms(info.RTTNet)),
			AttrServerProcMs.Float64(ms(info.ServerTime)))
	}
	attrs = append(attrs, telemetry.ServerAttrs(info.Server)...)
	if len(info.Mismatched) > 0 {
		fs := make([]string, len(info.Mismatched))
		for i, f := range info.Mismatched {
			fs[i] = string(f)
		}
		attrs = append(attrs, AttrMismatched.StringSlice(fs))
	}
	if info.CloseCode != 0 {
		attrs = append(attrs, AttrCloseCode.Int(info.CloseCode))
	}
	ctx := context.WithValue(context.Background(), idsKey{}, fixedIDs{tid, sid})
	_, span := t.tracer.Start(ctx, SpanRoundTrip,
		trace.WithNewRoot(), trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(info.Start), trace.WithAttributes(attrs...))
	if info.Outcome != probe.OutcomeOK {
		span.SetStatus(codes.Error, string(info.Outcome))
	}
	span.End(trace.WithTimestamp(info.End))
	return probe.Exemplar{TraceID: info.TraceID}
}

// Connect emits ws.connect with one child per recorded phase. The span
// carries info's pre-generated IDs (the ones sent in the upgrade request's
// traceparent); without them it gets fresh ones. Failures are always
// exported; successes follow the head decision on the span's trace ID.
func (t *Tracer) Connect(info probe.ConnectInfo) {
	if !t.enabled {
		return
	}
	tid, err1 := trace.TraceIDFromHex(info.TraceID)
	sid, err2 := trace.SpanIDFromHex(info.SpanID)
	sampled := info.Sampled
	if err1 != nil || err2 != nil {
		tid, sid = randomTraceID(), randomSpanID()
		sampled = t.headSampled(tid)
	}
	if info.Failure == "" && !sampled {
		return
	}
	attrs := make([]attribute.KeyValue, 0, len(t.attrs)+16)
	attrs = append(attrs, t.attrs...)
	if info.ConnID != "" {
		attrs = append(attrs, telemetry.AttrConnID.String(info.ConnID))
	}
	hdrs := make([]string, 0, len(info.Headers))
	for h := range info.Headers {
		hdrs = append(hdrs, h)
	}
	sort.Strings(hdrs)
	for _, h := range hdrs {
		if k, ok := correlationHeaders[h]; ok {
			attrs = append(attrs, k.String(info.Headers[h]))
		}
	}
	if info.PeerIP != "" {
		attrs = append(attrs, AttrPeerIP.String(info.PeerIP))
	}
	if info.LocalPort != 0 {
		attrs = append(attrs, AttrHostPort.Int(info.LocalPort))
	}
	if info.TLSVersion != "" { // omitted for ws://
		attrs = append(attrs, AttrTLSVersion.String(info.TLSVersion), AttrTLSCipher.String(info.TLSCipher))
	}
	if info.Failure != "" {
		attrs = append(attrs, AttrFailureReason.String(string(info.Failure)))
	}
	attrs = append(attrs, telemetry.ServerAttrs(info.Server)...)

	ctx := context.WithValue(context.Background(), idsKey{}, fixedIDs{tid, sid})
	ctx, span := t.tracer.Start(ctx, SpanConnect,
		trace.WithNewRoot(), trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(info.Start), trace.WithAttributes(attrs...))
	for _, ph := range info.Phases {
		_, c := t.tracer.Start(ctx, string(ph.Phase), trace.WithTimestamp(ph.Start))
		c.End(trace.WithTimestamp(ph.End))
	}
	if info.Failure != "" {
		span.SetStatus(codes.Error, string(info.Failure))
	}
	span.End(trace.WithTimestamp(info.End))
}

// PushAck emits ws.push_ack as a child of the server's ws.push, using the
// push frame's traceparent as the remote parent. It follows the
// parent's sampling decision; a push with no valid traceparent cannot be
// parented and gets no span.
func (t *Tracer) PushAck(info probe.PushAckInfo) {
	if !t.enabled {
		return
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(),
		propagation.MapCarrier{"traceparent": info.Traceparent})
	parent := trace.SpanContextFromContext(ctx)
	if !parent.IsValid() || !parent.IsSampled() {
		return
	}
	attrs := make([]attribute.KeyValue, 0, len(t.attrs)+3)
	attrs = append(attrs, t.attrs...)
	attrs = append(attrs, telemetry.AttrConnID.String(info.ConnID), telemetry.AttrPushSeq.Int64(info.PushSeq),
		AttrClientRecvEpochN.Int64(info.ClientRecvEpochNs))
	_, span := t.tracer.Start(ctx, SpanPushAck,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithTimestamp(info.Start), trace.WithAttributes(attrs...))
	span.End(trace.WithTimestamp(info.End))
}
