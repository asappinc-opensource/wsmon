package servertel

import (
	"context"
	"math/rand/v2"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
)

// Server-only span attribute keys. Shared keys (conn.id, msg.seq,
// push.seq, server.*, wsmon.outcome) are in internal/telemetry. There is
// deliberately no entry-point key: the server does not know the entry point
// and MUST NOT infer it.
const (
	AttrHandleMs  = attribute.Key("wsmon.handle_ms")
	AttrPushAckMs = attribute.Key("wsmon.push_ack_ms")
	AttrErrorCode = attribute.Key("wsmon.error_code")
)

// PushAckTimeout bounds a ws.push span whose push_ack never arrives, so no
// span outlives one round trip.
const PushAckTimeout = 30 * time.Second

// Tracing emits the server's ws.echo and ws.push spans. The zero value is not
// usable; construct with NewTracing or NewTracingWithProvider.
type Tracing struct {
	tracer   trace.Tracer
	enabled  bool
	identity []attribute.KeyValue
	shutdown func(context.Context) error
}

// NewTracing exports spans over OTLP gRPC to endpoint. An empty endpoint
// disables tracing; push frames still carry well-formed, unsampled trace
// context.
func NewTracing(ctx context.Context, endpoint string, sampleRatio float64, id protocol.ServerInfo, labels map[string]string) (*Tracing, error) {
	if endpoint == "" {
		return NewTracingWithProvider(noop.NewTracerProvider(), id, labels), nil
	}
	exp, err := telemetry.NewOTLPExporter(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(NewSampler(sampleRatio)),
		sdktrace.WithResource(resource.NewSchemaless(append([]attribute.KeyValue{
			attribute.String("service.name", telemetry.ServiceServer),
			attribute.String("service.version", id.Version),
			attribute.String("service.instance.id", id.Name),
		}, telemetry.LabelAttrs(labels)...)...)),
	)
	t := NewTracingWithProvider(tp, id, labels)
	t.shutdown = tp.Shutdown
	return t, nil
}

// NewTracingWithProvider uses an existing provider, e.g. an in-memory one in
// tests. Tracing counts as enabled unless tp is a noop provider. labels
// (WSMON_LABELS) go on every span, as on the client.
func NewTracingWithProvider(tp trace.TracerProvider, id protocol.ServerInfo, labels map[string]string) *Tracing {
	_, isNoop := tp.(noop.TracerProvider)
	return &Tracing{
		tracer:   tp.Tracer(telemetry.ServiceServer),
		enabled:  !isNoop,
		identity: append(telemetry.ServerAttrs(id), telemetry.LabelAttrs(labels)...),
		shutdown: func(context.Context) error { return nil },
	}
}

// Shutdown flushes buffered spans.
func (t *Tracing) Shutdown(ctx context.Context) error { return t.shutdown(ctx) }

// NewSampler respects the client's sampling decision for remote children,
// head-samples spans the server roots at ratio, and always samples spans
// that start with a non-"ok" wsmon.outcome.
func NewSampler(ratio float64) sdktrace.Sampler {
	return outcomeSampler{base: sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))}
}

type outcomeSampler struct{ base sdktrace.Sampler }

func (s outcomeSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	for _, a := range p.Attributes {
		if a.Key == telemetry.AttrOutcome && a.Value.AsString() != "ok" {
			return sdktrace.SamplingResult{
				Decision:   sdktrace.RecordAndSample,
				Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
			}
		}
	}
	return s.base.ShouldSample(p)
}

func (s outcomeSampler) Description() string {
	return "WsmonOutcome{" + s.base.Description() + "}"
}

// Echo records a ws.echo span covering t2..t3, parented on the probe's
// traceparent. It runs after the echo is written, so it never sits
// between t2 and t3. It reports whether the span was sampled, which decides
// whether the handle-time observation gets an exemplar.
func (t *Tracing) Echo(traceparent, connID string, seq int64, t2, t3 time.Time) bool {
	if !t.enabled {
		return false
	}
	parent, ok := ParseTraceparent(traceparent)
	if ok && !parent.IsSampled() {
		// ParentBased would drop it; skip building attributes.
		return false
	}
	ctx := context.Background()
	if ok {
		ctx = trace.ContextWithRemoteSpanContext(ctx, parent)
	}
	attrs := make([]attribute.KeyValue, 0, 8)
	attrs = append(attrs, telemetry.AttrConnID.String(connID), telemetry.AttrMsgSeq.Int64(seq))
	attrs = append(attrs, t.identity...)
	attrs = append(attrs, AttrHandleMs.Float64(float64(t3.Sub(t2))/float64(time.Millisecond)))
	_, span := t.tracer.Start(ctx, telemetry.SpanEcho,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithTimestamp(t2),
		trace.WithAttributes(attrs...),
	)
	sampled := span.SpanContext().IsSampled()
	span.End(trace.WithTimestamp(t3))
	return sampled
}

// EchoError records a ws.echo span for a probe that was answered with an
// error frame instead of an echo. Error paths are always sampled.
func (t *Tracing) EchoError(traceparent, connID string, seq int64, at time.Time, code protocol.ErrorCode) {
	if !t.enabled {
		return
	}
	ctx := context.Background()
	if parent, ok := ParseTraceparent(traceparent); ok {
		ctx = trace.ContextWithRemoteSpanContext(ctx, parent)
	}
	attrs := make([]attribute.KeyValue, 0, 8)
	attrs = append(attrs, telemetry.AttrOutcome.String(string(code)), AttrErrorCode.String(string(code)), telemetry.AttrConnID.String(connID), telemetry.AttrMsgSeq.Int64(seq))
	attrs = append(attrs, t.identity...)
	_, span := t.tracer.Start(ctx, telemetry.SpanEcho,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithTimestamp(at),
		trace.WithAttributes(attrs...),
	)
	span.SetStatus(codes.Error, string(code))
	span.End(trace.WithTimestamp(at))
}

// PushSpan is an in-flight ws.push span.
type PushSpan struct {
	span trace.Span
	sent time.Time
}

// StartPush roots a ws.push span for one push frame and returns the trace
// context the frame must carry. When tracing is disabled the IDs are
// random and the sampled flag is clear.
func (t *Tracing) StartPush(connID string, pushSeq int64, now time.Time) (ps PushSpan, traceID, spanID, traceparent string) {
	ps.sent = now
	var sc trace.SpanContext
	if t.enabled {
		attrs := make([]attribute.KeyValue, 0, 6)
		attrs = append(attrs, telemetry.AttrConnID.String(connID), telemetry.AttrPushSeq.Int64(pushSeq))
		attrs = append(attrs, t.identity...)
		_, ps.span = t.tracer.Start(context.Background(), telemetry.SpanPush,
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithTimestamp(now),
			trace.WithAttributes(attrs...),
		)
		sc = ps.span.SpanContext()
	}
	if !sc.IsValid() {
		var tid trace.TraceID
		var sid trace.SpanID
		putUint64(tid[:8], rand.Uint64())
		putUint64(tid[8:], rand.Uint64()|1)
		putUint64(sid[:], rand.Uint64()|1)
		sc = trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid})
	}
	traceID, spanID = sc.TraceID().String(), sc.SpanID().String()
	return ps, traceID, spanID, protocol.FormatTraceparent(traceID, spanID, sc.IsSampled())
}

// Acked ends the span when the push_ack arrives. rtt is the server-clock
// interval also observed in wsmon_server_push_ack_seconds.
func (ps PushSpan) Acked(at time.Time, rtt time.Duration) {
	if ps.span == nil {
		return
	}
	ps.span.SetAttributes(AttrPushAckMs.Float64(float64(rtt) / float64(time.Millisecond)))
	ps.span.End(trace.WithTimestamp(at))
}

// Lost ends the span of a push whose ack never arrived. The end timestamp is
// capped at PushAckTimeout after the send regardless of when this runs.
func (ps PushSpan) Lost(now time.Time) {
	if ps.span == nil {
		return
	}
	end := ps.sent.Add(PushAckTimeout)
	if now.Before(end) {
		end = now
	}
	ps.span.SetStatus(codes.Error, "push_ack not received")
	ps.span.End(trace.WithTimestamp(end))
}

// ParseTraceparent parses a W3C traceparent header value.
func ParseTraceparent(s string) (trace.SpanContext, bool) {
	// version "00" is exactly 55 chars; later versions may append fields.
	if len(s) < 55 || s[2] != '-' || s[35] != '-' || s[52] != '-' || (len(s) > 55 && s[55] != '-') {
		return trace.SpanContext{}, false
	}
	if s[:2] == "ff" || (s[:2] == "00" && len(s) != 55) {
		return trace.SpanContext{}, false
	}
	tid, err := trace.TraceIDFromHex(s[3:35])
	if err != nil {
		return trace.SpanContext{}, false
	}
	sid, err := trace.SpanIDFromHex(s[36:52])
	if err != nil {
		return trace.SpanContext{}, false
	}
	flags, ok := hexByte(s[53], s[54])
	if !ok {
		return trace.SpanContext{}, false
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.TraceFlags(flags) & trace.FlagsSampled,
		Remote:     true,
	})
	return sc, sc.IsValid()
}

func hexByte(hi, lo byte) (byte, bool) {
	h, ok1 := hexNibble(hi)
	l, ok2 := hexNibble(lo)
	return h<<4 | l, ok1 && ok2
}

func hexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	}
	return 0, false
}

func putUint64(b []byte, v uint64) {
	for i := 7; i >= 0; i-- {
		b[i] = byte(v)
		v >>= 8
	}
}

// Enabled reports whether spans are exported. Callers use it to skip work,
// such as decoding span attributes, that only matters when tracing is on.
func (t *Tracing) Enabled() bool { return t.enabled }
