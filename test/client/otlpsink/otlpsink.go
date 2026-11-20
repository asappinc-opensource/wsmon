// Package otlpsink is a minimal in-memory OTLP/gRPC trace receiver for tests.
// It implements TraceService/Export and stores every span it receives, so a
// test can assert parentage across the probe and the real server (TC-TEL-05,
// TC-TEL-10 in test/client/contract). It is test code only.
package otlpsink

import (
	"context"
	"encoding/hex"
	"net"
	"sync"
	"testing"
	"time"

	collectorpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
)

// Span is a received span with its IDs in lowercase hex.
type Span struct {
	Service      string // resource service.name
	Name         string
	TraceID      string
	SpanID       string
	ParentSpanID string // "" for a root span
	Start, End   time.Time
	Attrs        map[string]any
	Resource     map[string]any
}

// Duration is End − Start.
func (s Span) Duration() time.Duration { return s.End.Sub(s.Start) }

// Sink is a running receiver.
type Sink struct {
	collectorpb.UnimplementedTraceServiceServer

	// Addr is host:port for plaintext gRPC (WSMON_OTLP_ENDPOINT).
	Addr string

	srv *grpc.Server

	mu     sync.Mutex
	spans  []Span
	notify chan struct{}
}

// Start listens on addr ("127.0.0.1:0" in-process, "0.0.0.0:4317" when a
// container on a Docker network must reach it) and stops in t.Cleanup.
func Start(t testing.TB, addr string) *Sink {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	s := &Sink{Addr: ln.Addr().String(), srv: grpc.NewServer(), notify: make(chan struct{})}
	collectorpb.RegisterTraceServiceServer(s.srv, s)
	go func() { _ = s.srv.Serve(ln) }()
	t.Cleanup(s.srv.Stop)
	return s
}

// Export implements the OTLP TraceService.
func (s *Sink) Export(_ context.Context, req *collectorpb.ExportTraceServiceRequest) (*collectorpb.ExportTraceServiceResponse, error) {
	var got []Span
	for _, rs := range req.GetResourceSpans() {
		res := attrs(rs.GetResource().GetAttributes())
		svc, _ := res["service.name"].(string)
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				got = append(got, convert(sp, svc, res))
			}
		}
	}
	s.mu.Lock()
	s.spans = append(s.spans, got...)
	close(s.notify)
	s.notify = make(chan struct{})
	s.mu.Unlock()
	return &collectorpb.ExportTraceServiceResponse{}, nil
}

func convert(sp *tracepb.Span, svc string, res map[string]any) Span {
	parent := ""
	if len(sp.GetParentSpanId()) > 0 {
		parent = hex.EncodeToString(sp.GetParentSpanId())
	}
	return Span{
		Service: svc, Name: sp.GetName(),
		TraceID: hex.EncodeToString(sp.GetTraceId()), SpanID: hex.EncodeToString(sp.GetSpanId()),
		ParentSpanID: parent,
		Start:        time.Unix(0, int64(sp.GetStartTimeUnixNano())),
		End:          time.Unix(0, int64(sp.GetEndTimeUnixNano())),
		Attrs:        attrs(sp.GetAttributes()), Resource: res,
	}
}

// Spans returns a copy of every span received so far.
func (s *Sink) Spans() []Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Span(nil), s.spans...)
}

// Named returns the received spans with this name.
func (s *Sink) Named(name string) []Span {
	var out []Span
	for _, sp := range s.Spans() {
		if sp.Name == name {
			out = append(out, sp)
		}
	}
	return out
}

// ByID finds a span by trace and span ID.
func (s *Sink) ByID(traceID, spanID string) (Span, bool) {
	for _, sp := range s.Spans() {
		if sp.TraceID == traceID && sp.SpanID == spanID {
			return sp, true
		}
	}
	return Span{}, false
}

// WaitFor blocks until cond holds over the received spans or timeout.
func (s *Sink) WaitFor(cond func([]Span) bool, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		s.mu.Lock()
		ok, ch := cond(s.spans), s.notify
		s.mu.Unlock()
		if ok {
			return true
		}
		select {
		case <-ch:
		case <-deadline:
			return false
		}
	}
}
