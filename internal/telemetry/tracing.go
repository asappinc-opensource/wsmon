package telemetry

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// Service names reported in the OTel resource (and, for the client, as the
// remote-write job label).
const (
	ServiceServer = "wsmon-server"
	ServiceClient = "wsmon-probe"
)

// Span names. The server emits ws.echo and ws.push; the client emits
// ws.roundtrip (parent of ws.echo), ws.connect and ws.push_ack (child of
// ws.push).
const (
	SpanEcho      = "ws.echo"
	SpanPush      = "ws.push"
	SpanRoundTrip = "ws.roundtrip"
	SpanConnect   = "ws.connect"
	SpanPushAck   = "ws.push_ack"
)

// Span attribute keys used by both roles.
const (
	AttrConnID        = attribute.Key("conn.id")
	AttrMsgSeq        = attribute.Key("msg.seq")
	AttrPushSeq       = attribute.Key("push.seq")
	AttrServerName    = attribute.Key("server.name")
	AttrServerVersion = attribute.Key("server.version")
	// AttrOutcome is "ok" or the failure. On the server any other value
	// forces the span to be sampled.
	AttrOutcome = attribute.Key("wsmon.outcome")
)

// ServerAttrs renders the server identity as span attributes. Both the
// server (its own spans) and the client (from hello_ack and echo) use it, so
// a trace carries one spelling of the server's identity.
func ServerAttrs(s protocol.ServerInfo) []attribute.KeyValue {
	if s == (protocol.ServerInfo{}) {
		return nil
	}
	return []attribute.KeyValue{
		AttrServerName.String(s.Name), AttrServerVersion.String(s.Version),
	}
}

// LabelAttrs renders WSMON_LABELS as wsmon.<name>, sorted by name. Both roles
// put them on their resource and on every span.
func LabelAttrs(labels map[string]string) []attribute.KeyValue {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]attribute.KeyValue, 0, len(keys))
	for _, k := range keys {
		out = append(out, attribute.String("wsmon."+k, labels[k]))
	}
	return out
}

// NewOTLPExporter builds the OTLP/gRPC span exporter for WSMON_OTLP_ENDPOINT.
// A URL selects TLS by its scheme (http:// plaintext, https:// TLS); a bare
// host:port is plaintext gRPC, the in-cluster collector case.
func NewOTLPExporter(ctx context.Context, endpoint string) (sdktrace.SpanExporter, error) {
	var opts []otlptracegrpc.Option
	if strings.Contains(endpoint, "://") {
		opts = append(opts, otlptracegrpc.WithEndpointURL(endpoint))
	} else {
		opts = append(opts, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
	}
	exp, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	return exp, nil
}
