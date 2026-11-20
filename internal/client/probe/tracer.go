package probe

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// Outcome is the result of one round trip, as reported to the tracer.
type Outcome string

const (
	OutcomeOK         Outcome = "ok"
	OutcomeLost       Outcome = "lost"
	OutcomeMismatched Outcome = "mismatched"
	// There is no "late" round-trip span: a late echo belongs to a probe
	// whose span was already emitted as lost at its deadline, and a second
	// span with the same span ID would be malformed.
)

// RoundTripInfo describes one finished round trip. The span is created after
// the fact with explicit timestamps: no span may outlive one round trip.
type RoundTripInfo struct {
	ConnID     string
	Seq        int64
	Payload    Payload
	IdleBucket string // "" outside idle mode
	TraceID    string
	SpanID     string
	Sampled    bool // the head decision carried in the traceparent

	Start time.Time // t1
	End   time.Time // t4, or the loss deadline / disconnect time

	Outcome Outcome
	// HaveRTT is false when the outcome is not ok, or when the sample was
	// discarded as rtt_negative.
	HaveRTT    bool
	RTT        time.Duration
	RTTNet     time.Duration
	ServerTime time.Duration
	Mismatched []Field
	Server     protocol.ServerInfo // zero unless an echo arrived
	// CloseCode is set when the round trip was lost because its connection
	// closed (the close code, 1006 if no close frame); 0 otherwise.
	CloseCode int
}

// PhaseSpan is one handshake phase with its start and end.
type PhaseSpan struct {
	Phase      Phase
	Start, End time.Time
}

// ConnectInfo describes one connection attempt (the ws.connect span).
type ConnectInfo struct {
	ConnID string // "" if the attempt failed before hello
	// TraceID and SpanID are the ws.connect span's IDs, generated before
	// the dial so the upgrade request's traceparent header can carry them.
	// Sampled is the head decision on TraceID, the header's sampled flag.
	TraceID string
	SpanID  string
	Sampled bool
	Start   time.Time
	End     time.Time
	Phases  []PhaseSpan
	Failure Reason // "" on success
	// Headers holds the correlation headers present on the 101 response,
	// keyed by lower-case name: cf-ray, x-amzn-trace-id,
	// x-haproxy-request-id, x-request-id.
	Headers    map[string]string
	PeerIP     string
	LocalPort  int
	TLSVersion string // "" for ws://
	TLSCipher  string // "" for ws://
	Server     protocol.ServerInfo
}

// PushAckInfo describes one push → push_ack (the ws.push_ack span).
type PushAckInfo struct {
	// Traceparent is the push frame's traceparent: the remote parent.
	Traceparent       string
	TraceID           string
	ConnID            string
	PushSeq           int64
	ClientRecvEpochNs int64
	Start, End        time.Time
}

// Tracer is the tracing seam, implemented with OpenTelemetry by
// internal/telemetry. Implementations must be safe for concurrent use and
// must not block.
type Tracer interface {
	// NewIDs returns a trace ID (32 lowercase hex) and span ID (16
	// lowercase hex), never all-zero. The OTel implementation returns the
	// IDs its ws.roundtrip span will carry, so the server's ws.echo becomes
	// its child.
	NewIDs() (traceID, spanID string)
	// HeadDecision is called before serialisation; the result sets the
	// traceparent sampled flag.
	HeadDecision(traceID string) bool
	// RoundTrip is called exactly once per sent probe, at completion or at
	// the loss deadline / disconnect. It returns the exemplar to attach to
	// the RTT observation.
	RoundTrip(RoundTripInfo) Exemplar
	Connect(ConnectInfo)
	PushAck(PushAckInfo)
}

// NopTracer generates real IDs and records nothing. The head decision is
// always "not sampled".
type NopTracer struct{}

var _ Tracer = NopTracer{}

func (NopTracer) NewIDs() (string, string) { return randomHex(16), randomHex(8) }
func (NopTracer) HeadDecision(string) bool { return false }
func (NopTracer) RoundTrip(RoundTripInfo) Exemplar {
	return Exemplar{}
}
func (NopTracer) Connect(ConnectInfo) {}
func (NopTracer) PushAck(PushAckInfo) {}

// randomHex returns n random bytes as lowercase hex, never all-zero.
func randomHex(n int) string {
	b := make([]byte, n)
	for {
		_, _ = rand.Read(b)
		for _, c := range b {
			if c != 0 {
				return hex.EncodeToString(b)
			}
		}
	}
}
