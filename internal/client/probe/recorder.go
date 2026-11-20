package probe

import (
	"time"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// Bounded enums. Each value becomes a metric label, so the sets
// are closed. Do not invent values.

// Reason is a connect failure reason.
type Reason string

const (
	ReasonDNS             Reason = "dns"
	ReasonTCP             Reason = "tcp"
	ReasonTLS             Reason = "tls"
	ReasonAuth            Reason = "auth"
	ReasonHTTPStatus      Reason = "http_status"
	ReasonUpgradeRejected Reason = "upgrade_rejected"
	ReasonSubprotocol     Reason = "subprotocol"
	ReasonOptionsRejected Reason = "options_rejected"
	ReasonTimeout         Reason = "timeout"
	ReasonOther           Reason = "other"
)

// Reasons lists every Reason in a fixed order (for series pre-registration).
var Reasons = []Reason{ReasonDNS, ReasonTCP, ReasonTLS, ReasonAuth, ReasonHTTPStatus,
	ReasonUpgradeRejected, ReasonSubprotocol, ReasonOptionsRejected, ReasonTimeout, ReasonOther}

// Phase is a handshake phase.
type Phase string

const (
	PhaseDNS     Phase = "dns"
	PhaseTCP     Phase = "tcp"
	PhaseTLS     Phase = "tls"
	PhaseUpgrade Phase = "upgrade"
	PhaseHello   Phase = "hello"
)

// Phases lists every Phase in handshake order.
var Phases = []Phase{PhaseDNS, PhaseTCP, PhaseTLS, PhaseUpgrade, PhaseHello}

// Initiator says which side closed a connection.
type Initiator string

const (
	InitiatorClient Initiator = "client"
	InitiatorRemote Initiator = "remote"
)

// Field is a mismatched echo field.
type Field string

const (
	FieldTraceID Field = "trace_id"
	FieldConnID  Field = "conn_id"
	// FieldSeq cannot arise in this implementation because the in-flight map
	// is keyed by seq; it exists so the metrics layer can pre-register the series.
	FieldSeq Field = "seq"
	FieldT1  Field = "t1"
)

// Fields lists every Field in a fixed order.
var Fields = []Field{FieldTraceID, FieldConnID, FieldSeq, FieldT1}

// AnomalyKind is a wsmon_client_time_anomaly_total kind.
type AnomalyKind string

const (
	AnomalyServerTimeNegative AnomalyKind = "server_time_negative"
	AnomalyRTTNegative        AnomalyKind = "rtt_negative"
)

// IdleResult is a wsmon_client_idle_probe_total result.
type IdleResult string

const (
	IdleSurvived IdleResult = "survived"
	IdleDropped  IdleResult = "dropped"
)

// IdleCloseNone is the close_code of an idle rung dropped by echo timeout
// with no close.
const IdleCloseNone = "none"

// Payload is the payload label.
type Payload string

const (
	PayloadSmall Payload = "small"
	PayloadLarge Payload = "large"
)

// Exemplar links a histogram observation to a sampled trace. The
// zero value means "no sampled trace".
type Exemplar struct {
	TraceID string
}

// Recorder is the metrics seam: one method per client observable.
// mode and the universal labels are constant per process and are added by
// the implementation, never passed per call. Implementations must be safe
// for concurrent use and must not block.
//
// There is deliberately no method for one-way latency (t2−t1, t4−t3):
// it is meaningless across unsynchronised clocks and is never emitted (TC-TIME-05).
type Recorder interface {
	// Connection lifecycle.
	ConnectAttempt()
	ConnectFailure(reason Reason)
	ConnectPhase(phase Phase, d time.Duration)
	// ConnectionUp reports the wsmon_client_connection_up / wsmon_client_connections_active
	// state. since is when the connection came up (for
	// wsmon_client_connection_age_seconds); it is zero when up is false.
	ConnectionUp(up bool, since time.Time)
	// Closed is called once for every closure of an upgraded connection,
	// including those that never reached hello_ack. code is the numeric
	// close code (1006 when no close frame was received); the
	// implementation maps unknown codes to "other".
	Closed(code int, initiator Initiator, lifetime time.Duration)
	Reconnect()

	// Message plane.
	Sent(p Payload)
	Echoed(p Payload)
	Lost(p Payload)
	Late(p Payload)
	Mismatched(f Field)
	Duplicate()
	OutOfOrder()
	Unexpected()
	// RTT observes wsmon_client_rtt_seconds, wsmon_client_rtt_net_seconds and
	// wsmon_client_server_processing_seconds for one delivered echo. ex carries the
	// trace ID when a sampled trace exists for the message.
	RTT(p Payload, rtt, rttNet, serverTime time.Duration, serverName string, ex Exemplar)
	BurstCompletion(p Payload, d time.Duration)
	ClockOffset(d time.Duration)
	TimeAnomaly(kind AnomalyKind)
	// ServerError receives the code verbatim; unknown codes are passed
	// through and mapped to "other" by the implementation.
	ServerError(code protocol.ErrorCode)
	PushReceived()
	PushGap(d time.Duration)
	PushLost(n int64)
	PushOutOfOrder()

	// Idle probe. bucket is config.IdleBucketLabel(rung).
	IdleProbe(bucket string, r IdleResult)
	IdleClose(bucket string, closeCode string)
}

// NopRecorder discards everything. probetel.Recorder is the Prometheus
// implementation.
type NopRecorder struct{}

var _ Recorder = NopRecorder{}

func (NopRecorder) ConnectAttempt()                                                            {}
func (NopRecorder) ConnectFailure(Reason)                                                      {}
func (NopRecorder) ConnectPhase(Phase, time.Duration)                                          {}
func (NopRecorder) ConnectionUp(bool, time.Time)                                               {}
func (NopRecorder) Closed(int, Initiator, time.Duration)                                       {}
func (NopRecorder) Reconnect()                                                                 {}
func (NopRecorder) Sent(Payload)                                                               {}
func (NopRecorder) Echoed(Payload)                                                             {}
func (NopRecorder) Lost(Payload)                                                               {}
func (NopRecorder) Late(Payload)                                                               {}
func (NopRecorder) Mismatched(Field)                                                           {}
func (NopRecorder) Duplicate()                                                                 {}
func (NopRecorder) OutOfOrder()                                                                {}
func (NopRecorder) Unexpected()                                                                {}
func (NopRecorder) RTT(Payload, time.Duration, time.Duration, time.Duration, string, Exemplar) {}
func (NopRecorder) BurstCompletion(Payload, time.Duration)                                     {}
func (NopRecorder) ClockOffset(time.Duration)                                                  {}
func (NopRecorder) TimeAnomaly(AnomalyKind)                                                    {}
func (NopRecorder) ServerError(protocol.ErrorCode)                                             {}
func (NopRecorder) PushReceived()                                                              {}
func (NopRecorder) PushGap(time.Duration)                                                      {}
func (NopRecorder) PushLost(int64)                                                             {}
func (NopRecorder) PushOutOfOrder()                                                            {}
func (NopRecorder) IdleProbe(string, IdleResult)                                               {}
func (NopRecorder) IdleClose(string, string)                                                   {}
