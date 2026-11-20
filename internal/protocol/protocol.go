// Package protocol is the single source of truth for the wsmon.v1 wire
// contract. Both `wsmon server` and `wsmon client` import it: every
// frame struct, enum, limit and encoder/decoder lives here and nowhere else,
// so the two roles cannot drift apart.
//
// Files are split by direction:
//
//	protocol.go        types, enums, close codes and shared limits
//	epoch.go           the patched 20-byte timestamp slot (t1, t3)
//	encode_client.go   client → server encoders (hello, probe, push_ack)
//	codec_server.go    server-side decoder of client → server frames and
//	                   server → client encoders (hello_ack, echo, push, error)
//	decode_client.go   client-side decoder of server → client frames
//	ids.go             trace context and ULID helpers
package protocol

import "time"

// Version is the wire protocol version carried in every frame's "v" field.
const Version = 1

// Subprotocol is the WebSocket subprotocol both sides MUST negotiate.
const Subprotocol = "wsmon.v1"

// DefaultWSPath is the path the server serves upgrades on by default.
const DefaultWSPath = "/__synthetic/ws"

// Frame types.
const (
	TypeHello    = "hello"
	TypeHelloAck = "hello_ack"
	TypeProbe    = "probe"
	TypeEcho     = "echo"
	TypePush     = "push"
	TypePushAck  = "push_ack"
	TypeError    = "error"
)

// Limits shared by both roles. The server enforces them; the client validates
// its configuration against the same constants at startup, so a client
// configuration the server would reject or silently adjust is refused before
// it ever connects.
const (
	// MaxMsgsPerSec is the default per-connection message cap. The server's WSMON_MAX_MSGS_PER_SEC defaults to it and
	// the client keeps its own send rate at or below it.
	MaxMsgsPerSec = 200

	// MinPushInterval is the smallest push interval the server honours. A
	// smaller request is raised to it and reported in accepted_options,
	// which the client treats as options_rejected.
	MinPushInterval = 100 * time.Millisecond

	// MaxPushInterval is the largest push interval the server honours.
	MaxPushInterval = time.Hour

	// DefaultMaxFrameBytes is the server's default inbound frame limit.
	// The largest client probe must fit under it.
	DefaultMaxFrameBytes = 1 << 20
)

// HonourOptions returns the options the server applies for a hello that
// requested req. Whatever it returns is exactly what hello_ack reports.
// The client requires its requested options to be a fixed point of
// HonourOptions, which is what config validation checks.
func HonourOptions(req Options) Options {
	acc := Options{ServerKeepalive: req.ServerKeepalive}
	if req.PushIntervalMs > 0 {
		acc.PushIntervalMs = min(max(req.PushIntervalMs, MinPushInterval.Milliseconds()), MaxPushInterval.Milliseconds())
	}
	return acc
}

// ErrorCode is the closed enum carried in error frames. It becomes a
// metric label on both sides, so do not invent codes.
type ErrorCode string

const (
	ErrBadVersion    ErrorCode = "bad_version"
	ErrBadJSON       ErrorCode = "bad_json"
	ErrBadType       ErrorCode = "bad_type"
	ErrMissingField  ErrorCode = "missing_field"
	ErrSeqRegression ErrorCode = "seq_regression"
	ErrTooLarge      ErrorCode = "too_large"
	ErrNoHello       ErrorCode = "no_hello"
	ErrRateLimited   ErrorCode = "rate_limited"
	ErrShuttingDown  ErrorCode = "shutting_down"
)

// ErrorCodes lists every member of the enum, in a fixed order.
var ErrorCodes = []ErrorCode{
	ErrBadVersion, ErrBadJSON, ErrBadType, ErrMissingField, ErrSeqRegression,
	ErrTooLarge, ErrNoHello, ErrRateLimited, ErrShuttingDown,
}

// Known reports whether c is a member of the enum. The client's metric
// layer maps unknown codes to "other" to keep the label bounded.
func (c ErrorCode) Known() bool {
	for _, k := range ErrorCodes {
		if c == k {
			return true
		}
	}
	return false
}

// CloseCode returns the WebSocket close code the server sends after this
// error, or 0 if the connection stays open.
func (c ErrorCode) CloseCode() int {
	switch c {
	case ErrBadVersion, ErrBadJSON, ErrNoHello:
		return CloseProtocolError
	case ErrTooLarge:
		return CloseTooBig
	case ErrShuttingDown:
		return CloseGoingAway
	default:
		return 0
	}
}

// WebSocket close codes.
const (
	CloseNormal         = 1000
	CloseGoingAway      = 1001
	CloseProtocolError  = 1002
	CloseAbnormal       = 1006
	CloseTooBig         = 1009
	CloseInternalError  = 1011
	CloseServiceRestart = 1012
)

// CloseCodes lists every known close code. Both roles label close metrics with
// exactly these values plus "other", so a peer cannot mint label values.
var CloseCodes = []int{
	CloseNormal, CloseGoingAway, CloseProtocolError, CloseAbnormal,
	CloseTooBig, CloseInternalError, CloseServiceRestart,
}

// KnownCloseCode reports whether code is one of CloseCodes.
func KnownCloseCode(code int) bool {
	for _, k := range CloseCodes {
		if code == k {
			return true
		}
	}
	return false
}

// ClientInfo is descriptive only. The server MUST NOT branch on any of it;
// it checks only that "client" is present.
type ClientInfo struct {
	// Name is WSMON_NAME (host name if unset). Optional on the wire: older
	// clients do not send it.
	Name    string            `json:"name,omitempty"`
	Mode    string            `json:"mode"`
	Version string            `json:"version"`
	Labels  map[string]string `json:"labels,omitempty"`
}

// ServerInfo identifies the replica that handled a frame. Name is
// whatever the deployment calls this replica (WSMON_NAME); the server knows
// nothing about pods, nodes or zones.
type ServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Options are the per-connection options requested in hello and reported
// back in hello_ack.accepted_options.
type Options struct {
	ServerKeepalive bool  `json:"server_keepalive"`
	PushIntervalMs  int64 `json:"push_interval_ms"`
}

// Hello is the first frame on every connection, client → server.
type Hello struct {
	V       int        `json:"v"`
	Type    string     `json:"type"`
	ConnID  string     `json:"conn_id"`
	Client  ClientInfo `json:"client"`
	Options Options    `json:"options"`
}

// HelloAck is the server's reply to hello.
type HelloAck struct {
	V               int        `json:"v"`
	Type            string     `json:"type"`
	ConnID          string     `json:"conn_id"`
	Server          ServerInfo `json:"server"`
	AcceptedOptions Options    `json:"accepted_options"`
}

// Probe is a client → server measurement frame.
type Probe struct {
	V                 int        `json:"v"`
	Type              string     `json:"type"`
	TraceID           string     `json:"trace_id"`
	SpanID            string     `json:"span_id"`
	Traceparent       string     `json:"traceparent"`
	ConnID            string     `json:"conn_id"`
	Seq               int64      `json:"seq"`
	ClientSendEpochNs int64      `json:"client_send_epoch_ns"`
	Client            ClientInfo `json:"client"`
	Padding           string     `json:"padding,omitempty"`
}

// Echo is the server's reply to a probe. The server encodes it with
// AppendEcho rather than encoding/json so the copied fields stay verbatim.
type Echo struct {
	V                 int        `json:"v"`
	Type              string     `json:"type"`
	TraceID           string     `json:"trace_id"`
	ConnID            string     `json:"conn_id"`
	Seq               int64      `json:"seq"`
	ClientSendEpochNs int64      `json:"client_send_epoch_ns"`
	ServerRecvEpochNs int64      `json:"server_recv_epoch_ns"`
	ServerSendEpochNs int64      `json:"server_send_epoch_ns"`
	Server            ServerInfo `json:"server"`
}

// Push is an unsolicited server → client frame.
type Push struct {
	V                 int        `json:"v"`
	Type              string     `json:"type"`
	TraceID           string     `json:"trace_id"`
	SpanID            string     `json:"span_id"`
	Traceparent       string     `json:"traceparent"`
	ConnID            string     `json:"conn_id"`
	PushSeq           int64      `json:"push_seq"`
	ServerSendEpochNs int64      `json:"server_send_epoch_ns"`
	Server            ServerInfo `json:"server"`
}

// PushAck is the client's reply to a push. ClientRecvEpochNs is on the
// client clock and MUST NOT be used in any server metric.
type PushAck struct {
	V                 int    `json:"v"`
	Type              string `json:"type"`
	TraceID           string `json:"trace_id"`
	ConnID            string `json:"conn_id"`
	PushSeq           int64  `json:"push_seq"`
	ClientRecvEpochNs int64  `json:"client_recv_epoch_ns"`
}

// Error is a server → client error frame. ConnID may be empty and Seq
// may be 0 when unknown.
type Error struct {
	V       int       `json:"v"`
	Type    string    `json:"type"`
	ConnID  string    `json:"conn_id"`
	Seq     int64     `json:"seq"`
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}
