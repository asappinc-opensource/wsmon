package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
)

// Inbound is a single-pass view of any client → server frame. Scalar fields
// are kept as raw JSON so each can be validated on its own and, for the four
// fields the echo must carry verbatim, copied into the echo without re-encoding.
//
// Unknown fields (including padding) are ignored, as the protocol requires. An
// Inbound is reusable: Reset keeps the backing arrays so steady-state decoding
// does not allocate for these fields.
type Inbound struct {
	V                 json.RawMessage `json:"v"`
	Type              json.RawMessage `json:"type"`
	TraceID           json.RawMessage `json:"trace_id"`
	SpanID            json.RawMessage `json:"span_id"`
	Traceparent       json.RawMessage `json:"traceparent"`
	ConnID            json.RawMessage `json:"conn_id"`
	Seq               json.RawMessage `json:"seq"`
	ClientSendEpochNs json.RawMessage `json:"client_send_epoch_ns"`
	PushSeq           json.RawMessage `json:"push_seq"`
	ClientRecvEpochNs json.RawMessage `json:"client_recv_epoch_ns"`
	Client            json.RawMessage `json:"client"`
	Options           json.RawMessage `json:"options"`
}

// Reset clears every field while keeping capacity. It must be called before
// each decode: encoding/json leaves absent fields untouched.
func (in *Inbound) Reset() {
	in.V = in.V[:0]
	in.Type = in.Type[:0]
	in.TraceID = in.TraceID[:0]
	in.SpanID = in.SpanID[:0]
	in.Traceparent = in.Traceparent[:0]
	in.ConnID = in.ConnID[:0]
	in.Seq = in.Seq[:0]
	in.ClientSendEpochNs = in.ClientSendEpochNs[:0]
	in.PushSeq = in.PushSeq[:0]
	in.ClientRecvEpochNs = in.ClientRecvEpochNs[:0]
	in.Client = in.Client[:0]
	in.Options = in.Options[:0]
}

// Decode resets in and decodes b into it. A non-nil error means the frame is
// not a single valid JSON object (bad_json).
func (in *Inbound) Decode(b []byte) error {
	in.Reset()
	if t := bytes.TrimLeft(b, " \t\r\n"); len(t) == 0 || t[0] != '{' {
		return errNotObject
	}
	return json.Unmarshal(b, in)
}

var errNotObject = errors.New("frame is not a JSON object")

// FrameType returns the frame's type. present is false when "type" is absent
// or null; name is "" when it is present but not a known frame type.
func (in *Inbound) FrameType() (name string, present bool) {
	if !Present(in.Type) {
		return "", false
	}
	switch string(in.Type) {
	case `"hello"`:
		return TypeHello, true
	case `"probe"`:
		return TypeProbe, true
	case `"push_ack"`:
		return TypePushAck, true
	case `"hello_ack"`:
		return TypeHelloAck, true
	case `"echo"`:
		return TypeEcho, true
	case `"push"`:
		return TypePush, true
	case `"error"`:
		return TypeError, true
	}
	return "", true
}

// Present reports whether a raw field was supplied with a non-null value.
func Present(raw json.RawMessage) bool {
	return len(raw) > 0 && !bytes.Equal(raw, []byte("null"))
}

// IsString reports whether raw is a JSON string.
func IsString(raw json.RawMessage) bool {
	return len(raw) >= 2 && raw[0] == '"'
}

// Int64 parses raw as a JSON integer that fits in int64. Floats, exponents and
// quoted numbers are rejected so that nothing ever passes through a float.
func Int64(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	v, err := strconv.ParseInt(string(raw), 10, 64)
	return v, err == nil
}

// String decodes raw as a JSON string.
func String(raw json.RawMessage) (string, bool) {
	if !IsString(raw) {
		return "", false
	}
	if bytes.IndexByte(raw, '\\') < 0 {
		return string(raw[1 : len(raw)-1]), true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

// EncodeServerInfo pre-encodes server identity once, for use by the Append
// functions.
func EncodeServerInfo(s ServerInfo) []byte {
	b, _ := json.Marshal(s)
	return b
}

// AppendEcho appends an echo frame answering the probe in in, and returns the
// frame plus the offset of the server_send_epoch_ns slot for PatchEpochNs.
//
// trace_id, conn_id, seq and client_send_epoch_ns are copied byte-for-byte
// from the probe; the caller must have validated them. padding is
// never echoed. server is the output of EncodeServerInfo.
func AppendEcho(dst []byte, in *Inbound, t2 int64, server []byte) ([]byte, int) {
	dst = append(dst, `{"v":1,"type":"echo","trace_id":`...)
	dst = append(dst, in.TraceID...)
	dst = append(dst, `,"conn_id":`...)
	dst = append(dst, in.ConnID...)
	dst = append(dst, `,"seq":`...)
	dst = append(dst, in.Seq...)
	dst = append(dst, `,"client_send_epoch_ns":`...)
	dst = append(dst, in.ClientSendEpochNs...)
	dst = append(dst, `,"server_recv_epoch_ns":`...)
	dst = strconv.AppendInt(dst, t2, 10)
	dst = append(dst, `,"server_send_epoch_ns":`...)
	off := len(dst)
	dst = append(dst, epochPlaceholder...)
	dst = append(dst, `,"server":`...)
	dst = append(dst, server...)
	dst = append(dst, '}')
	return dst, off
}

// AppendPush appends a push frame and returns it plus the offset of the
// server_send_epoch_ns slot. traceID, spanID and traceparent are lowercase
// hex produced by the server; connID is the raw JSON conn_id from hello.
func AppendPush(dst []byte, traceID, spanID, traceparent string, connID []byte, pushSeq int64, server []byte) ([]byte, int) {
	dst = append(dst, `{"v":1,"type":"push","trace_id":`...)
	dst = appendString(dst, traceID)
	dst = append(dst, `,"span_id":`...)
	dst = appendString(dst, spanID)
	dst = append(dst, `,"traceparent":`...)
	dst = appendString(dst, traceparent)
	dst = append(dst, `,"conn_id":`...)
	dst = append(dst, connID...)
	dst = append(dst, `,"push_seq":`...)
	dst = strconv.AppendInt(dst, pushSeq, 10)
	dst = append(dst, `,"server_send_epoch_ns":`...)
	off := len(dst)
	dst = append(dst, epochPlaceholder...)
	dst = append(dst, `,"server":`...)
	dst = append(dst, server...)
	dst = append(dst, '}')
	return dst, off
}

// AppendHelloAck appends a hello_ack frame. connID is the raw JSON conn_id
// from hello.
func AppendHelloAck(dst []byte, connID []byte, server []byte, accepted Options) []byte {
	dst = append(dst, `{"v":1,"type":"hello_ack","conn_id":`...)
	dst = append(dst, connID...)
	dst = append(dst, `,"server":`...)
	dst = append(dst, server...)
	dst = append(dst, `,"accepted_options":{"server_keepalive":`...)
	dst = strconv.AppendBool(dst, accepted.ServerKeepalive)
	dst = append(dst, `,"push_interval_ms":`...)
	dst = strconv.AppendInt(dst, accepted.PushIntervalMs, 10)
	dst = append(dst, "}}"...)
	return dst
}

// AppendError appends an error frame. connID is raw JSON (a string) or nil
// when unknown; seq is 0 when unknown.
func AppendError(dst []byte, connID []byte, seq int64, code ErrorCode, message string) []byte {
	if !IsString(connID) {
		connID = []byte(`""`)
	}
	msg, _ := json.Marshal(message)
	dst = append(dst, `{"v":1,"type":"error","conn_id":`...)
	dst = append(dst, connID...)
	dst = append(dst, `,"seq":`...)
	dst = strconv.AppendInt(dst, seq, 10)
	dst = append(dst, `,"code":"`...)
	dst = append(dst, code...)
	dst = append(dst, `","message":`...)
	dst = append(dst, msg...)
	dst = append(dst, '}')
	return dst
}
