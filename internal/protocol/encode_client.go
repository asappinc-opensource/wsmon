package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// EncodeClientInfo pre-encodes client identity once per process, for use by
// AppendHello and AppendProbe. The values come from the environment and may
// need escaping, so it uses encoding/json. Mirrors the server's
// EncodeServerInfo.
func EncodeClientInfo(c ClientInfo) []byte {
	b, _ := json.Marshal(c)
	return b
}

// ProbeFields are the per-message values of a probe frame.
type ProbeFields struct {
	TraceID     string
	SpanID      string
	Traceparent string
	ConnID      string
	Seq         int64

	// Extra is a test-only additive field (TC-CONTRACT-11). Its zero value
	// adds nothing, and production code has no way to build a non-zero one:
	// see extra_wsmontest.go.
	Extra Extra
}

// Extra is one additional "key":value pair spliced into a probe just before
// its closing brace. Only tests can construct a non-zero Extra.
type Extra struct {
	kv []byte // `"key":value`, already valid JSON
}

// paddingOverhead is the size of `,"padding":""` without any filler.
const paddingOverhead = len(`,"padding":""`)

// AppendHello appends a hello frame. client is the output of
// EncodeClientInfo.
func AppendHello(dst []byte, connID string, client []byte, opts Options) []byte {
	dst = append(dst, `{"v":1,"type":"hello","conn_id":`...)
	dst = appendString(dst, connID)
	dst = append(dst, `,"client":`...)
	dst = append(dst, client...)
	dst = append(dst, `,"options":{"server_keepalive":`...)
	dst = strconv.AppendBool(dst, opts.ServerKeepalive)
	dst = append(dst, `,"push_interval_ms":`...)
	dst = strconv.AppendInt(dst, opts.PushIntervalMs, 10)
	dst = append(dst, "}}"...)
	return dst
}

// AppendProbe appends a probe frame in the key order of the documented
// example and returns it plus the offset of the client_send_epoch_ns slot,
// to be filled by StampT1 / PatchProbeT1 just before the write.
//
// targetSize 0 means the small payload: no padding key at all. targetSize > 0
// appends padding so that the whole frame is exactly targetSize bytes, with
// the slot counted at its full EpochSlotWidth. An error is returned if
// targetSize cannot hold the unpadded frame plus an empty padding field.
func AppendProbe(dst []byte, p ProbeFields, client []byte, targetSize int) (frame []byte, t1Off int, err error) {
	start := len(dst)
	dst = append(dst, `{"v":1,"type":"probe","trace_id":`...)
	dst = appendString(dst, p.TraceID)
	dst = append(dst, `,"span_id":`...)
	dst = appendString(dst, p.SpanID)
	dst = append(dst, `,"traceparent":`...)
	dst = appendString(dst, p.Traceparent)
	dst = append(dst, `,"conn_id":`...)
	dst = appendString(dst, p.ConnID)
	dst = append(dst, `,"seq":`...)
	dst = strconv.AppendInt(dst, p.Seq, 10)
	dst = append(dst, `,"client_send_epoch_ns":`...)
	t1Off = len(dst)
	dst = append(dst, epochPlaceholder...)
	dst = append(dst, `,"client":`...)
	dst = append(dst, client...)
	if len(p.Extra.kv) > 0 {
		dst = append(dst, ',')
		dst = append(dst, p.Extra.kv...)
	}
	if targetSize > 0 {
		unpadded := len(dst) - start + 1 // + closing brace
		fill := targetSize - unpadded - paddingOverhead
		if fill < 0 {
			return dst[:start], 0, fmt.Errorf("protocol: target size %d is smaller than the minimum padded probe (%d bytes)", targetSize, unpadded+paddingOverhead)
		}
		dst = append(dst, `,"padding":"`...)
		dst = append(dst, bytes.Repeat([]byte{'x'}, fill)...)
		dst = append(dst, '"')
	}
	dst = append(dst, '}')
	return dst, t1Off, nil
}

// AppendPushAck appends a push_ack frame. traceID and connID are the
// values decoded from the push; clientRecvNs is carried for span attributes
// only and is never used in a metric.
func AppendPushAck(dst []byte, traceID, connID string, pushSeq, clientRecvNs int64) []byte {
	dst = append(dst, `{"v":1,"type":"push_ack","trace_id":`...)
	dst = appendString(dst, traceID)
	dst = append(dst, `,"conn_id":`...)
	dst = appendString(dst, connID)
	dst = append(dst, `,"push_seq":`...)
	dst = strconv.AppendInt(dst, pushSeq, 10)
	dst = append(dst, `,"client_recv_epoch_ns":`...)
	dst = strconv.AppendInt(dst, clientRecvNs, 10)
	dst = append(dst, '}')
	return dst
}

// appendString appends s as a JSON string. IDs are plain ASCII and take the
// fast path; anything else (e.g. a conn_id echoed back from a misbehaving
// server) is escaped by encoding/json.
func appendString(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c > 0x7e || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			b, _ := json.Marshal(s)
			return append(dst, b...)
		}
	}
	dst = append(dst, '"')
	dst = append(dst, s...)
	return append(dst, '"')
}
