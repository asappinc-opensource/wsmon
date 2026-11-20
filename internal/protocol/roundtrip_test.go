package protocol

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// These tests close the loop between the two halves of this package: every
// frame one role encodes is decoded by the other role's decoder. Before the
// merge the client mirrored the server's structs and relied on golden files
// and a contract suite to notice drift; now drift does not compile, and the
// tests below catch semantic drift (field copying, slot patching, required
// fields) in milliseconds.

var testClient = EncodeClientInfo(ClientInfo{Mode: "pingpong", Version: "test", Labels: map[string]string{"site": "x"}})
var testServer = EncodeServerInfo(ServerInfo{Name: "p", Version: "v"})

// clientProbe encodes a probe the way the client does, with t1 stamped.
func clientProbe(t *testing.T, size int, extra Extra) ([]byte, int64) {
	t.Helper()
	tid, sid := strings.Repeat("ab", 16), strings.Repeat("cd", 8)
	frame, off, err := AppendProbe(nil, ProbeFields{
		TraceID: tid, SpanID: sid, Traceparent: FormatTraceparent(tid, sid, true),
		ConnID: "01J9ZQ8K4V6Y3N2M1P0QRSTVWX", Seq: 42, Extra: extra,
	}, testClient, size)
	if err != nil {
		t.Fatal(err)
	}
	t1 := StampT1(frame, off, func() int64 { return 1727000000123456789 })
	return frame, t1
}

// TestProbeEchoRoundTrip: client probe → server Inbound → server echo →
// client Decode, for both payload sizes. The four verbatim fields must
// survive unchanged and padding must never be echoed.
func TestProbeEchoRoundTrip(t *testing.T) {
	for _, size := range []int{0, 32768} {
		probe, t1 := clientProbe(t, size, Extra{})
		var in Inbound
		if err := in.Decode(probe); err != nil {
			t.Fatalf("server cannot decode client probe: %v\n%s", err, probe)
		}
		if typ, _ := in.FrameType(); typ != TypeProbe {
			t.Fatalf("frame type %q", typ)
		}
		if v, ok := Int64(in.V); !ok || v != Version {
			t.Fatalf("v = %s", in.V)
		}
		if !Present(in.Client) {
			t.Fatal("client field not present")
		}

		echo, off := AppendEcho(nil, &in, 1727000000123500000, testServer)
		PatchEpochNs(echo, off, 1727000000123600000)
		if strings.Contains(string(echo), "padding") {
			t.Fatalf("echo carries padding: %s", echo)
		}
		f, err := Decode(echo)
		if err != nil {
			t.Fatalf("client cannot decode server echo: %v\n%s", err, echo)
		}
		e := f.Echo
		if e.TraceID != strings.Repeat("ab", 16) || e.ConnID != "01J9ZQ8K4V6Y3N2M1P0QRSTVWX" || e.Seq != 42 || e.ClientSendEpochNs != t1 {
			t.Errorf("verbatim fields changed: %+v", e)
		}
		if e.ServerRecvEpochNs != 1727000000123500000 || e.ServerSendEpochNs != 1727000000123600000 {
			t.Errorf("server timestamps: %+v", e)
		}
		if e.Server.Name != "p" || e.Server.Version != "v" {
			t.Errorf("server identity: %+v", e.Server)
		}
	}
}

// TestUnknownClientFieldIgnored: an additive client field is
// invisible to the server's decoder and does not reach the echo.
func TestUnknownClientFieldIgnored(t *testing.T) {
	probe, _ := clientProbe(t, 0, Extra{kv: []byte(`"future_field":{"a":[1,2,3]}`)})
	var in Inbound
	if err := in.Decode(probe); err != nil {
		t.Fatal(err)
	}
	echo, off := AppendEcho(nil, &in, 1, testServer)
	PatchEpochNs(echo, off, 2)
	if strings.Contains(string(echo), "future_field") {
		t.Errorf("unknown field echoed: %s", echo)
	}
	if _, err := Decode(echo); err != nil {
		t.Fatal(err)
	}
}

// TestHelloRoundTrip: client hello → server Inbound, then the server's
// hello_ack for the honoured options → client Decode. The client compares
// accepted_options with what it requested, so for every option set the
// client may send, the round trip must return exactly the request.
func TestHelloRoundTrip(t *testing.T) {
	for _, req := range []Options{
		{}, {PushIntervalMs: MinPushInterval.Milliseconds()}, {PushIntervalMs: 1000},
		{PushIntervalMs: MaxPushInterval.Milliseconds()}, {ServerKeepalive: true},
	} {
		hello := AppendHello(nil, "01J9ZQ8K4V6Y3N2M1P0QRSTVWX", testClient, req)
		var in Inbound
		if err := in.Decode(hello); err != nil {
			t.Fatal(err)
		}
		if typ, _ := in.FrameType(); typ != TypeHello || !IsString(in.ConnID) {
			t.Fatalf("server sees %q conn_id %s", typ, in.ConnID)
		}
		var got Options
		if err := json.Unmarshal(in.Options, &got); err != nil || got != req {
			t.Fatalf("server decoded options %+v (%v), want %+v", got, err, req)
		}
		ack := AppendHelloAck(nil, in.ConnID, testServer, HonourOptions(got))
		f, err := Decode(ack)
		if err != nil {
			t.Fatalf("client cannot decode hello_ack: %v\n%s", err, ack)
		}
		if f.HelloAck.ConnID != "01J9ZQ8K4V6Y3N2M1P0QRSTVWX" || f.HelloAck.AcceptedOptions != req {
			t.Errorf("hello_ack %+v for request %+v", f.HelloAck, req)
		}
	}
}

// TestHonourOptionsAdjusts documents the requests the server does not take
// as-is; the client's config validation refuses exactly these.
func TestHonourOptionsAdjusts(t *testing.T) {
	for _, ms := range []int64{1, MinPushInterval.Milliseconds() - 1, MaxPushInterval.Milliseconds() + 1} {
		req := Options{PushIntervalMs: ms}
		if HonourOptions(req) == req {
			t.Errorf("push_interval_ms=%d honoured as-is", ms)
		}
	}
}

// TestPushAckRoundTrip: server push → client Decode → client push_ack →
// server Inbound. trace_id, conn_id and push_seq are copied back.
func TestPushAckRoundTrip(t *testing.T) {
	tid, sid := strings.Repeat("12", 16), strings.Repeat("34", 8)
	push, off := AppendPush(nil, tid, sid, FormatTraceparent(tid, sid, false), []byte(`"CONN"`), 7, testServer)
	PatchEpochNs(push, off, 99)
	f, err := Decode(push)
	if err != nil {
		t.Fatalf("client cannot decode push: %v\n%s", err, push)
	}
	if _, _, _, err := ParseTraceparent(f.Push.Traceparent); err != nil {
		t.Errorf("server traceparent %q rejected by the client: %v", f.Push.Traceparent, err)
	}
	ack := AppendPushAck(nil, f.Push.TraceID, f.Push.ConnID, f.Push.PushSeq, 123)
	var in Inbound
	if err := in.Decode(ack); err != nil {
		t.Fatal(err)
	}
	if typ, _ := in.FrameType(); typ != TypePushAck {
		t.Fatalf("type %q", typ)
	}
	if seq, ok := Int64(in.PushSeq); !ok || seq != 7 {
		t.Errorf("push_seq %s", in.PushSeq)
	}
	if s, _ := String(in.TraceID); s != tid {
		t.Errorf("trace_id %s", in.TraceID)
	}
}

// TestErrorFramesDecode: every error code the server can send decodes on
// the client, with and without a known conn_id.
func TestErrorFramesDecode(t *testing.T) {
	for _, code := range ErrorCodes {
		for _, connID := range [][]byte{nil, []byte(`"CONN"`)} {
			b := AppendError(nil, connID, 3, code, `message with "quotes" and \ backslash`)
			f, err := Decode(b)
			if err != nil {
				t.Fatalf("%s: %v\n%s", code, err, b)
			}
			if f.Error.Code != code || !f.Error.Code.Known() || f.Error.Seq != 3 {
				t.Errorf("%s: decoded %+v", code, f.Error)
			}
		}
	}
}

// TestEveryFrameTypeHasBothSides: each frame type has an encoder on the side
// that sends it and a decoder on the side that receives it.
func TestEveryFrameTypeHasBothSides(t *testing.T) {
	inboundKeys := map[string]bool{}
	for _, f := range JSONFields(reflect.TypeOf(Inbound{})) {
		inboundKeys[f.Name] = true
	}
	// The server decodes client frames through Inbound: every required field
	// of hello, probe and push_ack must have a slot there.
	for _, typ := range []reflect.Type{reflect.TypeOf(Hello{}), reflect.TypeOf(Probe{}), reflect.TypeOf(PushAck{})} {
		for _, f := range RequiredFields(typ) {
			if !inboundKeys[f.Name] {
				t.Errorf("%s.%s has no Inbound slot", typ.Name(), f.Name)
			}
		}
	}
	// The client decodes server frames through Decode.
	for _, typ := range []string{TypeHelloAck, TypeEcho, TypePush, TypeError} {
		if _, err := Decode([]byte(`{"v":1,"type":"` + typ + `"}`)); err == nil {
			t.Errorf("%s with no fields decoded without error", typ)
		} else if _, unknown := err.(*UnknownTypeError); unknown {
			t.Errorf("client decoder does not know %s", typ)
		}
	}
}
