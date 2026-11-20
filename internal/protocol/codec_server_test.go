package protocol

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

const probeJSON = `{"v":1,"type":"probe","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7",` +
	`"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","conn_id":"01J9ZQ8K3T4X5Y6Z7A8B9C0D1E",` +
	`"seq":1427,"client_send_epoch_ns":1758537600123456789,"client":{"target":"relay-prod-east","entrypoint":"skyedge",` +
	`"vantage":"nimbus-east1","mode":"pingpong","env":"prod","version":"1.4.2"},"padding":"xxxx","future_field":{"a":[1,2]}}`

// TC-WIRE-01, TC-WIRE-02, TC-WIRE-05 at the codec level.
func TestAppendEchoCopiesVerbatimAndDropsPadding(t *testing.T) {
	var in Inbound
	if err := in.Decode([]byte(probeJSON)); err != nil {
		t.Fatal(err)
	}
	server := EncodeServerInfo(ServerInfo{Name: "p", Version: "v"})
	frame, off := AppendEcho(nil, &in, 1758537600145678901, server)
	PatchEpochNs(frame, off, 1758537600145890123)

	for _, want := range []string{
		`"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`,
		`"conn_id":"01J9ZQ8K3T4X5Y6Z7A8B9C0D1E"`,
		`"seq":1427`,
		`"client_send_epoch_ns":1758537600123456789`,
	} {
		if !bytes.Contains(frame, []byte(want)) {
			t.Errorf("echo missing verbatim %s:\n%s", want, frame)
		}
	}
	if bytes.Contains(frame, []byte("padding")) {
		t.Errorf("echo contains padding: %s", frame)
	}

	var e Echo
	if err := json.Unmarshal(frame, &e); err != nil {
		t.Fatalf("echo is not valid JSON: %v\n%s", err, frame)
	}
	if e.Type != TypeEcho || e.V != 1 || e.Seq != 1427 || e.ClientSendEpochNs != 1758537600123456789 {
		t.Errorf("bad echo: %+v", e)
	}
	if e.ServerRecvEpochNs != 1758537600145678901 || e.ServerSendEpochNs != 1758537600145890123 {
		t.Errorf("bad timestamps: %+v", e)
	}
	if e.Server.Name != "p" || e.Server.Version != "v" {
		t.Errorf("bad server identity: %+v", e.Server)
	}
}

// Verbatim means verbatim, including escapes that a re-encode would change.
func TestAppendEchoKeepsEscapes(t *testing.T) {
	src := strings.Replace(probeJSON, `"conn_id":"01J9ZQ8K3T4X5Y6Z7A8B9C0D1E"`, `"conn_id":"a\u003cb<&"`, 1)
	var in Inbound
	if err := in.Decode([]byte(src)); err != nil {
		t.Fatal(err)
	}
	frame, off := AppendEcho(nil, &in, 1, []byte(`{}`))
	PatchEpochNs(frame, off, 2)
	if !bytes.Contains(frame, []byte(`"conn_id":"a\u003cb<&"`)) {
		t.Errorf("conn_id not verbatim: %s", frame)
	}
}

func TestPatchEpochNsAnyWidth(t *testing.T) {
	for _, ns := range []int64{0, 7, 1758537600145890123, -9223372036854775808, 9223372036854775807} {
		frame, off := AppendPush(nil, "t", "s", "tp", []byte(`"c"`), 1, []byte(`{}`))
		PatchEpochNs(frame, off, ns)
		var p Push
		if err := json.Unmarshal(frame, &p); err != nil {
			t.Fatalf("ns=%d: %v\n%s", ns, err, frame)
		}
		if p.ServerSendEpochNs != ns {
			t.Errorf("got %d want %d", p.ServerSendEpochNs, ns)
		}
	}
}

func TestInboundReuseClearsAbsentFields(t *testing.T) {
	var in Inbound
	if err := in.Decode([]byte(probeJSON)); err != nil {
		t.Fatal(err)
	}
	if err := in.Decode([]byte(`{"v":1,"type":"probe"}`)); err != nil {
		t.Fatal(err)
	}
	if Present(in.Seq) || Present(in.TraceID) || Present(in.Client) {
		t.Errorf("stale fields survived Reset: %+v", in)
	}
}

func TestDecodeRejectsNonObjects(t *testing.T) {
	for _, s := range []string{``, `null`, `[]`, `"x"`, `1`, `{"v":1}{"v":1}`, `{"v":1`, `{"v":1,}`} {
		var in Inbound
		if err := in.Decode([]byte(s)); err == nil {
			t.Errorf("%q: expected error", s)
		}
	}
}

func TestInt64RejectsFloats(t *testing.T) {
	for _, s := range []string{`1.0`, `1e3`, `"1"`, `null`, ``, `9223372036854775808`} {
		if _, ok := Int64(json.RawMessage(s)); ok {
			t.Errorf("%q accepted", s)
		}
	}
	if v, ok := Int64(json.RawMessage(`-42`)); !ok || v != -42 {
		t.Errorf("got %d %v", v, ok)
	}
}

func TestFrameType(t *testing.T) {
	cases := map[string]struct {
		name    string
		present bool
	}{
		`{"type":"probe"}`:  {TypeProbe, true},
		`{"type":"hello"}`:  {TypeHello, true},
		`{"type":"banana"}`: {"", true},
		`{"type":null}`:     {"", false},
		`{}`:                {"", false},
	}
	for src, want := range cases {
		var in Inbound
		if err := in.Decode([]byte(src)); err != nil {
			t.Fatal(err)
		}
		name, present := in.FrameType()
		if name != want.name || present != want.present {
			t.Errorf("%s: got (%q,%v) want (%q,%v)", src, name, present, want.name, want.present)
		}
	}
}

func TestAppendHelloAckAndError(t *testing.T) {
	ack := AppendHelloAck(nil, []byte(`"c1"`), EncodeServerInfo(ServerInfo{Name: "p"}), Options{ServerKeepalive: false, PushIntervalMs: 500})
	var h HelloAck
	if err := json.Unmarshal(ack, &h); err != nil {
		t.Fatal(err)
	}
	if h.Type != TypeHelloAck || h.ConnID != "c1" || h.AcceptedOptions.PushIntervalMs != 500 || h.AcceptedOptions.ServerKeepalive {
		t.Errorf("bad hello_ack: %+v", h)
	}

	ef := AppendError(nil, nil, 0, ErrBadVersion, `unsupported "2"`)
	var e Error
	if err := json.Unmarshal(ef, &e); err != nil {
		t.Fatal(err)
	}
	if e.Type != TypeError || e.Code != ErrBadVersion || e.ConnID != "" || e.Message != `unsupported "2"` {
		t.Errorf("bad error: %+v", e)
	}
}

func TestErrorCodeCloseCodes(t *testing.T) {
	want := map[ErrorCode]int{
		ErrBadVersion: 1002, ErrBadJSON: 1002, ErrBadType: 0, ErrMissingField: 0, ErrSeqRegression: 0,
		ErrTooLarge: 1009, ErrNoHello: 1002, ErrRateLimited: 0, ErrShuttingDown: 1001,
	}
	if len(want) != len(ErrorCodes) {
		t.Fatalf("enum has %d codes, table has %d", len(ErrorCodes), len(want))
	}
	for _, c := range ErrorCodes {
		if got := c.CloseCode(); got != want[c] {
			t.Errorf("%s: close %d want %d", c, got, want[c])
		}
	}
}

func BenchmarkDecodeAndEcho(b *testing.B) {
	var in Inbound
	server := EncodeServerInfo(ServerInfo{Name: "wsmon-server-7d9f8c6b5-xk2mn", Version: "2.0.0"})
	src := []byte(probeJSON)
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		if err := in.Decode(src); err != nil {
			b.Fatal(err)
		}
		var off int
		buf, off = AppendEcho(buf[:0], &in, 1, server)
		PatchEpochNs(buf, off, 2)
	}
}
