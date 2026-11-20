package protocol

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	gConnID  = "01J9ZQ8K3T4X5Y6Z7A8B9C0D1E"
	gTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	gSpanID  = "00f067aa0ba902b7"
	gT1      = int64(1758537600123456789)
	gPTrace  = "9c1e5d3b7a2f4e6081c9d2b3a4f5e607"
	gPSpan   = "b7ad6b7169203331"
)

func gClient(mode string) ClientInfo {
	return ClientInfo{Name: "probe-nimbus-east1-0", Mode: mode, Version: "1.4.2", Labels: map[string]string{"site": "nimbus-east1", "team": "netops"}}
}

var gServer = ServerInfo{Name: "wsmon-server-7d9f8c6b5-xk2mn", Version: "1.2.0"}

func gProbeFields(sampled bool) ProbeFields {
	return ProbeFields{TraceID: gTraceID, SpanID: gSpanID, Traceparent: FormatTraceparent(gTraceID, gSpanID, sampled), ConnID: gConnID, Seq: 1427}
}

// clientGoldens are the inputs of every byte-exact client → server golden.
// Keep them next to the files they produce (testdata/README.md).
var clientGoldens = []struct {
	file   string
	encode func(t *testing.T) []byte
}{
	{"probe.json", func(t *testing.T) []byte { return encodeProbe(t, gProbeFields(true), gClient("pingpong"), 0, gT1) }},
	{"probe_large.json", func(t *testing.T) []byte { return encodeProbe(t, gProbeFields(true), gClient("pingpong"), 32768, gT1) }},
	{"hello.json", func(*testing.T) []byte {
		return AppendHello(nil, gConnID, EncodeClientInfo(gClient("pingpong")), Options{ServerKeepalive: false, PushIntervalMs: 1000})
	}},
	{"hello_idle.json", func(*testing.T) []byte {
		return AppendHello(nil, gConnID, EncodeClientInfo(gClient("idle")), Options{})
	}},
	{"push_ack.json", func(*testing.T) []byte {
		return AppendPushAck(nil, gPTrace, gConnID, 42, 1758537600167000000)
	}},
}

func encodeProbe(t *testing.T, p ProbeFields, c ClientInfo, size int, t1 int64) []byte {
	t.Helper()
	frame, off, err := AppendProbe(nil, p, EncodeClientInfo(c), size)
	if err != nil {
		t.Fatal(err)
	}
	PatchEpochNs(frame, off, t1)
	return frame
}

func readGolden(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// serverGoldens maps every server → client golden to its frame type.
var serverGoldens = func() map[string]string {
	m := map[string]string{
		"echo.json": TypeEcho, "echo_patched.json": TypeEcho, "echo_unknown_fields.json": TypeEcho,
		"push.json": TypePush, "hello_ack.json": TypeHelloAck, "error.json": TypeError,
	}
	for _, c := range ErrorCodes {
		m["error_"+string(c)+".json"] = TypeError
	}
	return m
}()

// TC-WIRE-19 (byte part): every client-sent frame encodes to its golden.
func TestTCWIRE19_GoldenBytes(t *testing.T) {
	for _, g := range clientGoldens {
		t.Run(g.file, func(t *testing.T) {
			got, want := g.encode(t), readGolden(t, g.file)
			if !bytes.Equal(got, want) {
				t.Errorf("encoding differs from golden\n got: %.300s\nwant: %.300s", got, want)
			}
		})
	}
}

// TC-WIRE-19 (shape part).
func TestTCWIRE19_FrameShape(t *testing.T) {
	for _, g := range clientGoldens {
		t.Run(g.file, func(t *testing.T) {
			frame := g.encode(t)
			assertSingleObject(t, frame)
			var m map[string]any
			if err := json.Unmarshal(frame, &m); err != nil {
				t.Fatal(err)
			}
			if m["v"] != float64(1) {
				t.Errorf("v = %v", m["v"])
			}
			if tid, ok := m["trace_id"].(string); ok && !IsLowerHex(tid, 32) {
				t.Errorf("trace_id %q is not 32 lowercase hex", tid)
			}
			if sid, ok := m["span_id"].(string); ok && !IsLowerHex(sid, 16) {
				t.Errorf("span_id %q is not 16 lowercase hex", sid)
			}
			if cid, _ := m["conn_id"].(string); !ValidULID(cid) {
				t.Errorf("conn_id %q is not a ULID", cid)
			}
			if tp, ok := m["traceparent"].(string); ok {
				tid, sid, _, err := ParseTraceparent(tp)
				if err != nil || tid != m["trace_id"] || sid != m["span_id"] {
					t.Errorf("traceparent %q does not match trace_id/span_id (err %v)", tp, err)
				}
			}
		})
	}
}

func assertSingleObject(t *testing.T, b []byte) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(b))
	var obj map[string]json.RawMessage
	if err := dec.Decode(&obj); err != nil {
		t.Fatalf("not a JSON object: %v", err)
	}
	if rest := bytes.TrimSpace(b[dec.InputOffset():]); len(rest) > 0 {
		t.Fatalf("trailing data after object: %q", rest)
	}
}

func TestTCWIRE19_SampledFlag(t *testing.T) {
	for _, sampled := range []bool{true, false} {
		var p Probe
		if err := json.Unmarshal(encodeProbe(t, gProbeFields(sampled), gClient("pingpong"), 0, gT1), &p); err != nil {
			t.Fatal(err)
		}
		want := "-00"
		if sampled {
			want = "-01"
		}
		if !strings.HasSuffix(p.Traceparent, want) {
			t.Errorf("sampled=%v: traceparent %q", sampled, p.Traceparent)
		}
		if _, _, s, err := ParseTraceparent(p.Traceparent); err != nil || s != sampled {
			t.Errorf("sampled=%v: parsed %v, %v", sampled, s, err)
		}
	}
}

func TestTCWIRE19_Payloads(t *testing.T) {
	small := encodeProbe(t, gProbeFields(true), gClient("pingpong"), 0, gT1)
	if bytes.Contains(small, []byte(`"padding"`)) {
		t.Errorf("small probe has a padding key: %s", small)
	}
	for _, size := range []int{1024, 32768, 524288} {
		frame := encodeProbe(t, gProbeFields(true), gClient("pingpong"), size, gT1)
		if len(frame) != size {
			t.Errorf("size %d: got %d bytes", size, len(frame))
		}
		var p Probe
		if err := json.Unmarshal(frame, &p); err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if p.Padding == "" || strings.Trim(p.Padding, "x") != "" {
			t.Errorf("size %d: padding is not only 'x'", size)
		}
	}
	// Any t1 width keeps the size exact: the slot is counted at full width.
	for _, t1 := range []int64{0, 1, gT1, -1 << 63} {
		if got := len(encodeProbe(t, gProbeFields(true), gClient("pingpong"), 2048, t1)); got != 2048 {
			t.Errorf("t1=%d: size %d", t1, got)
		}
	}
}

func TestAppendProbeTooSmall(t *testing.T) {
	_, _, err := AppendProbe(nil, gProbeFields(true), EncodeClientInfo(gClient("pingpong")), len(readGolden(t, "probe.json"))+12)
	if err == nil {
		t.Fatal("expected an error for a target size below the minimum padded frame")
	}
	if _, _, err := AppendProbe(nil, gProbeFields(true), EncodeClientInfo(gClient("pingpong")), len(readGolden(t, "probe.json"))+13); err != nil {
		t.Fatalf("minimum padded size rejected: %v", err)
	}
}

// Client identity comes from env and may need escaping.
func TestEncodeEscapesClientInfo(t *testing.T) {
	c := gClient("pingpong")
	c.Labels["site"] = "a\"b\\c<d>\n"
	frame := encodeProbe(t, gProbeFields(true), c, 0, gT1)
	var p Probe
	if err := json.Unmarshal(frame, &p); err != nil {
		t.Fatal(err)
	}
	if p.Client.Labels["site"] != c.Labels["site"] {
		t.Errorf("label %q", p.Client.Labels["site"])
	}
}

func TestExtraFieldHook(t *testing.T) {
	p := gProbeFields(true)
	p.Extra = Extra{kv: []byte(`"wsmon_test_unknown":"x"`)}
	for _, size := range []int{0, 4096} {
		frame := encodeProbe(t, p, gClient("pingpong"), size, gT1)
		var m map[string]any
		if err := json.Unmarshal(frame, &m); err != nil {
			t.Fatal(err)
		}
		if m["wsmon_test_unknown"] != "x" {
			t.Errorf("extra field missing: %s", frame[:200])
		}
		if size > 0 && len(frame) != size {
			t.Errorf("extra field broke size: %d", len(frame))
		}
	}
}

// TC-WIRE-20 and golden decode.
func TestGoldenDecode(t *testing.T) {
	want := Echo{V: 1, Type: TypeEcho, TraceID: gTraceID, ConnID: gConnID, Seq: 1427, ClientSendEpochNs: gT1,
		ServerRecvEpochNs: 1758537600145678901, ServerSendEpochNs: 1758537600145890123, Server: gServer}
	for file, typ := range serverGoldens {
		t.Run(file, func(t *testing.T) {
			f, err := Decode(readGolden(t, file))
			if err != nil {
				t.Fatal(err)
			}
			if f.Type != typ {
				t.Fatalf("type %q, want %q", f.Type, typ)
			}
			switch typ {
			case TypeEcho:
				if *f.Echo != want {
					t.Errorf("got %+v\nwant %+v", *f.Echo, want)
				}
			case TypePush:
				p := *f.Push
				if p.TraceID != gPTrace || p.SpanID != gPSpan || p.ConnID != gConnID || p.PushSeq != 42 ||
					p.ServerSendEpochNs != 1758537600145890123 || p.Server != gServer {
					t.Errorf("push %+v", p)
				}
				if tid, sid, sampled, err := ParseTraceparent(p.Traceparent); err != nil || tid != p.TraceID || sid != p.SpanID || !sampled {
					t.Errorf("push traceparent %q", p.Traceparent)
				}
			case TypeHelloAck:
				if a := *f.HelloAck; a.ConnID != gConnID || a.Server != gServer || a.AcceptedOptions != (Options{}) {
					t.Errorf("hello_ack %+v", a)
				}
			case TypeError:
				e := *f.Error
				if !e.Code.Known() || e.Message == "" {
					t.Errorf("error %+v", e)
				}
				if file == "error.json" && (e.ConnID != gConnID || e.Seq != 1427 || e.Code != ErrBadVersion) {
					t.Errorf("error.json %+v", e)
				}
				if c := strings.TrimSuffix(strings.TrimPrefix(file, "error_"), ".json"); file != "error.json" && string(e.Code) != c {
					t.Errorf("code %q in %s", e.Code, file)
				}
			}
		})
	}
}

// Mirror check: every struct's JSON tags match the keys of its spec-derived
// goldens, so a field added on one side only is caught.
func TestMirrorCheck(t *testing.T) {
	cases := []struct {
		files []string
		typ   reflect.Type
	}{
		{[]string{"probe.json", "probe_large.json"}, reflect.TypeOf(Probe{})},
		{[]string{"hello.json", "hello_idle.json"}, reflect.TypeOf(Hello{})},
		{[]string{"push_ack.json"}, reflect.TypeOf(PushAck{})},
		{[]string{"echo.json", "echo_patched.json"}, reflect.TypeOf(Echo{})},
		{[]string{"push.json"}, reflect.TypeOf(Push{})},
		{[]string{"hello_ack.json"}, reflect.TypeOf(HelloAck{})},
		{[]string{"error.json"}, reflect.TypeOf(Error{})},
	}
	for _, c := range cases {
		all := tagSet(JSONFields(c.typ))
		required := tagSet(RequiredFields(c.typ))
		union := map[string]bool{}
		for _, f := range c.files {
			var m map[string]json.RawMessage
			b := readGolden(t, f)
			if c.typ == reflect.TypeOf(Probe{}) {
				b = bytes.Replace(b, []byte(strconv.FormatInt(gT1, 10)+" "), []byte(strconv.FormatInt(gT1, 10)), 1)
			}
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			for k, v := range m {
				union[k] = true
				if !all[k] {
					t.Errorf("%s: key %q has no field in %s", f, k, c.typ.Name())
				}
				// Nested structs: compare their keys too.
				for _, sf := range JSONFields(c.typ) {
					if sf.Name == k && sf.Type.Kind() == reflect.Struct {
						var nm map[string]json.RawMessage
						if err := json.Unmarshal(v, &nm); err != nil {
							t.Fatal(err)
						}
						if got, want := keys(nm), sortedKeys(tagSet(JSONFields(sf.Type))); !reflect.DeepEqual(got, want) {
							t.Errorf("%s.%s keys %v, want %v", f, k, got, want)
						}
					}
				}
			}
			for k := range required {
				if _, ok := m[k]; !ok {
					t.Errorf("%s: required key %q missing", f, k)
				}
			}
		}
		if !reflect.DeepEqual(sortedKeys(union), sortedKeys(all)) {
			t.Errorf("%s: golden keys %v, struct tags %v", c.typ.Name(), sortedKeys(union), sortedKeys(all))
		}
	}
}

func tagSet(fs []JSONField) map[string]bool {
	m := map[string]bool{}
	for _, f := range fs {
		m[f.Name] = true
	}
	return m
}

func keys(m map[string]json.RawMessage) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// inbound has the shape of the server's protocol.Inbound (raw fields).
type inbound struct {
	V                 json.RawMessage `json:"v"`
	Type              json.RawMessage `json:"type"`
	TraceID           json.RawMessage `json:"trace_id"`
	SpanID            json.RawMessage `json:"span_id"`
	Traceparent       json.RawMessage `json:"traceparent"`
	ConnID            json.RawMessage `json:"conn_id"`
	Seq               json.RawMessage `json:"seq"`
	ClientSendEpochNs json.RawMessage `json:"client_send_epoch_ns"`
	Client            json.RawMessage `json:"client"`
}

// Server-parse compatibility: the slot's trailing spaces must not reach the
// server's strconv.ParseInt.
func TestServerParseCompatibility(t *testing.T) {
	for _, t1 := range []int64{gT1, 7, 0} {
		for _, size := range []int{0, 32768} {
			var in inbound
			if err := json.Unmarshal(encodeProbe(t, gProbeFields(true), gClient("pingpong"), size, t1), &in); err != nil {
				t.Fatal(err)
			}
			raw := string(in.ClientSendEpochNs)
			if v, err := strconv.ParseInt(raw, 10, 64); err != nil || v != t1 {
				t.Errorf("t1=%d size=%d: raw client_send_epoch_ns %q does not parse as digits (%v)", t1, size, raw, err)
			}
			if s := string(in.Seq); s != "1427" {
				t.Errorf("raw seq %q", s)
			}
		}
	}
}

// TC-TIME-07: t1 is captured after serialisation, is the value in the frame,
// and patching does not change the frame length.
func TestTCTIME07_T1AfterSerialisation(t *testing.T) {
	frame, off, err := AppendProbe(nil, gProbeFields(true), EncodeClientInfo(gClient("pingpong")), 32768)
	if err != nil {
		t.Fatal(err)
	}
	lenBefore := len(frame)
	calls := 0
	clock := func() int64 {
		calls++
		// (a) At the moment the clock is read, serialisation is complete:
		// the frame ends with its closing brace and the slot is still empty.
		if frame[len(frame)-1] != '}' || len(frame) != 32768 {
			t.Error("clock read before serialisation finished")
		}
		if !bytes.Equal(frame[off:off+EpochSlotWidth], epochPlaceholder) {
			t.Error("slot already written when the clock was read")
		}
		return 1758537600999999999
	}
	t1 := StampT1(frame, off, clock)
	if calls != 1 {
		t.Errorf("clock read %d times", calls)
	}
	// (b) The decoded value equals the returned (recorded) t1.
	var p Probe
	if err := json.Unmarshal(frame, &p); err != nil {
		t.Fatal(err)
	}
	if p.ClientSendEpochNs != t1 || t1 != 1758537600999999999 {
		t.Errorf("frame t1 %d, recorded %d", p.ClientSendEpochNs, t1)
	}
	// (c) Length unchanged.
	if len(frame) != lenBefore {
		t.Errorf("length changed %d → %d", lenBefore, len(frame))
	}
}

func TestErrorCodes(t *testing.T) {
	want := []ErrorCode{"bad_version", "bad_json", "bad_type", "missing_field", "seq_regression", "too_large", "no_hello", "rate_limited", "shutting_down"}
	if !reflect.DeepEqual(ErrorCodes, want) {
		t.Errorf("ErrorCodes %v", ErrorCodes)
	}
	close := map[ErrorCode]int{
		"bad_version": 1002, "bad_json": 1002, "bad_type": 0, "missing_field": 0, "seq_regression": 0,
		"too_large": 1009, "no_hello": 1002, "rate_limited": 0, "shutting_down": 1001,
	}
	for c, w := range close {
		if got := c.CloseCode(); got != w {
			t.Errorf("%s.CloseCode() = %d, want %d", c, got, w)
		}
		if !c.Known() {
			t.Errorf("%s not Known", c)
		}
	}
	if ErrorCode("teapot").Known() || ErrorCode("").Known() {
		t.Error("unknown code reported as known")
	}
}
