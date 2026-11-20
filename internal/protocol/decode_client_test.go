package protocol

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

const validEcho = `{"v":1,"type":"echo","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","conn_id":"01J9ZQ8K3T4X5Y6Z7A8B9C0D1E",` +
	`"seq":1427,"client_send_epoch_ns":1758537600123456789,"server_recv_epoch_ns":1758537600145678901,` +
	`"server_send_epoch_ns":1758537600145890123,"server":{"name":"p","version":"v"}}`

func TestDecodeValid(t *testing.T) {
	f, err := Decode([]byte(validEcho))
	if err != nil || f.Echo == nil || f.Echo.Seq != 1427 {
		t.Fatalf("%+v %v", f, err)
	}
}

func TestDecodeStrictness(t *testing.T) {
	type want int
	const (
		malformed want = iota
		missing
		fieldType
		unknownType
		version
	)
	cases := []struct {
		name  string
		in    string
		want  want
		field string
	}{
		{"empty", ``, malformed, ""},
		{"whitespace only", "  \n", malformed, ""},
		{"array", `[` + validEcho + `]`, malformed, ""},
		{"string", `"echo"`, malformed, ""},
		{"null", `null`, malformed, ""},
		{"trailing object", validEcho + `{}`, malformed, ""},
		{"trailing garbage", validEcho + ` x`, malformed, ""},
		{"trailing bracket", validEcho + `]`, malformed, ""},
		{"truncated", validEcho[:50], malformed, ""},
		{"float seq", strings.Replace(validEcho, `"seq":1427`, `"seq":1.5`, 1), fieldType, "seq"},
		{"exponent seq", strings.Replace(validEcho, `"seq":1427`, `"seq":1e3`, 1), fieldType, "seq"},
		{"quoted seq", strings.Replace(validEcho, `"seq":1427`, `"seq":"123"`, 1), fieldType, "seq"},
		{"float t2", strings.Replace(validEcho, `1758537600145678901`, `1758537600145678901.0`, 1), fieldType, "server_recv_epoch_ns"},
		{"null seq", strings.Replace(validEcho, `"seq":1427`, `"seq":null`, 1), missing, "seq"},
		{"absent seq", strings.Replace(validEcho, `"seq":1427,`, ``, 1), missing, "seq"},
		{"null server", strings.Replace(validEcho, `"server":{"name":"p","version":"v"}`, `"server":null`, 1), missing, "server"},
		{"absent server.name", strings.Replace(validEcho, `"name":"p",`, ``, 1), missing, "server.name"},
		{"server not object", strings.Replace(validEcho, `"server":{"name":"p","version":"v"}`, `"server":7`, 1), fieldType, "server"},
		{"absent type", strings.Replace(validEcho, `"type":"echo",`, ``, 1), missing, "type"},
		{"null type", strings.Replace(validEcho, `"type":"echo"`, `"type":null`, 1), missing, "type"},
		{"numeric type", strings.Replace(validEcho, `"type":"echo"`, `"type":5`, 1), fieldType, "type"},
		{"unknown type", strings.Replace(validEcho, `"type":"echo"`, `"type":"pong"`, 1), unknownType, ""},
		{"client type", strings.Replace(validEcho, `"type":"echo"`, `"type":"probe"`, 1), unknownType, ""},
		{"v2", strings.Replace(validEcho, `"v":1`, `"v":2`, 1), version, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Decode([]byte(c.in))
			if err == nil {
				t.Fatal("accepted")
			}
			var (
				mf *MissingFieldError
				ft *FieldTypeError
				ut *UnknownTypeError
				ve *VersionError
			)
			switch c.want {
			case malformed:
				if !errors.Is(err, ErrMalformed) {
					t.Errorf("want ErrMalformed, got %T %v", err, err)
				}
			case missing:
				if !errors.As(err, &mf) || mf.Field != c.field {
					t.Errorf("want MissingFieldError{%s}, got %T %v", c.field, err, err)
				}
			case fieldType:
				if !errors.As(err, &ft) || ft.Field != c.field {
					t.Errorf("want FieldTypeError{%s}, got %T %v", c.field, err, err)
				}
			case unknownType:
				if !errors.As(err, &ut) {
					t.Errorf("want UnknownTypeError, got %T %v", err, err)
				}
			case version:
				if !errors.As(err, &ve) || ve.V != 2 {
					t.Errorf("want VersionError, got %T %v", err, err)
				}
			}
		})
	}
}

func TestDecodeMissingFieldEveryType(t *testing.T) {
	for file, typ := range serverGoldens {
		b := readGolden(t, file)
		if _, err := Decode(b); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		for _, rf := range requiredFor(typ) {
			// Replace the value of the key with null; must be reported missing.
			s := string(b)
			i := strings.Index(s, `"`+rf+`"`)
			if i < 0 {
				t.Fatalf("%s: key %s not found", file, rf)
			}
			j := i + len(rf) + 2
			j += strings.Index(s[j:], ":") + 1
			k := j + valueEnd(s[j:])
			mut := s[:j] + "null" + s[k:]
			_, err := Decode([]byte(mut))
			var mf *MissingFieldError
			wantType := typ
			if rf == "type" {
				wantType = ""
			}
			if !errors.As(err, &mf) || mf.Field != rf || mf.Type != wantType {
				t.Errorf("%s: nulling %s gave %v", file, rf, err)
			}
		}
	}
}

func requiredFor(typ string) []string {
	var out []string
	for _, f := range RequiredFields(reflect.TypeOf(map[string]any{TypeEcho: Echo{}, TypePush: Push{}, TypeHelloAck: HelloAck{}, TypeError: Error{}}[typ])) {
		out = append(out, f.Name)
	}
	return out
}

// valueEnd returns the length of the JSON value at the start of s (after
// optional whitespace), for the simple values in the goldens.
func valueEnd(s string) int {
	depth, inStr := 0, false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inStr:
			if c == '\\' {
				i++
			} else if c == '"' {
				inStr = false
				if depth == 0 {
					return i + 1
				}
			}
		case c == '"':
			inStr = true
		case c == '{' || c == '[':
			depth++
		case c == '}' || c == ']':
			if depth == 0 {
				return i
			}
			depth--
			if depth == 0 {
				return i + 1
			}
		case c == ',' && depth == 0:
			return i
		}
	}
	return len(s)
}

func TestDecodeUnknownErrorCodeAccepted(t *testing.T) {
	f, err := Decode([]byte(`{"v":1,"type":"error","conn_id":"","seq":0,"code":"teapot","message":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	if f.Error.Code.Known() {
		t.Error("teapot is not a known code")
	}
}

func TestIDs(t *testing.T) {
	if !IsLowerHex("00f067aa0ba902b7", 16) || IsLowerHex("00F067AA0BA902B7", 16) || IsLowerHex("00f067aa0ba902b", 16) || IsLowerHex("00f067aa0ba902bg", 16) {
		t.Error("IsLowerHex")
	}
	good := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if tid, sid, s, err := ParseTraceparent(good); err != nil || tid != "4bf92f3577b34da6a3ce929d0e0e4736" || sid != "00f067aa0ba902b7" || !s {
		t.Errorf("ParseTraceparent(good) = %s %s %v %v", tid, sid, s, err)
	}
	if _, _, s, err := ParseTraceparent(strings.TrimSuffix(good, "01") + "00"); err != nil || s {
		t.Error("unsampled flag")
	}
	for _, bad := range []string{
		"",
		strings.ToUpper(good),
		"01" + good[2:],
		good + "-",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0g",
		"00_4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	} {
		if _, _, _, err := ParseTraceparent(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	if FormatTraceparent("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", true) != good {
		t.Error("FormatTraceparent")
	}
	if !ValidULID("01J9ZQ8K3T4X5Y6Z7A8B9C0D1E") || ValidULID("01j9zq8k3t4x5y6z7a8b9c0d1e") || ValidULID("81J9ZQ8K3T4X5Y6Z7A8B9C0D1E") ||
		ValidULID("01J9ZQ8K3T4X5Y6Z7A8B9C0D1") || ValidULID("01J9ZQ8K3T4X5Y6Z7A8B9C0D1U") {
		t.Error("ValidULID")
	}
}
