package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Frame is a decoded server → client frame. Exactly one of the pointers is
// non-nil, matching Type.
type Frame struct {
	Type     string
	HelloAck *HelloAck
	Echo     *Echo
	Push     *Push
	Error    *Error
}

// ErrMalformed is wrapped by every error for input that is not exactly one
// JSON object: empty input, a non-object value, invalid JSON, or trailing
// data after the object.
var ErrMalformed = errors.New("protocol: malformed frame")

// MissingFieldError reports a required field that is absent or null.
// Nested fields are dotted, e.g. "server.name".
type MissingFieldError struct {
	Type  string // frame type, "" if the type itself is missing
	Field string
}

func (e *MissingFieldError) Error() string {
	if e.Type == "" {
		return "protocol: missing field " + e.Field
	}
	return fmt.Sprintf("protocol: %s frame: missing field %s", e.Type, e.Field)
}

// FieldTypeError reports a field present with the wrong JSON type, including
// numbers that are not exact integers (1.5, 1e3) and quoted numbers ("123").
type FieldTypeError struct {
	Type  string
	Field string
	Err   error
}

func (e *FieldTypeError) Error() string {
	return fmt.Sprintf("protocol: %s frame: bad field %s: %v", e.Type, e.Field, e.Err)
}

func (e *FieldTypeError) Unwrap() error { return e.Err }

// UnknownTypeError reports a "type" that is not a server → client frame type.
type UnknownTypeError struct{ Type string }

func (e *UnknownTypeError) Error() string {
	return fmt.Sprintf("protocol: unknown frame type %q", e.Type)
}

// VersionError reports a frame whose "v" is not Version.
type VersionError struct{ V int }

func (e *VersionError) Error() string { return fmt.Sprintf("protocol: unsupported version %d", e.V) }

// Decode parses one server → client frame. It uses a real JSON decoder:
// whitespace anywhere (the server's patched t3 slot) and unknown fields are
// accepted; frames are never byte-compared.
func Decode(b []byte) (Frame, error) {
	if t := bytes.TrimLeft(b, " \t\r\n"); len(t) == 0 || t[0] != '{' {
		return Frame{}, fmt.Errorf("%w: not a JSON object", ErrMalformed)
	}
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&raw); err != nil {
		return Frame{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if rest := bytes.TrimLeft(b[dec.InputOffset():], " \t\r\n"); len(rest) > 0 {
		return Frame{}, fmt.Errorf("%w: trailing data after object", ErrMalformed)
	}

	typRaw := raw["type"]
	if !present(typRaw) {
		return Frame{}, &MissingFieldError{Field: "type"}
	}
	var typ string
	if err := json.Unmarshal(typRaw, &typ); err != nil {
		return Frame{}, &FieldTypeError{Field: "type", Err: err}
	}

	f := Frame{Type: typ}
	var target any
	switch typ {
	case TypeHelloAck:
		f.HelloAck = new(HelloAck)
		target = f.HelloAck
	case TypeEcho:
		f.Echo = new(Echo)
		target = f.Echo
	case TypePush:
		f.Push = new(Push)
		target = f.Push
	case TypeError:
		f.Error = new(Error)
		target = f.Error
	default:
		return Frame{}, &UnknownTypeError{Type: typ}
	}

	if err := checkRequired(typ, raw, reflect.TypeOf(target).Elem(), ""); err != nil {
		return Frame{}, err
	}
	if err := json.Unmarshal(b, target); err != nil {
		var ute *json.UnmarshalTypeError
		if errors.As(err, &ute) {
			return Frame{}, &FieldTypeError{Type: typ, Field: ute.Field, Err: err}
		}
		return Frame{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if v := reflect.ValueOf(target).Elem().FieldByName("V").Int(); v != Version {
		return Frame{}, &VersionError{V: int(v)}
	}
	return f, nil
}

func present(raw json.RawMessage) bool {
	return len(raw) > 0 && !bytes.Equal(raw, []byte("null"))
}

// checkRequired verifies that every non-omitempty field of t (recursing into
// nested structs) is present and non-null in raw.
func checkRequired(typ string, raw map[string]json.RawMessage, t reflect.Type, prefix string) error {
	for _, f := range RequiredFields(t) {
		v := raw[f.Name]
		if !present(v) {
			return &MissingFieldError{Type: typ, Field: prefix + f.Name}
		}
		if f.Type.Kind() != reflect.Struct {
			continue
		}
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(v, &nested); err != nil {
			return &FieldTypeError{Type: typ, Field: prefix + f.Name, Err: err}
		}
		if err := checkRequired(typ, nested, f.Type, prefix+f.Name+"."); err != nil {
			return err
		}
	}
	return nil
}

// JSONField is a struct field's JSON key and Go type.
type JSONField struct {
	Name      string
	Type      reflect.Type
	OmitEmpty bool
}

// JSONFields lists the JSON keys of struct type t in declaration order.
func JSONFields(t reflect.Type) []JSONField {
	var out []JSONField
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		tag := sf.Tag.Get("json")
		if tag == "-" || !sf.IsExported() {
			continue
		}
		name, opts, _ := strings.Cut(tag, ",")
		if name == "" {
			name = sf.Name
		}
		out = append(out, JSONField{Name: name, Type: sf.Type, OmitEmpty: strings.Contains(","+opts+",", ",omitempty,")})
	}
	return out
}

// RequiredFields lists the non-omitempty JSON keys of struct type t.
func RequiredFields(t reflect.Type) []JSONField {
	var out []JSONField
	for _, f := range JSONFields(t) {
		if !f.OmitEmpty {
			out = append(out, f)
		}
	}
	return out
}
