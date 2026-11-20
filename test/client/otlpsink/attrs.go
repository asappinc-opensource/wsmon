package otlpsink

import (
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// attrs flattens OTLP attributes into Go values (string, bool, int64,
// float64, []any).
func attrs(kvs []*commonpb.KeyValue) map[string]any {
	out := make(map[string]any, len(kvs))
	for _, kv := range kvs {
		out[kv.GetKey()] = value(kv.GetValue())
	}
	return out
}

func value(v *commonpb.AnyValue) any {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		return x.BoolValue
	case *commonpb.AnyValue_IntValue:
		return x.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return x.DoubleValue
	case *commonpb.AnyValue_ArrayValue:
		var out []any
		for _, e := range x.ArrayValue.GetValues() {
			out = append(out, value(e))
		}
		return out
	}
	return nil
}
