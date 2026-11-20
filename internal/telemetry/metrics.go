// Package telemetry holds the observability building blocks shared by both
// roles: histogram buckets and options, label mapping, exemplars, span names
// and attribute keys, the OTLP exporter, the admin HTTP listener and logging.
// The role-specific metric sets live in internal/server/servertel and
// internal/client/probetel and are built from these pieces, so the same
// quantity measured on both sides is bucketed and labelled identically.
package telemetry

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// Histogram buckets.
var (
	// RTTBuckets serve every round-trip-shaped histogram on both sides:
	// wsmon_client_rtt_seconds, wsmon_client_rtt_net_seconds,
	// wsmon_client_burst_completion_seconds, wsmon_client_push_gap_seconds and
	// wsmon_server_push_ack_seconds. 0.250 must stay an exact boundary while
	// 250 ms is the latency threshold.
	RTTBuckets = []float64{
		0.005, 0.010, 0.025, 0.050, 0.075, 0.100, 0.150, 0.200,
		0.250, 0.500, 0.750, 1.0, 2.5, 5.0, 10.0,
	}

	// HandleBuckets resolve the server's sub-millisecond handling time
	// t3 − t2. The server observes it as wsmon_server_handle_seconds and the
	// client as wsmon_client_server_processing_seconds: one quantity, one
	// set of buckets.
	HandleBuckets = []float64{
		0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
	}
)

// Native (exponential) histograms are enabled alongside the classic buckets
// bounded so a histogram's memory stays constant.
const (
	NativeBucketFactor = 1.1
	NativeMaxBuckets   = 160
	NativeMinReset     = time.Hour
)

// HistogramOpts returns options with the classic buckets and the shared
// native-histogram settings.
func HistogramOpts(name, help string, buckets []float64, constLabels prometheus.Labels) prometheus.HistogramOpts {
	return prometheus.HistogramOpts{
		Name: name, Help: help, ConstLabels: constLabels, Buckets: buckets,
		NativeHistogramBucketFactor:     NativeBucketFactor,
		NativeHistogramMaxBucketNumber:  NativeMaxBuckets,
		NativeHistogramMinResetDuration: NativeMinReset,
	}
}

// LabelOther is the label value for anything outside a bounded enum.
const LabelOther = "other"

// CloseCodeLabel renders a close code as a metric label: the code for known
// close codes, "other" for anything else, so a peer cannot mint label values.
func CloseCodeLabel(code int) string {
	if protocol.KnownCloseCode(code) {
		return strconv.Itoa(code)
	}
	return LabelOther
}

// ErrorCodeLabel renders an error code as a metric label: the code for known
// error codes, "other" for anything else.
func ErrorCodeLabel(code protocol.ErrorCode) string {
	if code.Known() {
		return string(code)
	}
	return LabelOther
}

// ObserveWithTrace records v on o, attaching traceID as an exemplar when
// non-empty.
func ObserveWithTrace(o prometheus.Observer, v float64, traceID string) {
	if traceID != "" {
		if eo, ok := o.(prometheus.ExemplarObserver); ok {
			eo.ObserveWithExemplar(v, prometheus.Labels{"trace_id": traceID})
			return
		}
	}
	o.Observe(v)
}
