// Package servertel registers the server's Prometheus metrics and
// emits its OpenTelemetry spans. Buckets, label mapping, exemplars, span
// names and attribute keys come from internal/telemetry, shared with the
// client.
package servertel

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
)

// Metrics is exactly the server metric set. No metric carries an entry-point, trace,
// or connection label. Label children are resolved once at construction so
// the hot path never touches a vector's shared map.
type Metrics struct {
	Registry *prometheus.Registry

	ConnectionsActive prometheus.Gauge
	ConnectionsTotal  prometheus.Counter

	MsgProbe   prometheus.Counter
	MsgHello   prometheus.Counter
	MsgPush    prometheus.Counter
	MsgPushAck prometheus.Counter

	HandleProbe prometheus.Observer
	HandleHello prometheus.Observer

	PushAck prometheus.Observer

	errors map[protocol.ErrorCode]prometheus.Counter
	closes map[int]prometheus.Counter
	other  prometheus.Counter
}

// NewMetrics registers every server metric plus the Go runtime and process
// collectors on a fresh registry. labels (WSMON_LABELS) go on every series,
// as on the client; name (WSMON_NAME) only on wsmon_server_build_info.
func NewMetrics(version, name string, labels map[string]string) *Metrics {
	reg := prometheus.NewRegistry()
	wrapped := prometheus.WrapRegistererWith(prometheus.Labels(labels), reg)
	f := func(c prometheus.Collector) prometheus.Collector { wrapped.MustRegister(c); return c }

	// Go runtime metrics are mandatory: GC pause duration is
	// what rules the server out during an investigation.
	f(collectors.NewGoCollector(collectors.WithGoCollectorRuntimeMetrics(
		collectors.MetricsGC, collectors.MetricsScheduler, collectors.MetricsMemory,
	)))
	f(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{Registry: reg}

	m.ConnectionsActive = f(prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wsmon_server_connections_active",
		Help: "WebSocket connections currently open.",
	})).(prometheus.Gauge)
	m.ConnectionsTotal = f(prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wsmon_server_connections_total",
		Help: "WebSocket connections accepted.",
	})).(prometheus.Counter)

	messages := f(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wsmon_server_messages_total",
		Help: "Application frames by type: probe, hello and push_ack received; push sent.",
	}, []string{"type"})).(*prometheus.CounterVec)
	m.MsgProbe = messages.WithLabelValues(protocol.TypeProbe)
	m.MsgHello = messages.WithLabelValues(protocol.TypeHello)
	m.MsgPush = messages.WithLabelValues(protocol.TypePush)
	m.MsgPushAck = messages.WithLabelValues(protocol.TypePushAck)

	handle := f(prometheus.NewHistogramVec(telemetry.HistogramOpts(
		"wsmon_server_handle_seconds", "Server handling time, t3 - t2.", telemetry.HandleBuckets, nil,
	), []string{"type"})).(*prometheus.HistogramVec)
	m.HandleProbe = handle.WithLabelValues(protocol.TypeProbe)
	m.HandleHello = handle.WithLabelValues(protocol.TypeHello)

	m.PushAck = f(prometheus.NewHistogram(telemetry.HistogramOpts(
		"wsmon_server_push_ack_seconds", "Server-clock interval from push sent to push_ack received.", telemetry.RTTBuckets, nil,
	))).(prometheus.Histogram)

	errs := f(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wsmon_server_errors_total",
		Help: "Error frames sent, by code.",
	}, []string{"code"})).(*prometheus.CounterVec)
	m.errors = make(map[protocol.ErrorCode]prometheus.Counter, len(protocol.ErrorCodes))
	for _, c := range protocol.ErrorCodes {
		m.errors[c] = errs.WithLabelValues(string(c))
	}

	closes := f(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wsmon_server_closes_total",
		Help: "Connections closed, by WebSocket close code.",
	}, []string{"close_code"})).(*prometheus.CounterVec)
	m.closes = make(map[int]prometheus.Counter, len(protocol.CloseCodes))
	for _, c := range protocol.CloseCodes {
		m.closes[c] = closes.WithLabelValues(telemetry.CloseCodeLabel(c))
	}
	m.other = closes.WithLabelValues(telemetry.LabelOther)

	f(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wsmon_server_build_info",
		Help: "Always 1; carries build and server identity.",
	}, []string{"version", "name"})).(*prometheus.GaugeVec).WithLabelValues(version, name).Set(1)

	return m
}

// Error counts one error frame sent (every error frame increments it).
func (m *Metrics) Error(code protocol.ErrorCode) {
	if c, ok := m.errors[code]; ok {
		c.Inc()
	}
}

// Close counts one closed connection.
func (m *Metrics) Close(code int) {
	if c, ok := m.closes[code]; ok {
		c.Inc()
		return
	}
	m.other.Inc()
}
