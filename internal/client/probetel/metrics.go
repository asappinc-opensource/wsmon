package probetel

import (
	"net"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/asappinc-opensource/wsmon/internal/client/probe"
	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
)

// Client-only histogram buckets. RTT and server-processing buckets
// are shared with the server and live in internal/telemetry.
var (
	ConnectBuckets  = []float64{0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0}
	LifetimeBuckets = []float64{10, 30, 60, 120, 300, 600, 1800, 3600, 7200, 21600, 86400}
	LoopLagBuckets  = []float64{0.001, 0.005, 0.010, 0.050, 0.100, 0.500, 1.0}
)

// Recorder is the Prometheus probe.Recorder: exactly the documented client metrics.
// Every metric carries the WSMON_LABELS pairs (added by the wrapping
// registerer); mode is a constant label only where the metric is mode-specific.
type Recorder struct {
	connectAttempts prometheus.Counter
	connectFailures *prometheus.CounterVec
	connectDuration *prometheus.HistogramVec
	connectionUp    prometheus.Gauge
	connsActive     prometheus.Gauge
	upSince         atomic.Int64 // unix ns; 0 while down
	lifetime        *prometheus.HistogramVec
	closes          *prometheus.CounterVec
	reconnects      prometheus.Counter

	sent, echoed, lost, late *prometheus.CounterVec
	mismatched               *prometheus.CounterVec
	duplicate, outOfOrder    prometheus.Counter
	unexpected               prometheus.Counter
	rtt                      *prometheus.HistogramVec
	rttNet                   *prometheus.HistogramVec
	serverProcessing         prometheus.Histogram
	burstCompletion          *prometheus.HistogramVec // nil unless burst
	clockOffset              prometheus.Gauge
	timeAnomaly              *prometheus.CounterVec
	serverErrors             *prometheus.CounterVec
	pushReceived             prometheus.Counter // push metrics nil unless push is requested
	pushGap                  prometheus.Histogram
	pushLost                 prometheus.Counter
	pushOutOfOrder           prometheus.Counter
	idleProbe                *prometheus.CounterVec // idle metrics nil unless idle
	idleClose                *prometheus.CounterVec

	loopLag prometheus.Histogram

	// remote-write self-metrics; nil unless remote-write is enabled.
	rwFailures    prometheus.Counter
	rwLastSuccess prometheus.Gauge
}

var _ probe.Recorder = (*Recorder)(nil)

// applicablePhases are the handshake phases this configuration can produce:
// no tls for ws://, no dns when the address is pinned or an IP literal.
func applicablePhases(cfg config.Client) []probe.Phase {
	var out []probe.Phase
	for _, ph := range probe.Phases {
		switch ph {
		case probe.PhaseDNS:
			if cfg.ResolveIP != nil || (cfg.URL != nil && net.ParseIP(cfg.URL.Hostname()) != nil) {
				continue
			}
		case probe.PhaseTLS:
			if cfg.URL == nil || cfg.URL.Scheme != "wss" {
				continue
			}
		}
		out = append(out, ph)
	}
	return out
}

// payloadsFor are the payload label values this mode can produce: idle
// always sends small; otherwise the configured rotation.
func payloadsFor(cfg config.Client) []string {
	if cfg.Mode == config.ModeIdle {
		return []string{config.PayloadSmall}
	}
	var out []string
	seen := map[string]bool{}
	for _, p := range cfg.Payloads {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

// newRecorder registers every metric applicable to cfg on reg (which already
// adds the universal labels) and pre-initialises every bounded series.
func newRecorder(reg prometheus.Registerer, cfg config.Client, version, configHash string, start time.Time) *Recorder {
	must := func(c prometheus.Collector) { reg.MustRegister(c) }
	mode := prometheus.Labels{"mode": cfg.Mode}
	counter := func(name, help string, constLabels prometheus.Labels) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: constLabels})
		must(c)
		return c
	}
	counterVec := func(name, help string, constLabels prometheus.Labels, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: constLabels}, labels)
		must(c)
		return c
	}
	histOpts := telemetry.HistogramOpts
	hist := func(name, help string, buckets []float64, constLabels prometheus.Labels) prometheus.Histogram {
		h := prometheus.NewHistogram(histOpts(name, help, buckets, constLabels))
		must(h)
		return h
	}
	histVec := func(name, help string, buckets []float64, constLabels prometheus.Labels, labels ...string) *prometheus.HistogramVec {
		h := prometheus.NewHistogramVec(histOpts(name, help, buckets, constLabels), labels)
		must(h)
		return h
	}
	gauge := func(name, help string, constLabels prometheus.Labels) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: constLabels})
		must(g)
		return g
	}

	r := &Recorder{}
	payloads := payloadsFor(cfg)

	// Connection lifecycle.
	r.connectAttempts = counter("wsmon_client_connect_attempts_total", "WebSocket connection attempts.", mode)
	r.connectFailures = counterVec("wsmon_client_connect_failures_total", "Failed connection attempts by reason.", mode, "reason")
	for _, reason := range probe.Reasons {
		r.connectFailures.WithLabelValues(string(reason))
	}
	r.connectDuration = histVec("wsmon_client_connect_duration_seconds", "Connect duration by handshake phase.", ConnectBuckets, mode, "phase")
	for _, ph := range applicablePhases(cfg) {
		r.connectDuration.WithLabelValues(string(ph))
	}
	r.connectionUp = gauge("wsmon_client_connection_up", "1 while the connection is established past hello_ack.", mode)
	r.connsActive = gauge("wsmon_client_connections_active", "Open connections (0 or 1); mirrors wsmon_client_connection_up.", mode)
	must(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "wsmon_client_connection_age_seconds", Help: "Age of the live connection; 0 when down.", ConstLabels: mode,
	}, func() float64 {
		if ns := r.upSince.Load(); ns != 0 {
			return time.Since(time.Unix(0, ns)).Seconds()
		}
		return 0
	}))
	r.lifetime = histVec("wsmon_client_connection_lifetime_seconds", "Connection lifetime, observed on close.", LifetimeBuckets, mode, "close_code")
	r.closes = counterVec("wsmon_client_closes_total", "Connection closures by close code and initiator.", mode, "close_code", "initiator")
	r.reconnects = counter("wsmon_client_reconnects_total", "Reconnect attempts.", mode)

	// Message plane.
	r.sent = counterVec("wsmon_client_messages_sent_total", "Probe frames sent.", mode, "payload")
	r.echoed = counterVec("wsmon_client_messages_echoed_total", "Probe frames echoed.", mode, "payload")
	r.lost = counterVec("wsmon_client_messages_lost_total", "Probe frames with no echo by the deadline (including in flight at disconnect).", mode, "payload")
	r.late = counterVec("wsmon_client_messages_late_total", "Echoes received after the deadline.", mode, "payload")
	for _, p := range payloads {
		for _, v := range []*prometheus.CounterVec{r.sent, r.echoed, r.lost, r.late} {
			v.WithLabelValues(p)
		}
	}
	r.mismatched = counterVec("wsmon_client_messages_mismatched_total", "Echoes whose verbatim fields differ from the probe.", mode, "field")
	for _, f := range probe.Fields {
		r.mismatched.WithLabelValues(string(f))
	}
	r.duplicate = counter("wsmon_client_messages_duplicate_total", "Echoes for an already completed probe.", mode)
	r.outOfOrder = counter("wsmon_client_messages_out_of_order_total", "Echoes below the highest seq already echoed.", mode)
	r.unexpected = counter("wsmon_client_messages_unexpected_total", "Echoes for a seq never sent on this connection.", mode)
	r.rtt = histVec("wsmon_client_rtt_seconds", "Round-trip time t4 − t1.", telemetry.RTTBuckets, mode, "payload", "server_name")
	r.rttNet = histVec("wsmon_client_rtt_net_seconds", "Round-trip time minus server time.", telemetry.RTTBuckets, mode, "payload")
	for _, p := range payloads {
		r.rttNet.WithLabelValues(p)
	}
	r.serverProcessing = hist("wsmon_client_server_processing_seconds", "Server time t3 − t2, clamped at 0.", telemetry.HandleBuckets, mode)
	if cfg.Mode == config.ModeBurst {
		r.burstCompletion = histVec("wsmon_client_burst_completion_seconds", "First send to last echo of a complete burst.", telemetry.RTTBuckets, nil, "payload")
		for _, p := range payloads {
			r.burstCompletion.WithLabelValues(p)
		}
	}
	r.clockOffset = gauge("wsmon_client_clock_offset_seconds", "NTP-style clock offset to the server. Diagnostic only.", nil)
	r.timeAnomaly = counterVec("wsmon_client_time_anomaly_total", "Clock anomalies.", nil, "kind")
	for _, k := range []probe.AnomalyKind{probe.AnomalyServerTimeNegative, probe.AnomalyRTTNegative} {
		r.timeAnomaly.WithLabelValues(string(k))
	}
	r.serverErrors = counterVec("wsmon_client_server_errors_received_total", "Error frames received, by code.", nil, "code")
	for _, c := range protocol.ErrorCodes {
		r.serverErrors.WithLabelValues(string(c))
	}
	r.serverErrors.WithLabelValues(telemetry.LabelOther)
	if cfg.Mode == config.ModePingpong && cfg.PushIntervalMs > 0 {
		r.pushReceived = counter("wsmon_client_push_received_total", "Push frames received.", nil)
		r.pushGap = hist("wsmon_client_push_gap_seconds", "Client-clock interval between consecutive pushes on one connection.", telemetry.RTTBuckets, nil)
		r.pushLost = counter("wsmon_client_push_lost_total", "Skipped push_seq values.", nil)
		r.pushOutOfOrder = counter("wsmon_client_push_out_of_order_total", "Pushes below the push_seq high-water mark.", nil)
	}

	// Idle probe.
	if cfg.Mode == config.ModeIdle {
		r.idleProbe = counterVec("wsmon_client_idle_probe_total", "Idle ladder rungs by result.", nil, "idle_bucket", "result")
		for _, rung := range cfg.IdleLadder {
			for _, res := range []probe.IdleResult{probe.IdleSurvived, probe.IdleDropped} {
				r.idleProbe.WithLabelValues(config.IdleBucketLabel(rung), string(res))
			}
		}
		r.idleClose = counterVec("wsmon_client_idle_probe_close_total", "Close code of dropped idle rungs; none for an echo timeout.", nil, "idle_bucket", "close_code")
	}

	// Probe self-health.
	r.loopLag = hist("wsmon_client_loop_lag_seconds", "Self-monitoring ticker scheduling delay.", LoopLagBuckets, nil)
	bi := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "wsmon_client_build_info", Help: "Build information and client identity (WSMON_NAME); always 1."}, []string{"version", "go_version", "name"})
	must(bi)
	bi.WithLabelValues(version, runtime.Version(), cfg.Name).Set(1)
	ch := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "wsmon_client_config_hash", Help: "Hash of the resolved configuration, secrets excluded; always 1."}, []string{"hash"})
	must(ch)
	ch.WithLabelValues(configHash).Set(1)
	gauge("wsmon_client_start_time_seconds", "Process start time, Unix seconds.", nil).Set(float64(start.UnixNano()) / 1e9)
	if cfg.RemoteWriteURL != "" {
		r.rwFailures = counter("wsmon_client_remote_write_failures_total", "Failed remote-write pushes.", nil)
		r.rwLastSuccess = gauge("wsmon_client_remote_write_last_success_timestamp_seconds", "Time of the last successful remote-write push, Unix seconds.", nil)
	}
	return r
}

func secs(d time.Duration) float64 { return d.Seconds() }

func (r *Recorder) ConnectAttempt() { r.connectAttempts.Inc() }
func (r *Recorder) ConnectFailure(rs probe.Reason) {
	r.connectFailures.WithLabelValues(string(rs)).Inc()
}
func (r *Recorder) ConnectPhase(ph probe.Phase, d time.Duration) {
	r.connectDuration.WithLabelValues(string(ph)).Observe(secs(d))
}

func (r *Recorder) ConnectionUp(up bool, since time.Time) {
	v := 0.0
	if up {
		v = 1
		r.upSince.Store(since.UnixNano())
	} else {
		r.upSince.Store(0)
	}
	r.connectionUp.Set(v)
	r.connsActive.Set(v)
}

func (r *Recorder) Closed(code int, init probe.Initiator, lifetime time.Duration) {
	l := telemetry.CloseCodeLabel(code)
	r.closes.WithLabelValues(l, string(init)).Inc()
	r.lifetime.WithLabelValues(l).Observe(secs(lifetime))
}

func (r *Recorder) Reconnect()                      { r.reconnects.Inc() }
func (r *Recorder) Sent(p probe.Payload)            { r.sent.WithLabelValues(string(p)).Inc() }
func (r *Recorder) Echoed(p probe.Payload)          { r.echoed.WithLabelValues(string(p)).Inc() }
func (r *Recorder) Lost(p probe.Payload)            { r.lost.WithLabelValues(string(p)).Inc() }
func (r *Recorder) Late(p probe.Payload)            { r.late.WithLabelValues(string(p)).Inc() }
func (r *Recorder) Mismatched(f probe.Field)        { r.mismatched.WithLabelValues(string(f)).Inc() }
func (r *Recorder) Duplicate()                      { r.duplicate.Inc() }
func (r *Recorder) OutOfOrder()                     { r.outOfOrder.Inc() }
func (r *Recorder) Unexpected()                     { r.unexpected.Inc() }
func (r *Recorder) ClockOffset(d time.Duration)     { r.clockOffset.Set(secs(d)) }
func (r *Recorder) TimeAnomaly(k probe.AnomalyKind) { r.timeAnomaly.WithLabelValues(string(k)).Inc() }

func observe(o prometheus.Observer, v float64, ex probe.Exemplar) {
	telemetry.ObserveWithTrace(o, v, ex.TraceID)
}

func (r *Recorder) RTT(p probe.Payload, rtt, rttNet, serverTime time.Duration, serverName string, ex probe.Exemplar) {
	observe(r.rtt.WithLabelValues(string(p), serverName), secs(rtt), ex)
	observe(r.rttNet.WithLabelValues(string(p)), secs(rttNet), ex)
	r.serverProcessing.Observe(secs(serverTime))
}

func (r *Recorder) BurstCompletion(p probe.Payload, d time.Duration) {
	if r.burstCompletion != nil {
		r.burstCompletion.WithLabelValues(string(p)).Observe(secs(d))
	}
}

func (r *Recorder) ServerError(code protocol.ErrorCode) {
	r.serverErrors.WithLabelValues(telemetry.ErrorCodeLabel(code)).Inc()
}

// Push metrics are registered only when push is requested. An unrequested
// push (a contract violation, already logged at WARN by the probe) is not
// counted: registering the series on first use would make the metric set
// depend on server behaviour.
func (r *Recorder) PushReceived() {
	if r.pushReceived != nil {
		r.pushReceived.Inc()
	}
}

func (r *Recorder) PushGap(d time.Duration) {
	if r.pushGap != nil {
		r.pushGap.Observe(secs(d))
	}
}

func (r *Recorder) PushLost(n int64) {
	if r.pushLost != nil {
		r.pushLost.Add(float64(n))
	}
}

func (r *Recorder) PushOutOfOrder() {
	if r.pushOutOfOrder != nil {
		r.pushOutOfOrder.Inc()
	}
}

func (r *Recorder) IdleProbe(bucket string, res probe.IdleResult) {
	if r.idleProbe != nil {
		r.idleProbe.WithLabelValues(bucket, string(res)).Inc()
	}
}

func (r *Recorder) IdleClose(bucket, closeCode string) {
	if r.idleClose == nil {
		return
	}
	l := closeCode
	if l != probe.IdleCloseNone {
		n, err := strconv.Atoi(closeCode)
		if err != nil {
			l = telemetry.LabelOther
		} else {
			l = telemetry.CloseCodeLabel(n)
		}
	}
	r.idleClose.WithLabelValues(bucket, l).Inc()
}

// LoopLag observes one self-monitoring tick's scheduling delay.
func (r *Recorder) LoopLag(d time.Duration) { r.loopLag.Observe(secs(d)) }
