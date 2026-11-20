package probetel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
)

// RemoteWriteJob is the job label added to every pushed series.
// No instance label is ever added.
const RemoteWriteJob = telemetry.ServiceClient

// rwWarnEvery rate-limits the remote-write failure WARN.
const rwWarnEvery = time.Minute

// pusher sends the current registry snapshot with Prometheus remote-write
// 1.0 every REMOTE_WRITE_INTERVAL on its own goroutine. It shares no
// lock with the probe: gathering happens here, and only the current snapshot
// is ever sent (no queue, no history).
type pusher struct {
	cfg     config.Client
	version string
	gather  prometheus.Gatherer
	rec     *Recorder
	log     *slog.Logger
	client  *http.Client

	stop chan struct{}
	done chan struct{}

	mu       sync.Mutex // serialises pushes and guards lastWarn
	lastWarn time.Time
}

func newPusher(cfg config.Client, version string, g prometheus.Gatherer, rec *Recorder, log *slog.Logger) *pusher {
	return &pusher{
		cfg: cfg, version: version, gather: g, rec: rec, log: log,
		client: &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()},
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (p *pusher) start() {
	go func() {
		defer close(p.done)
		t := time.NewTicker(p.cfg.RemoteWriteInterval)
		defer t.Stop()
		for {
			select {
			case <-p.stop:
				return
			case <-t.C:
				ctx, cancel := context.WithTimeout(context.Background(), p.cfg.RemoteWriteTimeout)
				p.push(ctx)
				cancel()
			}
		}
	}()
}

// final stops the loop and does one last push bounded by ctx.
func (p *pusher) final(ctx context.Context) {
	close(p.stop)
	select {
	case <-p.done:
	case <-ctx.Done():
		return
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.RemoteWriteTimeout)
	defer cancel()
	p.push(ctx)
}

// push gathers, encodes and sends one snapshot. Failures are counted and
// logged (rate-limited); the next tick is the retry.
func (p *pusher) push(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.send(ctx); err != nil {
		p.rec.rwFailures.Inc()
		if now := time.Now(); now.Sub(p.lastWarn) >= rwWarnEvery {
			p.lastWarn = now
			p.log.Warn("remote-write push failed; retrying at the next interval", "error", err.Error())
		}
		return
	}
	p.rec.rwLastSuccess.Set(float64(time.Now().UnixNano()) / 1e9)
}

func (p *pusher) send(ctx context.Context) error {
	mfs, err := p.gather.Gather()
	if err != nil {
		return fmt.Errorf("gather: %w", err)
	}
	body := snappy.Encode(nil, EncodeWriteRequest(mfs, time.Now().UnixMilli()))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.RemoteWriteURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	req.Header.Set("User-Agent", telemetry.ServiceClient+"/"+p.version)
	switch {
	case p.cfg.RemoteWriteBearerToken != "":
		req.Header.Set("Authorization", "Bearer "+p.cfg.RemoteWriteBearerToken)
	case p.cfg.RemoteWriteUsername != "":
		req.SetBasicAuth(p.cfg.RemoteWriteUsername, p.cfg.RemoteWritePassword)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
	}
	return nil
}

// Label is one remote-write label.
type Label struct{ Name, Value string }

// Series is one remote-write time series with a single sample.
type Series struct {
	Labels      []Label // sorted by name; includes __name__ and job
	Value       float64
	TimestampMs int64
}

// Series expands gathered families into remote-write series, exactly as the
// text exposition names them: counters keep _total; classic histograms
// become _bucket{le} (including +Inf), _sum and _count. Native-histogram
// data and exemplars are not pushed. job="wsmon-probe" is added; no
// instance label.
func ToSeries(mfs []*dto.MetricFamily, tsMs int64) []Series {
	var out []Series
	for _, mf := range mfs {
		name := mf.GetName()
		for _, m := range mf.GetMetric() {
			base := make([]Label, 0, len(m.GetLabel())+3)
			for _, lp := range m.GetLabel() {
				base = append(base, Label{lp.GetName(), lp.GetValue()})
			}
			base = append(base, Label{"job", RemoteWriteJob})
			add := func(suffix string, v float64, extra ...Label) {
				ls := make([]Label, 0, len(base)+len(extra)+1)
				ls = append(ls, Label{"__name__", name + suffix})
				ls = append(ls, base...)
				ls = append(ls, extra...)
				sort.Slice(ls, func(i, j int) bool { return ls[i].Name < ls[j].Name })
				out = append(out, Series{Labels: ls, Value: v, TimestampMs: tsMs})
			}
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				add("", m.GetCounter().GetValue())
			case dto.MetricType_GAUGE:
				add("", m.GetGauge().GetValue())
			case dto.MetricType_UNTYPED:
				add("", m.GetUntyped().GetValue())
			case dto.MetricType_HISTOGRAM:
				h := m.GetHistogram()
				if len(h.GetBucket()) == 0 {
					continue // native-only histogram: not pushed
				}
				inf := false
				for _, b := range h.GetBucket() {
					inf = inf || math.IsInf(b.GetUpperBound(), +1)
					add("_bucket", float64(b.GetCumulativeCount()), Label{"le", formatLe(b.GetUpperBound())})
				}
				if !inf {
					add("_bucket", float64(h.GetSampleCount()), Label{"le", "+Inf"})
				}
				add("_sum", h.GetSampleSum())
				add("_count", float64(h.GetSampleCount()))
			}
		}
	}
	return out
}

// formatLe renders a bucket bound as the text exposition does.
func formatLe(f float64) string {
	switch {
	case math.IsInf(f, +1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	case math.IsNaN(f):
		return "NaN"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// EncodeWriteRequest hand-encodes a remote-write 1.0 WriteRequest:
//
//	WriteRequest { repeated TimeSeries timeseries = 1; }
//	TimeSeries   { repeated Label labels = 1; repeated Sample samples = 2; }
//	Label        { string name = 1; string value = 2; }
//	Sample       { double value = 1; int64 timestamp = 2; }
func EncodeWriteRequest(mfs []*dto.MetricFamily, tsMs int64) []byte {
	var out, ts, tmp []byte
	for _, s := range ToSeries(mfs, tsMs) {
		ts = ts[:0]
		for _, l := range s.Labels {
			tmp = tmp[:0]
			tmp = protowire.AppendTag(tmp, 1, protowire.BytesType)
			tmp = protowire.AppendString(tmp, l.Name)
			tmp = protowire.AppendTag(tmp, 2, protowire.BytesType)
			tmp = protowire.AppendString(tmp, l.Value)
			ts = protowire.AppendTag(ts, 1, protowire.BytesType)
			ts = protowire.AppendBytes(ts, tmp)
		}
		tmp = tmp[:0]
		tmp = protowire.AppendTag(tmp, 1, protowire.Fixed64Type)
		tmp = protowire.AppendFixed64(tmp, math.Float64bits(s.Value))
		tmp = protowire.AppendTag(tmp, 2, protowire.VarintType)
		tmp = protowire.AppendVarint(tmp, uint64(s.TimestampMs))
		ts = protowire.AppendTag(ts, 2, protowire.BytesType)
		ts = protowire.AppendBytes(ts, tmp)

		out = protowire.AppendTag(out, 1, protowire.BytesType)
		out = protowire.AppendBytes(out, ts)
	}
	return out
}
