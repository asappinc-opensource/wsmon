package integration

import (
	"testing"
	"time"

	"github.com/asappinc-opensource/wsmon/test/client/fakeserver"
)

// TC-TIME-02 / TC-TIME-03: a ±5 s server clock leaves rtt and rtt_net
// unaffected, the offset gauge reads ≈ ±5 s with a WARN, and no negative
// RTT is emitted. (TC-TIME-01, 04, 06 are unit tests on the pure maths in
// internal/probe; TC-TIME-05's Recorder half is TestTCTIME05_NoOneWayMethod.)
func TestTCTIME02_03_ClockSkew(t *testing.T) {
	for _, tc := range []struct {
		name string
		skew time.Duration
	}{{"TC-TIME-02/+5s", 5 * time.Second}, {"TC-TIME-03/-5s", -5 * time.Second}} {
		t.Run(tc.name, func(t *testing.T) {
			srv := fakeserver.New(t, fakeserver.Behaviour{ClockSkew: tc.skew})
			h := newHarness(t, probeEnv(srv.URL))
			h.Wait("clock offset observed", func() bool { return len(h.rec.Obs("clock_offset")) >= 2 }, 5*time.Second)
			h.Stop()
			for _, o := range h.rec.Obs("clock_offset") {
				if d := o.Value - tc.skew; d.Abs() > 100*time.Millisecond {
					t.Errorf("offset %v, want ≈ %v", o.Value, tc.skew)
				}
			}
			for _, k := range []string{"rtt{small}", "rtt{large}", "rtt_net{small}", "rtt_net{large}"} {
				for _, o := range h.rec.Obs(k) {
					if o.Value < 0 || o.Value > 500*time.Millisecond {
						t.Errorf("%s = %v: skew leaked into latency", k, o.Value)
					}
				}
			}
			if len(h.rec.Obs("rtt{small}")) == 0 || h.rec.SumPrefix("time_anomaly") != 0 {
				t.Errorf("recorder:\n%s", h.rec.Dump())
			}
			if len(h.logs.Find("WARN", "clock offset")) == 0 {
				t.Error("no clock offset WARN")
			}
		})
	}
}

// TC-TIME-04 end to end: t3 < t2 → server_time 0, rtt_net ≥ 0, anomaly
// counted, RTT still observed.
func TestTCTIME04_ServerTimeNegativeEndToEnd(t *testing.T) {
	srv := fakeserver.New(t, fakeserver.Behaviour{NegativeServerTime: true})
	h := newHarness(t, probeEnv(srv.URL))
	h.WaitCount("time_anomaly{server_time_negative}", 2, 5*time.Second)
	h.Stop()
	for _, o := range h.rec.Obs("server_time") {
		if o.Value != 0 {
			t.Errorf("server_time %v, want 0", o.Value)
		}
	}
	for _, o := range append(h.rec.Obs("rtt_net{small}"), h.rec.Obs("rtt_net{large}")...) {
		if o.Value < 0 {
			t.Errorf("negative rtt_net %v", o.Value)
		}
	}
	if h.Count("time_anomaly{rtt_negative}") != 0 {
		t.Error("unexpected rtt_negative")
	}
}
