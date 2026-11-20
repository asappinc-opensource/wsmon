// Package rwsink is an in-test Prometheus remote-write 1.0 receiver
// (TC-TEL-11). It decodes requests with its own minimal protobuf reader,
// independent of the probe's encoder. It is test code only.
package rwsink

import (
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"
)

// Mode is how the receiver answers.
type Mode int

const (
	OK     Mode = iota // 204, store the request
	Hang               // block until the client gives up
	Refuse             // 500
)

// Sample is one decoded sample.
type Sample struct {
	Labels      map[string]string
	Value       float64
	TimestampMs int64
}

// Key renders a series identity: name{a="1",b="2"} with labels sorted.
func (s Sample) Key() string { return SeriesKey(s.Labels) }

// SeriesKey renders labels (including __name__) canonically.
func SeriesKey(ls map[string]string) string {
	var names []string
	for k := range ls {
		if k != "__name__" {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString(ls["__name__"])
	b.WriteByte('{')
	for i, k := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k + `="` + ls[k] + `"`)
	}
	b.WriteByte('}')
	return b.String()
}

// Request is one received push.
type Request struct {
	At      time.Time
	Header  http.Header
	Samples []Sample
	Err     error // decode error, if any
}

// Sink is a running receiver.
type Sink struct {
	URL string
	srv *httptest.Server

	closed    chan struct{}
	closeOnce sync.Once

	mu   sync.Mutex
	mode Mode
	reqs []Request
	hung int
}

// Start runs a receiver; it is closed in t.Cleanup.
func Start(t testing.TB) *Sink {
	s := &Sink{closed: make(chan struct{})}
	s.srv = httptest.NewServer(http.HandlerFunc(s.handle))
	s.URL = s.srv.URL + "/api/v1/write"
	t.Cleanup(s.Close)
	return s
}

// Close stops the receiver; later pushes get connection refused.
func (s *Sink) Close() {
	s.closeOnce.Do(func() { close(s.closed) })
	s.srv.CloseClientConnections()
	s.srv.Close()
}

// SetMode changes how later requests are answered.
func (s *Sink) SetMode(m Mode) {
	s.mu.Lock()
	s.mode = m
	s.mu.Unlock()
}

// Requests returns every stored (OK-mode) request.
func (s *Sink) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.reqs...)
}

// Hung is how many requests were held in Hang mode.
func (s *Sink) Hung() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hung
}

func (s *Sink) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	mode := s.mode
	if mode == Hang {
		s.hung++
	}
	s.mu.Unlock()
	switch mode {
	case Hang:
		// Read the body first: net/http only notices the client going
		// away (and cancels r.Context) once the body is consumed.
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-s.closed:
		}
		return
	case Refuse:
		http.Error(w, "refused", http.StatusInternalServerError)
		return
	}
	body, err := io.ReadAll(r.Body)
	req := Request{At: time.Now(), Header: r.Header.Clone()}
	if err == nil {
		var raw []byte
		raw, err = snappy.Decode(nil, body)
		if err == nil {
			req.Samples, err = decodeWriteRequest(raw)
		}
	}
	req.Err = err
	s.mu.Lock()
	s.reqs = append(s.reqs, req)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

var errMalformed = errors.New("rwsink: malformed protobuf")

// fields iterates over the top-level fields of one message.
func fields(b []byte, f func(num protowire.Number, typ protowire.Type, v []byte, u uint64) error) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return errMalformed
		}
		b = b[n:]
		var v []byte
		var u uint64
		switch typ {
		case protowire.BytesType:
			v, n = protowire.ConsumeBytes(b)
		case protowire.VarintType:
			u, n = protowire.ConsumeVarint(b)
		case protowire.Fixed64Type:
			u, n = protowire.ConsumeFixed64(b)
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
		}
		if n < 0 {
			return errMalformed
		}
		b = b[n:]
		if err := f(num, typ, v, u); err != nil {
			return err
		}
	}
	return nil
}

func decodeWriteRequest(b []byte) ([]Sample, error) {
	var out []Sample
	err := fields(b, func(num protowire.Number, typ protowire.Type, ts []byte, _ uint64) error {
		if num != 1 || typ != protowire.BytesType {
			return nil
		}
		labels := map[string]string{}
		var samples []Sample
		err := fields(ts, func(num protowire.Number, typ protowire.Type, v []byte, _ uint64) error {
			switch {
			case num == 1 && typ == protowire.BytesType: // Label
				var name, value string
				if err := fields(v, func(n protowire.Number, _ protowire.Type, s []byte, _ uint64) error {
					if n == 1 {
						name = string(s)
					} else if n == 2 {
						value = string(s)
					}
					return nil
				}); err != nil {
					return err
				}
				if _, dup := labels[name]; dup {
					return errors.New("rwsink: duplicate label " + name)
				}
				labels[name] = value
			case num == 2 && typ == protowire.BytesType: // Sample
				var s Sample
				if err := fields(v, func(n protowire.Number, _ protowire.Type, _ []byte, u uint64) error {
					if n == 1 {
						s.Value = math.Float64frombits(u)
					} else if n == 2 {
						s.TimestampMs = int64(u)
					}
					return nil
				}); err != nil {
					return err
				}
				samples = append(samples, s)
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, s := range samples {
			s.Labels = labels
			out = append(out, s)
		}
		return nil
	})
	return out, err
}
