package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/internal/server/servertel"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
)

// maxRetainedReadBuf caps the read buffer kept between frames, so one large
// frame does not pin up to max_frame_bytes per idle connection.
const maxRetainedReadBuf = 64 << 10

// pushRingSize is how many unacknowledged pushes are tracked per connection.
const pushRingSize = 64

// errServerClosed ends the read loop after the server initiated a close.
var errServerClosed = errors.New("closed by server")

// conn is the state of one connection. The reader goroutine (run/readLoop)
// owns everything except the fields marked otherwise; the controller
// goroutine owns hello timeout, push and ping scheduling, and shutdown.
// Nothing here is shared with other connections.
type conn struct {
	s  *Server
	ws *websocket.Conn

	// ctx is cancelled when the connection ends. It is never passed to a
	// read: a cancellable read context is a read deadline.
	ctx    context.Context
	cancel context.CancelFunc

	// Reader-owned.
	in       protocol.Inbound
	rbuf     bytes.Buffer
	limited  io.LimitedReader
	wbuf     []byte
	haveSeq  bool
	lastSeq  int64
	rlWindow time.Time
	rlCount  int

	// Written once by the reader before hello_ack, read by the controller
	// after receiving from helloDone.
	connIDRaw []byte
	connID    string
	accepted  protocol.Options
	helloDone chan struct{}
	active    atomic.Bool

	// Controller-owned.
	pbuf    []byte
	pushSeq int64
	pinging atomic.Bool

	// closeCode is the code of the first server-initiated close, 0 if none.
	closeCode atomic.Int32

	pushMu sync.Mutex
	pushes [pushRingSize]pushRecord
}

type pushRecord struct {
	seq  int64
	sent time.Time
	span servertel.PushSpan
}

func newConn(s *Server, ws *websocket.Conn) *conn {
	c := &conn{s: s, ws: ws, helloDone: make(chan struct{})}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

// run drives the connection to completion and records how it closed.
func (c *conn) run() {
	ctrlDone := make(chan struct{})
	go func() { defer close(ctrlDone); c.control() }()

	err := c.readLoop()

	code := int(c.closeCode.Load())
	if code == 0 {
		code = int(websocket.CloseStatus(err))
		if code == -1 {
			// No close frame was received and we did not send one: the
			// peer or an intermediary dropped the connection.
			code = protocol.CloseAbnormal
		}
	}
	c.cancel()
	<-ctrlDone
	_ = c.ws.CloseNow()
	c.endPushes(time.Now())
	c.s.metrics.Close(code)
	c.s.log.Debug("connection closed", "conn_id", c.connID, "close_code", code, "err", err)
}

// control runs the connection's timers: the hello timeout, push and ping
// schedules, and the shutdown broadcast.
func (c *conn) control() {
	helloTimer := time.NewTimer(c.s.cfg.HelloTimeout)
	defer helloTimer.Stop()
	helloDone := c.helloDone
	var connID []byte // set once hello is done; c.connIDRaw is reader-owned until then
	var pushC, pingC <-chan time.Time

	for {
		select {
		case <-c.ctx.Done():
			return

		case <-helloTimer.C:
			if !c.active.Load() {
				c.closeWith(protocol.CloseProtocolError, "hello timeout")
				return
			}

		case <-helloDone:
			helloDone = nil
			helloTimer.Stop()
			connID = c.connIDRaw
			if ms := c.accepted.PushIntervalMs; ms > 0 {
				t := time.NewTicker(time.Duration(ms) * time.Millisecond)
				defer t.Stop()
				pushC = t.C
			}
			// Keepalive pings exist only when the client asked for them.
			// With server_keepalive=false no ping is ever sent.
			if c.accepted.ServerKeepalive {
				t := time.NewTicker(c.s.cfg.ServerPingInterval)
				defer t.Stop()
				pingC = t.C
			}

		case <-pushC:
			if err := c.sendPush(); err != nil {
				return
			}

		case <-pingC:
			c.ping()

		case <-c.s.shutdown:
			c.fail(connID, protocol.ErrShuttingDown, 0, "server shutting down")
			return
		}
	}
}

// readLoop reads frames until the connection ends. It never sets a read
// deadline: the read context has no Done channel, so the library installs
// no timeout.
func (c *conn) readLoop() error {
	readCtx := context.Background()
	limit := c.s.cfg.MaxFrameBytes
	for {
		typ, r, err := c.ws.Reader(readCtx)
		if err != nil {
			return err
		}
		c.rbuf.Reset()
		c.limited.R, c.limited.N = r, limit+1
		n, err := c.rbuf.ReadFrom(&c.limited)
		// t2: immediately after the read completes, before parsing.
		t2 := time.Now()
		if err != nil {
			return err
		}
		if n > limit {
			c.fail(c.connIDRaw, protocol.ErrTooLarge, 0, fmt.Sprintf("frame exceeds max_frame_bytes (%d)", limit))
			return errServerClosed
		}
		if typ != websocket.MessageText {
			c.fail(c.connIDRaw, protocol.ErrBadJSON, 0, "binary frames are not supported")
			return errServerClosed
		}

		if !c.allow(t2) {
			c.sendError(protocol.ErrRateLimited, 0, "message rate cap exceeded; frame dropped")
		} else if !c.handle(c.rbuf.Bytes(), t2) {
			return errServerClosed
		}

		if c.rbuf.Cap() > maxRetainedReadBuf {
			c.rbuf = bytes.Buffer{}
		}
	}
}

// allow applies max_msgs_per_sec_per_conn as a fixed one-second window.
func (c *conn) allow(now time.Time) bool {
	if now.Sub(c.rlWindow) >= time.Second {
		c.rlWindow, c.rlCount = now, 0
	}
	c.rlCount++
	return c.rlCount <= c.s.cfg.MaxMsgsPerSec
}

// handle processes one text frame. It reports false when the connection is
// being closed.
func (c *conn) handle(b []byte, t2 time.Time) bool {
	in := &c.in
	if err := in.Decode(b); err != nil {
		c.fail(c.connIDRaw, protocol.ErrBadJSON, 0, "frame is not a valid JSON object")
		return false
	}
	if v, ok := protocol.Int64(in.V); !ok || v != protocol.Version {
		c.fail(c.connIDRaw, protocol.ErrBadVersion, 0, "unsupported protocol version "+rawForMessage(in.V))
		return false
	}
	typ, present := in.FrameType()

	if !c.active.Load() {
		if typ != protocol.TypeHello {
			c.fail(c.connIDRaw, protocol.ErrNoHello, 0, "first frame must be hello")
			return false
		}
		return c.handleHello(t2)
	}

	switch {
	case typ == protocol.TypeProbe:
		return c.handleProbe(t2)
	case typ == protocol.TypePushAck:
		return c.handlePushAck(t2)
	case typ == protocol.TypeHello:
		// A repeated hello changes nothing; options are fixed per connection.
		return true
	case !present:
		return c.sendError(protocol.ErrMissingField, 0, "missing field: type")
	default:
		return c.sendError(protocol.ErrBadType, 0, "unknown frame type "+rawForMessage(in.Type))
	}
}

func (c *conn) handleHello(t2 time.Time) bool {
	in := &c.in
	if !protocol.IsString(in.ConnID) {
		return c.sendError(protocol.ErrMissingField, 0, "missing field: conn_id")
	}
	var req protocol.Options
	if protocol.Present(in.Options) {
		if err := json.Unmarshal(in.Options, &req); err != nil {
			return c.sendError(protocol.ErrMissingField, 0, "malformed field: options")
		}
	}
	c.s.metrics.MsgHello.Inc()

	c.connIDRaw = append([]byte(nil), in.ConnID...)
	c.connID, _ = protocol.String(in.ConnID)
	c.accepted = protocol.HonourOptions(req)

	c.wbuf = protocol.AppendHelloAck(c.wbuf[:0], c.connIDRaw, c.s.serverJSON, c.accepted)
	t3 := time.Now()
	// ACTIVE before the ack is written, so a probe sent the instant the
	// client reads hello_ack is never mistaken for a pre-hello frame.
	c.active.Store(true)
	close(c.helloDone)
	if err := c.write(c.wbuf); err != nil {
		return false
	}
	c.s.metrics.HandleHello.Observe(t3.Sub(t2).Seconds())
	return true
}

func (c *conn) handleProbe(t2 time.Time) bool {
	in := &c.in
	seq, seqOK := protocol.Int64(in.Seq)
	if field := missingProbeField(in, seqOK); field != "" {
		c.traceProbeError(seq, t2, protocol.ErrMissingField)
		return c.sendError(protocol.ErrMissingField, seq, "missing field: "+field)
	}
	if c.haveSeq && seq <= c.lastSeq {
		c.traceProbeError(seq, t2, protocol.ErrSeqRegression)
		return c.sendError(protocol.ErrSeqRegression, seq, fmt.Sprintf("seq %d does not follow %d", seq, c.lastSeq))
	}
	c.haveSeq, c.lastSeq = true, seq
	c.s.metrics.MsgProbe.Inc()

	var off int
	c.wbuf, off = protocol.AppendEcho(c.wbuf[:0], in, t2.UnixNano(), c.s.serverJSON)
	// t3: after serialisation, immediately before the write.
	t3 := time.Now()
	protocol.PatchEpochNs(c.wbuf, off, t3.UnixNano())
	if err := c.write(c.wbuf); err != nil {
		return false
	}

	// Everything below runs after the write and outside t3 - t2.
	traceID := ""
	if c.s.tracing.Enabled() {
		tp, _ := protocol.String(in.Traceparent)
		connID, _ := protocol.String(in.ConnID)
		if c.s.tracing.Echo(tp, connID, seq, t2, t3) {
			traceID, _ = protocol.String(in.TraceID)
		}
	}
	telemetry.ObserveWithTrace(c.s.metrics.HandleProbe, t3.Sub(t2).Seconds(), traceID)
	return true
}

// missingProbeField names the first required probe field that is
// absent or of the wrong JSON type, or returns "".
func missingProbeField(in *protocol.Inbound, seqOK bool) string {
	switch {
	case !protocol.IsString(in.TraceID):
		return "trace_id"
	case !protocol.IsString(in.SpanID):
		return "span_id"
	case !protocol.IsString(in.Traceparent):
		return "traceparent"
	case !protocol.IsString(in.ConnID):
		return "conn_id"
	case !seqOK:
		return "seq"
	}
	if _, ok := protocol.Int64(in.ClientSendEpochNs); !ok {
		return "client_send_epoch_ns"
	}
	// Presence only: the contents of client.* are never inspected.
	if !protocol.Present(in.Client) {
		return "client"
	}
	return ""
}

func (c *conn) traceProbeError(seq int64, at time.Time, code protocol.ErrorCode) {
	if !c.s.tracing.Enabled() {
		return
	}
	tp, _ := protocol.String(c.in.Traceparent)
	connID, _ := protocol.String(c.in.ConnID)
	c.s.tracing.EchoError(tp, connID, seq, at, code)
}

func (c *conn) handlePushAck(t2 time.Time) bool {
	seq, ok := protocol.Int64(c.in.PushSeq)
	if !ok {
		return c.sendError(protocol.ErrMissingField, 0, "missing field: push_seq")
	}
	c.s.metrics.MsgPushAck.Inc()
	if seq <= 0 {
		return true
	}

	c.pushMu.Lock()
	rec := &c.pushes[seq%pushRingSize]
	if rec.seq != seq {
		// Unknown, duplicate or already expired: nothing to measure.
		c.pushMu.Unlock()
		return true
	}
	sent, span := rec.sent, rec.span
	*rec = pushRecord{}
	c.pushMu.Unlock()

	// Both instants are on the server's monotonic clock, so this is a
	// skew-free round trip. push_ack.client_recv_epoch_ns is never used.
	rtt := t2.Sub(sent)
	c.s.metrics.PushAck.Observe(rtt.Seconds())
	span.Acked(t2, rtt)
	return true
}

// sendPush emits the next push frame. push_seq starts at 1 and never
// skips.
func (c *conn) sendPush() error {
	now := time.Now()
	c.expirePushes(now)
	c.pushSeq++
	seq := c.pushSeq
	span, traceID, spanID, traceparent := c.s.tracing.StartPush(c.connID, seq, now)

	var off int
	c.pbuf, off = protocol.AppendPush(c.pbuf[:0], traceID, spanID, traceparent, c.connIDRaw, seq, c.s.serverJSON)
	t3 := time.Now()
	protocol.PatchEpochNs(c.pbuf, off, t3.UnixNano())

	c.pushMu.Lock()
	rec := &c.pushes[seq%pushRingSize]
	if rec.seq != 0 {
		rec.span.Lost(t3)
	}
	*rec = pushRecord{seq: seq, sent: t3, span: span}
	c.pushMu.Unlock()

	if err := c.write(c.pbuf); err != nil {
		return err
	}
	c.s.metrics.MsgPush.Inc()
	return nil
}

// expirePushes ends the spans of pushes unacknowledged for PushAckTimeout.
func (c *conn) expirePushes(now time.Time) {
	c.pushMu.Lock()
	defer c.pushMu.Unlock()
	for i := range c.pushes {
		rec := &c.pushes[i]
		if rec.seq != 0 && now.Sub(rec.sent) >= servertel.PushAckTimeout {
			rec.span.Lost(now)
			*rec = pushRecord{}
		}
	}
}

// endPushes ends every outstanding push span when the connection closes.
func (c *conn) endPushes(now time.Time) {
	c.pushMu.Lock()
	defer c.pushMu.Unlock()
	for i := range c.pushes {
		if c.pushes[i].seq != 0 {
			c.pushes[i].span.Lost(now)
			c.pushes[i] = pushRecord{}
		}
	}
}

// ping sends one keepalive ping unless one is already outstanding. Only
// reachable when the client requested server_keepalive=true. A missing pong
// never closes the connection: pings keep proxies' idle timers fed, they are
// not a liveness check.
func (c *conn) ping() {
	if !c.pinging.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.pinging.Store(false)
		ctx, cancel := context.WithTimeout(c.ctx, c.s.cfg.ServerPingInterval)
		defer cancel()
		_ = c.ws.Ping(ctx)
	}()
}

// write sends one text frame, bounded by write_timeout. A timed-out write is
// closed with 1011. write_timeout bounds the write only; it is not an
// idle timeout.
func (c *conn) write(b []byte) error {
	ctx, cancel := context.WithTimeout(c.ctx, c.s.cfg.WriteTimeout)
	err := c.ws.Write(ctx, websocket.MessageText, b)
	timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
	cancel()
	if err != nil && timedOut {
		c.closeWith(protocol.CloseInternalError, "write timeout")
	}
	return err
}

// sendError sends an error frame that leaves the connection open. It
// reports whether the connection is still usable.
func (c *conn) sendError(code protocol.ErrorCode, seq int64, msg string) bool {
	c.s.metrics.Error(code)
	b := protocol.AppendError(nil, c.connIDRaw, seq, code, msg)
	return c.write(b) == nil
}

// fail sends an error frame and then closes with the code assigned to
// it. The error frame is always written before the close frame.
// connID is passed in because the reader and controller own different
// copies of it.
func (c *conn) fail(connID []byte, code protocol.ErrorCode, seq int64, msg string) {
	if c.closeCode.Load() != 0 {
		return
	}
	c.s.metrics.Error(code)
	_ = c.write(protocol.AppendError(nil, connID, seq, code, msg))
	c.closeWith(code.CloseCode(), string(code))
}

// closeWith performs a close handshake with code. Every server-initiated
// close goes through here, so the server always sends a close frame and
// never manufactures a 1006.
func (c *conn) closeWith(code int, reason string) {
	if !c.closeCode.CompareAndSwap(0, int32(code)) {
		return
	}
	_ = c.ws.Close(websocket.StatusCode(code), reason)
}

// rawForMessage renders a raw JSON value for an error message, truncated.
func rawForMessage(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "(absent)"
	}
	const maxLen = 32
	if len(raw) > maxLen {
		return string(raw[:maxLen]) + "..."
	}
	return string(raw)
}
