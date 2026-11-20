package probe

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// writeReq is one frame for the writer goroutine.
type writeReq struct {
	frame []byte
	// t1Off is the client_send_epoch_ns slot of a probe, or -1. The writer
	// stamps t1 there immediately before Write, after any queueing.
	t1Off int
	seq   int64
	// helloWrite marks the hello, whose write time starts the hello phase.
	helloWrite bool
}

// helloResult is the first frame received on a connection.
type helloResult struct {
	ack *protocol.HelloAck
	at  time.Time
	err error // set if the first frame was anything but hello_ack
}

// modeResult says why a mode loop returned.
type modeResult int

const (
	resNone      modeResult = iota
	resShutdown             // ctx cancelled: graceful shutdown
	resClosed               // the connection is gone (reader exited)
	resReconnect            // the client decided to close with 1000 and reconnect
)

// writeQueue bounds queued frames. The largest burst is 100, plus
// push_acks.
const writeQueue = 256

// conn is one upgraded WebSocket connection. Goroutines: one writer (owns
// every write), one reader (no read deadline), one sweeper while the mode
// runs. The mode loop runs on the caller's (lifecycle) goroutine.
type conn struct {
	p   *Probe
	ws  *websocket.Conn
	raw net.Conn // the conn carrying WebSocket bytes; closing it abandons the connection

	connID        string
	corr          *correlator
	server        protocol.ServerInfo
	pushRequested bool
	upgradedAt    time.Time

	writeq     chan writeReq
	stopW      chan struct{}
	writerDone chan struct{}
	helloAt    atomic.Int64 // unix ns of the hello write

	readerDone chan struct{}
	readErr    error // valid once readerDone is closed
	helloCh    chan helloResult

	push    pushState // owned by the reader goroutine
	nextSeq int64     // last seq assigned; owned by the mode goroutine

	// lastInbound is the arrival time (unix ns) of the most recent valid
	// inbound frame of any type. dead is closed by the sweeper when the
	// connection is presumed half-open.
	lastInbound atomic.Int64
	dead        chan struct{}

	closeOnce sync.Once
	closeCode int
	closeInit Initiator
	startOnce sync.Once
}

func newConn(p *Probe, ws *websocket.Conn, raw net.Conn) *conn {
	return &conn{
		p: p, ws: ws, raw: raw,
		upgradedAt: p.now(),
		writeq:     make(chan writeReq, writeQueue),
		stopW:      make(chan struct{}),
		writerDone: make(chan struct{}),
		readerDone: make(chan struct{}),
		helloCh:    make(chan helloResult, 1),
		dead:       make(chan struct{}),
	}
}

func (c *conn) start() {
	c.startOnce.Do(func() {
		go c.writer()
		go c.reader()
	})
}

func (c *conn) helloWriteTime() time.Time {
	if ns := c.helloAt.Load(); ns != 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

// enqueue hands a frame to the writer. It returns false if the writer has
// stopped.
func (c *conn) enqueue(r writeReq) bool {
	select {
	case c.writeq <- r:
		return true
	case <-c.writerDone:
		return false
	}
}

// writer owns all writes. For probes it patches t1 in place, records it on
// the in-flight entry, then writes immediately. Writes use
// a context that is never cancelled: coder/websocket closes the connection
// when a write context expires, which would drop the socket without a close
// frame. A stuck write is unblocked by abandoning the raw conn instead.
func (c *conn) writer() {
	defer close(c.writerDone)
	for {
		var r writeReq
		select {
		case <-c.stopW:
			return
		case r = <-c.writeq:
		}
		if r.t1Off >= 0 {
			var now time.Time
			t1 := protocol.StampT1(r.frame, r.t1Off, func() int64 { now = time.Now(); return now.UnixNano() })
			c.corr.markSent(r.seq, t1, now)
		} else if r.helloWrite {
			c.helloAt.Store(time.Now().UnixNano())
		}
		if err := c.ws.Write(context.Background(), websocket.MessageText, r.frame); err != nil {
			c.p.log.Debug("write failed; abandoning connection", "conn_id", c.connID, "error", err.Error())
			_ = c.raw.Close()
			return
		}
	}
}

// reader loops on Read with no deadline. t4 is the first statement after
// Read returns, before any decoding.
func (c *conn) reader() {
	defer close(c.readerDone)
	acked := false
	for {
		typ, b, err := c.ws.Read(context.Background())
		t4 := time.Now()
		if err != nil {
			c.readErr = err
			return
		}
		if typ != websocket.MessageText {
			// The protocol allows text frames only.
			c.p.log.Warn("received a binary frame; ignoring", "conn_id", c.connID, "len", len(b))
			c.p.rec.Unexpected()
			continue
		}
		f, err := protocol.Decode(b)
		if err != nil {
			c.p.log.Warn("received an undecodable frame; ignoring", "conn_id", c.connID, "len", len(b), "error", err.Error())
			c.p.rec.Unexpected()
			continue
		}
		c.lastInbound.Store(t4.UnixNano())
		if !acked {
			res := helloResult{at: t4}
			switch f.Type {
			case protocol.TypeHelloAck:
				res.ack = f.HelloAck
				acked = true
			case protocol.TypeError:
				c.serverError(f.Error)
				res.err = fmt.Errorf("error frame before hello_ack: %s: %s", f.Error.Code, f.Error.Message)
			default:
				res.err = fmt.Errorf("%s frame before hello_ack", f.Type)
			}
			select {
			case c.helloCh <- res:
			default:
			}
			continue
		}
		switch f.Type {
		case protocol.TypeEcho:
			c.corr.onEcho(f.Echo, t4)
		case protocol.TypePush:
			c.onPush(f.Push, t4)
		case protocol.TypeError:
			c.serverError(f.Error)
		case protocol.TypeHelloAck:
			c.p.log.Warn("received a second hello_ack; ignoring", "conn_id", c.connID)
		}
	}
}

// serverError handles an error frame. The connection
// stays open unless the server closes it.
func (c *conn) serverError(e *protocol.Error) {
	c.p.log.Warn("server sent an error frame",
		"code", string(e.Code), "message", e.Message, "conn_id", e.ConnID, "seq", e.Seq)
	c.p.rec.ServerError(e.Code)
}

// remoteCloseCode maps a read error to the close code received: the code of
// the peer's close frame, or 1006 when none arrived.
func remoteCloseCode(err error) int {
	if s := websocket.CloseStatus(err); s != -1 {
		return int(s)
	}
	return protocol.CloseAbnormal
}

// readerClosed reports whether the reader has exited (the connection is
// gone).
func (c *conn) readerClosed() bool {
	select {
	case <-c.readerDone:
		return true
	default:
		return false
	}
}

// remoteCodeLabel is the close code of a connection the remote side ended.
// Only valid once readerDone is closed.
func (c *conn) remoteCodeLabel() string {
	return strconv.Itoa(remoteCloseCode(c.readErr))
}

// shut ends the connection exactly once and reports how. If the reader has
// already exited, the remote closed it. Otherwise the client closes with
// clientCode, sending a close frame (never a bare drop, which would
// manufacture a 1006), bounded by closeTimeout, after which the socket is
// abandoned. Returns only once the socket is closed, so the next dial cannot
// overlap it (TC-INT-14).
func (c *conn) shut(clientCode int, reason string) (int, Initiator) {
	c.closeOnce.Do(func() {
		c.start()
		if c.readerClosed() || clientCode == 0 {
			<-c.readerDone
			c.closeCode, c.closeInit = remoteCloseCode(c.readErr), InitiatorRemote
			_ = c.ws.CloseNow()
		} else {
			c.closeCode, c.closeInit = clientCode, InitiatorClient
			done := make(chan struct{})
			go func() {
				_ = c.ws.Close(websocket.StatusCode(clientCode), reason)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(closeTimeout):
				c.p.log.Warn("close handshake timed out; abandoning connection", "conn_id", c.connID)
				_ = c.raw.Close()
				<-done
			}
			<-c.readerDone
		}
		_ = c.raw.Close()
		close(c.stopW)
		<-c.writerDone
	})
	return c.closeCode, c.closeInit
}

// endEarly closes a connection that never reached the mode loop and records
// the closure.
func (c *conn) endEarly(clientCode int, reason string, since time.Time) (int, Initiator) {
	code, init := c.shut(clientCode, reason)
	c.p.rec.Closed(code, init, c.p.now().Sub(since))
	c.p.log.Info("connection closed", "conn_id", c.connID, "close_code", code, "initiator", string(init))
	return code, init
}

// run drives the mode on an established connection until it closes or the
// probe shuts down, then tears it down. It returns the connection lifetime.
func (c *conn) run(ctx context.Context) time.Duration {
	upAt := c.p.now()
	c.p.cur.Store(c)
	c.p.rec.ConnectionUp(true, upAt)
	c.p.log.Info("connected", "conn_id", c.connID, "server_name", c.server.Name, "server_version", c.server.Version)

	stopSweep := make(chan struct{})
	sweepDone := make(chan struct{})
	go c.sweeper(stopSweep, sweepDone)

	var res modeResult
	switch c.p.cfg.Mode {
	case "pingpong":
		res = c.runPingpong(ctx)
	case "burst":
		res = c.runBurst(ctx)
	case "idle":
		res = c.runIdle(ctx)
	default:
		res = resReconnect // unreachable: config validation rejects other modes
	}

	if res == resShutdown {
		c.drain()
	}
	code, init := c.shut(protocol.CloseNormal, "")
	close(stopSweep)
	<-sweepDone

	now := c.p.now()
	lifetime := now.Sub(upAt)
	c.corr.failAll(time.Now(), code)
	c.p.rec.Closed(code, init, lifetime)
	c.p.rec.ConnectionUp(false, time.Time{})
	c.p.log.Info("connection closed", "conn_id", c.connID, "close_code", code,
		"initiator", string(init), "lifetime", lifetime)
	return lifetime
}

// drain waits for in-flight echoes during graceful shutdown:
// up to min(MESSAGE_TIMEOUT, SHUTDOWN_TIMEOUT − shutdownMargin).
func (c *conn) drain() {
	wait := min(c.p.cfg.MessageTimeout, c.p.cfg.ShutdownTimeout-shutdownMargin)
	if wait <= 0 {
		return
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for c.corr.pending() > 0 {
		select {
		case <-deadline.C:
			return
		case <-c.readerDone:
			return
		case <-tick.C:
		}
	}
}

// deadAfterFactor sets DEAD_AFTER = deadAfterFactor × MESSAGE_TIMEOUT, the
// half-open detection threshold. With the
// default 10 s timeout that is 40 s: longer than a 30 s server stall plus one
// message timeout, so a stall recovers on the same connection (TC-FAULT-10),
// while a half-open socket is torn down (TC-FAULT-05). Deliberately not an
// env var (that would change the configuration surface).
const deadAfterFactor = 4

// sweeper runs every MESSAGE_TIMEOUT/4. In pingpong and burst it also
// applies the half-open rule: if at least one probe has been counted
// lost since the last valid inbound frame, and no inbound frame has arrived
// for DEAD_AFTER, the connection is presumed half-open; dead is closed and
// the mode loop returns resReconnect (close 1000, then normal backoff). Idle
// is unchanged: a lost rung probe already forces a reconnect.
func (c *conn) sweeper(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(c.p.cfg.MessageTimeout / 4)
	defer t.Stop()
	deadAfter := deadAfterFactor * c.p.cfg.MessageTimeout
	checkDead := c.p.cfg.Mode != "idle"
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			c.corr.sweep(now)
			if !checkDead {
				continue
			}
			last := c.lastInbound.Load()
			silent := time.Duration(now.UnixNano() - last)
			if c.corr.lastLossNs() > last && silent >= deadAfter {
				c.p.log.Warn("connection presumed half-open; reconnecting",
					"conn_id", c.connID, "since_last_inbound", silent, "dead_after", deadAfter)
				close(c.dead)
				return
			}
		}
	}
}

// wait sleeps d unless the connection closes or the probe shuts down.
func (c *conn) wait(ctx context.Context, d time.Duration) modeResult {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return resNone
	case <-c.dead:
		return resReconnect
	case <-c.readerDone:
		return resClosed
	case <-ctx.Done():
		return resShutdown
	}
}

// sendProbe builds and queues one probe. The head-sampling decision is made
// before serialisation. The entry is registered before the frame is
// queued so the echo can never arrive first.
func (c *conn) sendProbe(payload Payload, idleBucket string, g *burstGroup, burstFirst bool) (*entry, bool) {
	p := c.p
	c.nextSeq++
	traceID, spanID := p.tr.NewIDs()
	sampled := p.tr.HeadDecision(traceID)
	size := 0
	if payload == PayloadLarge {
		size = p.cfg.LargePayloadBytes
	}
	frame, off, err := protocol.AppendProbe(nil, protocol.ProbeFields{
		TraceID: traceID, SpanID: spanID,
		Traceparent: protocol.FormatTraceparent(traceID, spanID, sampled),
		ConnID:      c.connID, Seq: c.nextSeq, Extra: p.opts.Extra,
	}, p.clientJSON, size)
	if err != nil {
		// Unreachable: LARGE_PAYLOAD_BYTES ≥ 1024 is validated at startup.
		p.log.Error("cannot build probe frame", "error", err.Error())
		return nil, false
	}
	e := &entry{
		seq: c.nextSeq, payload: payload, traceID: traceID, spanID: spanID, sampled: sampled,
		idleBucket: idleBucket, burst: g, burstFirst: burstFirst, done: make(chan Outcome, 1),
	}
	c.corr.register(e)
	if !c.enqueue(writeReq{frame: frame, t1Off: off, seq: e.seq}) {
		return e, false
	}
	return e, true
}
