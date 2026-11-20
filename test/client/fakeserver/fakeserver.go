// The Behaviour knobs, the CA and the idle proxy cover the misbehaviours the
// client integration tests need.

package fakeserver

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
	"github.com/asappinc-opensource/wsmon/test/client/wirecap"
)

// Path is the upgrade path, as on the real server.
const Path = "/__synthetic/ws"

// DefaultServer is the identity the fake reports.
var DefaultServer = protocol.ServerInfo{Name: "fake-server-0", Version: "2.0.0-fake"}

// Behaviour configures the fake. Zero value: a well-behaved server.
type Behaviour struct {
	Server protocol.ServerInfo // default DefaultServer

	// Upgrade misbehaviour.
	Token           string // if set, 401 unless "Authorization: Bearer <Token>"
	HTTPStatus      int    // reply with this status instead of 101
	BadAccept       bool   // 101 with a wrong Sec-WebSocket-Accept (raw hijack)
	OmitSubprotocol bool   // complete the upgrade without wsmon.v1

	// Hello misbehaviour.
	AcceptedOptions func(requested protocol.Options) protocol.Options // override accepted_options
	HelloAckConnID  string                                            // override hello_ack conn_id
	HelloAckDelay   time.Duration
	BeforeHelloAck  func(c *Conn) // runs before hello_ack is sent (e.g. send an error frame)
	NoHelloAck      bool
	// OnHello runs (in its own goroutine) after hello_ack is sent.
	OnHello func(c *Conn)

	// Echo. OnProbe replaces the default echo; it runs on the read loop,
	// so anything slow must spawn a goroutine.
	OnProbe   func(c *Conn, p Probe)
	ClockSkew time.Duration // added to t2 and t3
	// NegativeServerTime makes t3 = t2 − 1 ms.
	NegativeServerTime bool

	// Push. Pushes are sent when the hello requests them, or always with
	// ForcePush. PushSeqs scripts the push_seq values (nil: 1, 2, 3, …);
	// PushInterval overrides the requested interval.
	ForcePush    bool
	PushSeqs     []int64
	PushInterval time.Duration

	// NoRecord keeps memory bounded for soak tests: no wire capture, and
	// no per-frame or per-push record (Frames, Probes, SentPushes stay empty).
	NoRecord bool

	// IdleTimeout closes a connection after this long with no frame in
	// either direction: abruptly (no close frame) if IdleCloseCode is 0,
	// else with that code.
	IdleTimeout   time.Duration
	IdleCloseCode int
}

// Probe is a received probe frame.
type Probe struct {
	protocol.Probe
	Raw []byte
	T2  time.Time // read time
}

// Received is one frame the fake received.
type Received struct {
	At   time.Time
	Type string
	Raw  []byte
}

// Server is the fake. Every field is read-only after New.
type Server struct {
	t   testing.TB
	URL string // ws:// or wss:// URL of Path
	// Addr is host:port of the listener (always 127.0.0.1).
	Addr string

	mu        sync.Mutex
	b         Behaviour
	conns     []*Conn
	active    int
	maxActive int
	snis      []string

	ln net.Listener
	hs *http.Server
}

type ctxKey struct{}

// New starts a ws:// fake on 127.0.0.1. host, if non-empty, replaces
// 127.0.0.1 in URL (the listener is still on 127.0.0.1).
func New(t testing.TB, b Behaviour) *Server { return start(t, b, nil, "") }

// NewTLS starts a wss:// fake whose certificate is issued by ca. host is the
// URL host (e.g. "localhost" or "probe.test").
func NewTLS(t testing.TB, b Behaviour, ca *CA, host string) *Server { return start(t, b, ca, host) }

func start(t testing.TB, b Behaviour, ca *CA, host string) *Server {
	t.Helper()
	if b.Server == (protocol.ServerInfo{}) {
		b.Server = DefaultServer
	}
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{t: t, b: b, Addr: tcp.Addr().String()}
	_, port, _ := net.SplitHostPort(s.Addr)
	scheme := "ws"
	if host == "" {
		host = "127.0.0.1"
	}
	var tcfg *tls.Config
	if ca != nil {
		scheme = "wss"
		tcfg = &tls.Config{
			Certificates: []tls.Certificate{ca.Leaf},
			NextProtos:   []string{"http/1.1"},
			GetConfigForClient: func(h *tls.ClientHelloInfo) (*tls.Config, error) {
				s.mu.Lock()
				s.snis = append(s.snis, h.ServerName)
				s.mu.Unlock()
				return nil, nil
			},
		}
	}
	s.URL = scheme + "://" + net.JoinHostPort(host, port) + Path
	s.ln = &countingListener{Listener: tcp, s: s, tls: tcfg}
	mux := http.NewServeMux()
	mux.HandleFunc(Path, s.serve)
	s.hs = &http.Server{
		Handler:     mux,
		ConnContext: func(ctx context.Context, c net.Conn) context.Context { return context.WithValue(ctx, ctxKey{}, c) },
		ErrorLog:    nil,
	}
	go func() { _ = s.hs.Serve(s.ln) }()
	t.Cleanup(s.Close)
	return s
}

// Close stops the listener and kills every connection.
func (s *Server) Close() {
	_ = s.hs.Close()
	for _, c := range s.Conns() {
		c.Kill()
	}
}

// SetBehaviour replaces the behaviour for connections accepted from now on.
func (s *Server) SetBehaviour(f func(*Behaviour)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(&s.b)
}

// Conns returns every request seen, in arrival order (including rejected
// upgrades).
func (s *Server) Conns() []*Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*Conn(nil), s.conns...)
}

// Upgraded returns only the connections that completed the upgrade.
func (s *Server) Upgraded() []*Conn {
	var out []*Conn
	for _, c := range s.Conns() {
		if c.Upgraded() {
			out = append(out, c)
		}
	}
	return out
}

// MaxActive is the high-water mark of concurrently open TCP connections.
func (s *Server) MaxActive() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxActive
}

// SNIs returns the SNI of every TLS handshake.
func (s *Server) SNIs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.snis...)
}

// countingListener tracks concurrent connections and wraps each one in a
// wire capture (inside TLS, so frames are plaintext).
type countingListener struct {
	net.Listener
	s   *Server
	tls *tls.Config
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if l.tls != nil {
		c = tls.Server(c, l.tls)
	}
	s := l.s
	s.mu.Lock()
	s.active++
	s.maxActive = max(s.maxActive, s.active)
	s.mu.Unlock()
	return wirecap.WrapNotify(c, func() {
		s.mu.Lock()
		s.active--
		s.mu.Unlock()
	}), nil
}

// Conn is one request / connection seen by the fake.
type Conn struct {
	s     *Server
	b     Behaviour
	Index int

	Header       http.Header // request headers (Authorization, …)
	Host         string      // request Host
	Query        url.Values  // request URL query
	Subprotocols string      // offered Sec-WebSocket-Protocol
	Cap          *wirecap.Conn

	ws   *websocket.Conn
	done chan struct{}

	mu          sync.Mutex
	frames      []Received
	hello       *protocol.Hello
	helloAckAt  time.Time
	pushAcks    int
	pushesSent  int
	sentPushes  []protocol.Push
	closeCode   int // close code received from the client; -1 if none
	closeErr    error
	lastTraffic time.Time
	stalled     chan struct{}
	opened      time.Time
	closedAt    time.Time
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	b := s.b
	c := &Conn{
		s: s, b: b, Index: len(s.conns),
		Header: r.Header.Clone(), Host: r.Host, Query: r.URL.Query(), Subprotocols: r.Header.Get("Sec-WebSocket-Protocol"),
		done: make(chan struct{}), closeCode: -1, opened: time.Now(),
	}
	c.Cap, _ = r.Context().Value(ctxKey{}).(*wirecap.Conn)
	if b.NoRecord && c.Cap != nil {
		c.Cap.NoRecord.Store(true)
	}
	s.conns = append(s.conns, c)
	s.mu.Unlock()

	switch {
	case b.Token != "" && r.Header.Get("Authorization") != "Bearer "+b.Token:
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	case b.HTTPStatus != 0:
		http.Error(w, "fake status", b.HTTPStatus)
		return
	case b.BadAccept:
		rc, brw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		_, _ = brw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: bm90LXRoZS1yaWdodC1hY2NlcHQ=\r\nSec-WebSocket-Protocol: wsmon.v1\r\n\r\n")
		_ = brw.Flush()
		_ = rc.Close()
		return
	}

	opts := &websocket.AcceptOptions{
		InsecureSkipVerify: true,
		// Negotiated only if the client offers permessage-deflate.
		CompressionMode: websocket.CompressionNoContextTakeover,
	}
	if !b.OmitSubprotocol {
		opts.Subprotocols = []string{protocol.Subprotocol}
	}
	ws, err := websocket.Accept(w, r, opts)
	if err != nil {
		return
	}
	ws.SetReadLimit(-1)
	s.mu.Lock()
	c.ws = ws
	s.mu.Unlock()
	c.touch()
	if b.IdleTimeout > 0 {
		go c.idleWatch()
	}
	c.loop()
}

func (c *Conn) touch() {
	c.mu.Lock()
	c.lastTraffic = time.Now()
	c.mu.Unlock()
}

func (c *Conn) idleWatch() {
	for {
		c.mu.Lock()
		left := c.b.IdleTimeout - time.Since(c.lastTraffic)
		c.mu.Unlock()
		if left <= 0 {
			if c.b.IdleCloseCode == 0 {
				c.Kill()
			} else {
				c.Close(c.b.IdleCloseCode)
			}
			return
		}
		select {
		case <-c.done:
			return
		case <-time.After(left):
		}
	}
}

func (c *Conn) loop() {
	defer func() {
		c.mu.Lock()
		c.closedAt = time.Now()
		c.mu.Unlock()
		close(c.done)
		_ = c.ws.CloseNow()
		if c.Cap != nil {
			_ = c.Cap.Close()
		}
	}()
	for {
		c.mu.Lock()
		st := c.stalled
		c.mu.Unlock()
		if st != nil {
			<-st
		}
		typ, data, err := c.ws.Read(context.Background())
		t2 := time.Now()
		if err != nil {
			c.mu.Lock()
			c.closeErr = err
			if s := websocket.CloseStatus(err); s != -1 {
				c.closeCode = int(s)
			}
			c.mu.Unlock()
			return
		}
		c.touch()
		var env struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(data, &env)
		if typ != websocket.MessageText {
			env.Type = "<binary>"
		}
		if !c.b.NoRecord {
			c.mu.Lock()
			c.frames = append(c.frames, Received{At: t2, Type: env.Type, Raw: data})
			c.mu.Unlock()
		}

		switch env.Type {
		case protocol.TypeHello:
			c.onHello(data)
		case protocol.TypeProbe:
			var p protocol.Probe
			if err := json.Unmarshal(data, &p); err != nil {
				continue
			}
			pr := Probe{Probe: p, Raw: data, T2: t2}
			if c.b.OnProbe != nil {
				c.b.OnProbe(c, pr)
			} else {
				_ = c.Echo(pr)
			}
		case protocol.TypePushAck:
			c.mu.Lock()
			c.pushAcks++
			c.mu.Unlock()
		}
	}
}

func (c *Conn) onHello(data []byte) {
	var h protocol.Hello
	if err := json.Unmarshal(data, &h); err != nil {
		return
	}
	c.mu.Lock()
	c.hello = &h
	c.mu.Unlock()
	if c.b.BeforeHelloAck != nil {
		c.b.BeforeHelloAck(c)
	}
	if c.b.NoHelloAck {
		return
	}
	if c.b.HelloAckDelay > 0 {
		time.Sleep(c.b.HelloAckDelay)
	}
	acc := h.Options
	if c.b.AcceptedOptions != nil {
		acc = c.b.AcceptedOptions(h.Options)
	}
	id := h.ConnID
	if c.b.HelloAckConnID != "" {
		id = c.b.HelloAckConnID
	}
	c.mu.Lock()
	c.helloAckAt = time.Now()
	c.mu.Unlock()
	_ = c.SendJSON(protocol.HelloAck{V: 1, Type: protocol.TypeHelloAck, ConnID: id, Server: c.b.Server, AcceptedOptions: acc})
	interval := time.Duration(acc.PushIntervalMs) * time.Millisecond
	if c.b.PushInterval > 0 {
		interval = c.b.PushInterval
	}
	if (acc.PushIntervalMs > 0 || c.b.ForcePush) && interval > 0 {
		go c.pusher(h.ConnID, interval)
	}
	if c.b.OnHello != nil {
		go c.b.OnHello(c)
	}
}

func (c *Conn) pusher(connID string, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-c.done:
			return
		case <-t.C:
		}
		seq := int64(i + 1)
		if c.b.PushSeqs != nil {
			if i >= len(c.b.PushSeqs) {
				return
			}
			seq = c.b.PushSeqs[i]
		}
		traceID, spanID := randHex(16), randHex(8)
		push := protocol.Push{
			V: 1, Type: protocol.TypePush, TraceID: traceID, SpanID: spanID,
			Traceparent: protocol.FormatTraceparent(traceID, spanID, true),
			ConnID:      connID, PushSeq: seq, ServerSendEpochNs: time.Now().UnixNano(), Server: c.b.Server,
		}
		_ = c.SendJSON(push)
		c.mu.Lock()
		c.pushesSent++
		if !c.b.NoRecord {
			c.sentPushes = append(c.sentPushes, push)
		}
		c.mu.Unlock()
	}
}

// Send writes one text frame.
func (c *Conn) Send(b []byte) error {
	c.touch()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.ws.Write(ctx, websocket.MessageText, b)
}

// SendJSON marshals v and writes it as one text frame.
func (c *Conn) SendJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Send(b)
}

// MakeEcho builds the echo for p, copying the verbatim fields and applying
// the clock behaviours.
func (c *Conn) MakeEcho(p Probe) protocol.Echo {
	t2 := p.T2.Add(c.b.ClockSkew).UnixNano()
	t3 := time.Now().Add(c.b.ClockSkew).UnixNano()
	if c.b.NegativeServerTime {
		t3 = t2 - int64(time.Millisecond)
	}
	return protocol.Echo{
		V: 1, Type: protocol.TypeEcho, TraceID: p.TraceID, ConnID: p.ConnID, Seq: p.Seq,
		ClientSendEpochNs: p.ClientSendEpochNs, ServerRecvEpochNs: t2, ServerSendEpochNs: t3, Server: c.b.Server,
	}
}

// Echo sends the normal echo for p.
func (c *Conn) Echo(p Probe) error { return c.SendJSON(c.MakeEcho(p)) }

// SendError sends an error frame.
func (c *Conn) SendError(code protocol.ErrorCode, seq int64, msg string) error {
	return c.SendJSON(protocol.Error{V: 1, Type: protocol.TypeError, ConnID: c.ConnID(), Seq: seq, Code: code, Message: msg})
}

// Drain sends error{shutting_down} then closes 1001, as the real server does
// on shutdown (S10).
func (c *Conn) Drain() {
	_ = c.SendError(protocol.ErrShuttingDown, 0, "server shutting down")
	c.Close(protocol.CloseGoingAway)
}

// Close performs a server-initiated close handshake with code.
func (c *Conn) Close(code int) {
	if c.ws != nil {
		go func() { _ = c.ws.Close(websocket.StatusCode(code), "") }()
	}
}

// Kill drops the TCP connection with no close frame (the client sees 1006).
func (c *Conn) Kill() {
	if c.Cap != nil {
		_ = c.Cap.Close()
	}
}

// Stall stops reading (and therefore echoing) until Resume; the socket stays
// open.
func (c *Conn) Stall() {
	c.mu.Lock()
	if c.stalled == nil {
		c.stalled = make(chan struct{})
	}
	c.mu.Unlock()
}

// Resume undoes Stall.
func (c *Conn) Resume() {
	c.mu.Lock()
	if c.stalled != nil {
		close(c.stalled)
		c.stalled = nil
	}
	c.mu.Unlock()
}

// WS exposes the server side of the connection, for tests that need to
// misbehave at the frame level (e.g. the TC-IDLE-03 negative control).
func (c *Conn) WS() *websocket.Conn { return c.ws }

// Done is closed when the connection has ended.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Upgraded reports whether the WebSocket upgrade completed.
func (c *Conn) Upgraded() bool {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	return c.ws != nil
}

// Frames returns every frame received, in order.
func (c *Conn) Frames() []Received {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Received(nil), c.frames...)
}

// Probes returns every probe frame received, decoded.
func (c *Conn) Probes() []protocol.Probe {
	var out []protocol.Probe
	for _, f := range c.Frames() {
		if f.Type == protocol.TypeProbe {
			var p protocol.Probe
			if json.Unmarshal(f.Raw, &p) == nil {
				out = append(out, p)
			}
		}
	}
	return out
}

// Hello returns the hello received, or nil.
func (c *Conn) Hello() *protocol.Hello {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hello
}

// ConnID is the conn_id from the hello ("" before it).
func (c *Conn) ConnID() string {
	if h := c.Hello(); h != nil {
		return h.ConnID
	}
	return ""
}

// HelloAckAt is when hello_ack was sent (zero if not).
func (c *Conn) HelloAckAt() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.helloAckAt
}

// PushAcks and PushesSent count push traffic.
func (c *Conn) PushAcks() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pushAcks
}

func (c *Conn) PushesSent() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pushesSent
}

// SentPushes returns the push frames sent so far (for trace parentage).
func (c *Conn) SentPushes() []protocol.Push {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]protocol.Push(nil), c.sentPushes...)
}

// CloseCodeReceived is the code of the client's close frame, or -1.
func (c *Conn) CloseCodeReceived() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeCode
}

// Opened and ClosedAt bracket the connection's life on the server.
func (c *Conn) Opened() time.Time { return c.opened }
func (c *Conn) ClosedAt() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closedAt
}

var errNoConn = errors.New("fakeserver: no such connection")

// WaitConn waits until at least n requests have been seen and returns the
// n-th (1-based).
func (s *Server) WaitConn(n int, timeout time.Duration) (*Conn, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cs := s.Upgraded(); len(cs) >= n {
			return cs[n-1], nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil, errNoConn
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
