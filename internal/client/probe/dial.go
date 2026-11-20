package probe

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// correlationHeaders are captured from the 101 response as ws.connect span
// attributes. Never metric labels.
var correlationHeaders = []string{"cf-ray", "x-amzn-trace-id", "x-haproxy-request-id", "x-request-id"}

// errAborted marks a connect abandoned because the probe is shutting down.
var errAborted = errors.New("connect aborted by shutdown")

// dialState is the per-attempt state shared by the custom dialers and the
// classifier. Which step failed is recorded here, so classification never
// depends on sniffing error types (dns / tcp / tls).
type dialState struct {
	p   *Probe
	ctx context.Context // the CONNECT_TIMEOUT-bounded context

	mu       sync.Mutex
	failStep Reason
	phases   []PhaseSpan
	tcp      net.Conn // the raw TCP conn
	ws       net.Conn // the conn carrying WebSocket bytes (after WrapConn)
	tlsState *tls.ConnectionState
}

func (d *dialState) phase(ph Phase, start, end time.Time) {
	d.mu.Lock()
	d.phases = append(d.phases, PhaseSpan{Phase: ph, Start: start, End: end})
	d.mu.Unlock()
}

func (d *dialState) fail(r Reason) {
	d.mu.Lock()
	if d.failStep == "" {
		d.failStep = r
	}
	d.mu.Unlock()
}

// closeRaw synchronously closes whatever this attempt dialled, so a failed
// attempt never leaves a socket open while the next one dials (TC-INT-14).
func (d *dialState) closeRaw() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ws != nil {
		_ = d.ws.Close()
	}
	if d.tcp != nil {
		_ = d.tcp.Close()
	}
}

// dialTCP resolves (unless pinned or an IP literal) and connects. The dns and
// tcp phases come from httptrace hooks, which the net package fires for any
// net.Dialer whose context carries a ClientTrace.
func (d *dialState) dialTCP(addr string) (net.Conn, error) {
	cfg := d.p.cfg
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	target := addr
	if cfg.ResolveOverride != "" && strings.EqualFold(host, cfg.ResolveHost) {
		// curl --resolve semantics: dial the pinned IP, no DNS lookup.
		target = net.JoinHostPort(cfg.ResolveIP.String(), port)
	}

	var (
		tmu                sync.Mutex
		dnsStart, dnsEnd   time.Time
		dnsRan             bool
		dnsErr             error
		dnsAddrs           int
		connStart, connEnd time.Time
	)
	trace := &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) {
			tmu.Lock()
			dnsStart = time.Now()
			tmu.Unlock()
		},
		DNSDone: func(i httptrace.DNSDoneInfo) {
			tmu.Lock()
			dnsEnd, dnsRan, dnsErr, dnsAddrs = time.Now(), true, i.Err, len(i.Addrs)
			tmu.Unlock()
		},
		// Happy eyeballs may try several addresses concurrently: the tcp
		// phase runs from the first attempt's start to the successful one's
		// end.
		ConnectStart: func(string, string) {
			tmu.Lock()
			if connStart.IsZero() {
				connStart = time.Now()
			}
			tmu.Unlock()
		},
		ConnectDone: func(_, _ string, err error) {
			if err == nil {
				tmu.Lock()
				connEnd = time.Now()
				tmu.Unlock()
			}
		},
	}
	// TCP keepalive is disabled: Go enables it by default (15 s), and an L4
	// hop (e.g. a TCP-mode load balancer) may count keepalive segments as
	// traffic, which would keep an idle-mode connection alive the same way
	// a WebSocket ping would.
	nd := net.Dialer{KeepAlive: -1}
	c, err := nd.DialContext(httptrace.WithClientTrace(d.ctx, trace), "tcp", target)

	tmu.Lock()
	defer tmu.Unlock()
	if dnsRan && dnsErr == nil && dnsAddrs > 0 {
		d.phase(PhaseDNS, dnsStart, dnsEnd)
	}
	if err != nil {
		if dnsRan && (dnsErr != nil || dnsAddrs == 0) {
			d.fail(ReasonDNS)
		} else {
			d.fail(ReasonTCP)
		}
		return nil, err
	}
	if !connStart.IsZero() && !connEnd.IsZero() {
		d.phase(PhaseTCP, connStart, connEnd)
	}
	d.mu.Lock()
	d.tcp = c
	d.mu.Unlock()
	return c, nil
}

func (d *dialState) wrap(c net.Conn) net.Conn {
	if d.p.opts.WrapConn != nil {
		c = d.p.opts.WrapConn(c)
	}
	d.mu.Lock()
	d.ws = c
	d.mu.Unlock()
	return c
}

// dialPlain is the Transport's DialContext, used for ws://. There is no tls
// phase at all.
func (d *dialState) dialPlain(_ context.Context, _, addr string) (net.Conn, error) {
	c, err := d.dialTCP(addr)
	if err != nil {
		return nil, err
	}
	return d.wrap(c), nil
}

// dialTLS is the Transport's DialTLSContext, used for wss://. The handshake
// is done and timed here because httptrace's TLSHandshake hooks do not fire
// for a custom DialTLSContext.
func (d *dialState) dialTLS(_ context.Context, _, addr string) (net.Conn, error) {
	c, err := d.dialTCP(addr)
	if err != nil {
		return nil, err
	}
	cfg := d.p.cfg
	host, _, _ := net.SplitHostPort(addr)
	sni := host
	if cfg.SNIHost != "" {
		sni = cfg.SNIHost
	}
	tc := tls.Client(c, &tls.Config{
		ServerName:         sni,
		InsecureSkipVerify: cfg.InsecureSkipVerify, // validated: wss:// only
		RootCAs:            d.p.opts.RootCAs,
		NextProtos:         []string{"http/1.1"},
		MinVersion:         tls.VersionTLS12,
	})
	start := time.Now()
	if err := tc.HandshakeContext(d.ctx); err != nil {
		d.fail(ReasonTLS)
		_ = c.Close()
		return nil, err
	}
	d.phase(PhaseTLS, start, time.Now())
	st := tc.ConnectionState()
	d.mu.Lock()
	d.tlsState = &st
	d.mu.Unlock()
	return d.wrap(tc), nil
}

// connect runs the connect sequence, bounded as a whole by CONNECT_TIMEOUT:
// DNS → TCP → TLS → upgrade with wsmon.v1 → hello → hello_ack → verify
// accepted_options. On success the connection's reader and writer are
// running. On failure everything dialled is closed before returning, and the
// reason is set. A nil conn with ctx cancelled means shutdown.
func (p *Probe) connect(ctx context.Context) (*conn, Reason, error) {
	info := ConnectInfo{Start: time.Now()}
	// The ws.connect span's IDs exist before the dial, so the upgrade
	// request can carry them and an edge or server that logs or traces
	// traceparent joins this attempt's trace.
	info.TraceID, info.SpanID = p.tr.NewIDs()
	info.Sampled = p.tr.HeadDecision(info.TraceID)
	cctx, cancel := context.WithTimeout(ctx, p.cfg.ConnectTimeout)
	defer cancel()
	d := &dialState{p: p, ctx: cctx}

	finish := func(c *conn, r Reason, err error) (*conn, Reason, error) {
		d.mu.Lock()
		info.Phases = append([]PhaseSpan(nil), d.phases...)
		d.mu.Unlock()
		for _, ph := range info.Phases {
			p.rec.ConnectPhase(ph.Phase, ph.End.Sub(ph.Start))
		}
		info.End, info.Failure = time.Now(), r
		if !errors.Is(err, errAborted) {
			p.tr.Connect(info)
		}
		return c, r, err
	}

	tr := &http.Transport{
		Proxy:             nil, // a probe measures the configured path, never an env proxy
		DialContext:       d.dialPlain,
		DialTLSContext:    d.dialTLS,
		DisableKeepAlives: true,
		ForceAttemptHTTP2: false,
		TLSNextProto:      map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	defer tr.CloseIdleConnections()

	var (
		umu                 sync.Mutex
		wroteReq, firstByte time.Time
	)
	rctx := httptrace.WithClientTrace(cctx, &httptrace.ClientTrace{
		WroteRequest: func(httptrace.WroteRequestInfo) {
			umu.Lock()
			wroteReq = time.Now()
			umu.Unlock()
		},
		GotFirstResponseByte: func() {
			umu.Lock()
			firstByte = time.Now()
			umu.Unlock()
		},
	})
	// WSMON_HEADERS first; config validation keeps them off every header
	// the handshake sets, so the library's own Set calls cannot clash.
	hdr := p.cfg.Headers.Clone()
	if hdr == nil {
		hdr = http.Header{}
	}
	if p.cfg.AuthToken != "" {
		hdr.Set("Authorization", "Bearer "+p.cfg.AuthToken)
	}
	if p.cfg.UpgradeTraceparent {
		hdr.Set("Traceparent", protocol.FormatTraceparent(info.TraceID, info.SpanID, info.Sampled))
	}
	compression := websocket.CompressionDisabled
	if p.cfg.EnablePermessageDeflate {
		compression = websocket.CompressionNoContextTakeover
	}
	ws, resp, err := websocket.Dial(rctx, p.cfg.DialURL(), &websocket.DialOptions{
		HTTPClient:      &http.Client{Transport: tr},
		HTTPHeader:      hdr,
		Host:            p.cfg.HostHeader, // sets req.Host (TC-INT-15, TC-INT-19)
		Subprotocols:    []string{protocol.Subprotocol},
		CompressionMode: compression,
		OnPingReceived:  p.pingReceived,
	})
	umu.Lock()
	if !wroteReq.IsZero() && !firstByte.IsZero() {
		d.phase(PhaseUpgrade, wroteReq, firstByte)
	}
	umu.Unlock()
	if resp != nil {
		info.Headers = pickHeaders(resp.Header)
	}
	d.mu.Lock()
	if d.tcp != nil {
		if a, ok := d.tcp.RemoteAddr().(*net.TCPAddr); ok {
			info.PeerIP = a.IP.String()
		}
		if a, ok := d.tcp.LocalAddr().(*net.TCPAddr); ok {
			info.LocalPort = a.Port
		}
	}
	if d.tlsState != nil {
		info.TLSVersion = tls.VersionName(d.tlsState.Version)
		info.TLSCipher = tls.CipherSuiteName(d.tlsState.CipherSuite)
	}
	d.mu.Unlock()

	if err != nil {
		d.closeRaw()
		r := p.classifyDialError(ctx, cctx, d, resp, err)
		if r == "" {
			return finish(nil, "", errAborted)
		}
		return finish(nil, r, err)
	}

	// Upgraded. From here every exit closes with a close frame, never a
	// bare socket drop, because that would manufacture a 1006.
	ws.SetReadLimit(readLimit)
	d.mu.Lock()
	raw := d.ws
	d.mu.Unlock()
	c := newConn(p, ws, raw)
	c.connID = p.newConnID()
	c.corr = newCorrelator(p, c.connID)

	if sp := ws.Subprotocol(); sp != protocol.Subprotocol {
		// TC-WIRE-10. The upgrade completed, so close with 1002 rather
		// than CloseNow.
		c.start()
		c.endEarly(protocol.CloseProtocolError, "subprotocol not negotiated", c.upgradedAt)
		return finish(nil, ReasonSubprotocol, fmt.Errorf("server negotiated subprotocol %q, want %q", sp, protocol.Subprotocol))
	}

	// Hello, immediately after the 101.
	info.ConnID = c.connID
	opts := protocol.Options{ServerKeepalive: false}
	if p.cfg.Mode != "idle" {
		opts.PushIntervalMs = p.cfg.PushIntervalMs
	}
	// In idle mode options are exactly {false, 0}, always.
	c.pushRequested = opts.PushIntervalMs > 0
	c.start()
	if !c.enqueue(writeReq{frame: protocol.AppendHello(nil, c.connID, p.clientJSON, opts), t1Off: -1, helloWrite: true}) {
		code, init := c.endEarly(protocol.CloseNormal, "", c.upgradedAt)
		return finish(nil, ReasonOther, fmt.Errorf("could not write hello (closed %d by %s)", code, init))
	}

	select {
	case r := <-c.helloCh:
		if r.ack != nil {
			if hs := c.helloWriteTime(); !hs.IsZero() {
				d.phase(PhaseHello, hs, r.at)
			}
		}
		switch {
		case r.err != nil:
			c.endEarly(protocol.CloseNormal, "", c.upgradedAt)
			p.log.Warn("unexpected frame before hello_ack", "conn_id", c.connID, "error", r.err.Error())
			return finish(nil, ReasonOther, r.err)
		case r.ack.ConnID != c.connID:
			c.endEarly(protocol.CloseNormal, "", c.upgradedAt)
			p.log.Warn("hello_ack conn_id does not match hello", "conn_id", c.connID, "ack_conn_id", r.ack.ConnID)
			return finish(nil, ReasonOther, fmt.Errorf("hello_ack conn_id %q, want %q", r.ack.ConnID, c.connID))
		case r.ack.AcceptedOptions != opts:
			p.log.Warn("server did not accept the requested options",
				"conn_id", c.connID,
				"requested_server_keepalive", opts.ServerKeepalive, "requested_push_interval_ms", opts.PushIntervalMs,
				"accepted_server_keepalive", r.ack.AcceptedOptions.ServerKeepalive, "accepted_push_interval_ms", r.ack.AcceptedOptions.PushIntervalMs)
			c.endEarly(protocol.CloseProtocolError, "options rejected", c.upgradedAt)
			return finish(nil, ReasonOptionsRejected, errors.New("accepted_options differ from requested options"))
		}
		c.server = r.ack.Server
		c.corr.mu.Lock()
		c.corr.server = r.ack.Server
		c.corr.mu.Unlock()
		info.Server = r.ack.Server
		return finish(c, "", nil)
	case <-c.readerDone:
		code, _ := c.endEarly(0, "", c.upgradedAt)
		return finish(nil, ReasonOther, fmt.Errorf("connection closed by remote before hello_ack (close code %d)", code))
	case <-cctx.Done():
		c.endEarly(protocol.CloseNormal, "", c.upgradedAt)
		if ctx.Err() != nil {
			return finish(nil, "", errAborted)
		}
		return finish(nil, ReasonTimeout, errors.New("hello_ack not received within connect timeout"))
	}
}

// classifyDialError maps a failed upgrade to a connect failure Reason. It returns ""
// when the probe itself is shutting down.
func (p *Probe) classifyDialError(ctx, cctx context.Context, d *dialState, resp *http.Response, err error) Reason {
	d.mu.Lock()
	step := d.failStep
	d.mu.Unlock()
	switch {
	case ctx.Err() != nil:
		return ""
	case errors.Is(cctx.Err(), context.DeadlineExceeded):
		return ReasonTimeout
	case step != "":
		return step
	case resp != nil && resp.StatusCode != http.StatusSwitchingProtocols:
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return ReasonAuth
		}
		return ReasonHTTPStatus
	case resp != nil:
		// 101 but the library rejected it. A subprotocol other than the one
		// offered is a subprotocol failure; anything else (bad
		// Sec-WebSocket-Accept, missing Upgrade headers) is
		// upgrade_rejected. The library has already dropped the socket, so
		// no close frame is possible here.
		if sp := resp.Header.Get("Sec-WebSocket-Protocol"); sp != "" && sp != protocol.Subprotocol {
			return ReasonSubprotocol
		}
		return ReasonUpgradeRejected
	default:
		p.log.Warn("unclassified connect failure", "error", err.Error())
		return ReasonOther
	}
}

func pickHeaders(h http.Header) map[string]string {
	var out map[string]string
	for _, k := range correlationHeaders {
		if v := h.Get(k); v != "" {
			if out == nil {
				out = make(map[string]string)
			}
			out[k] = v
		}
	}
	return out
}

// pingReceived is called if the peer sends a ping. The server must never do so
// (S7), so this is logged loudly. The library still replies with a pong, as
// RFC 6455 §5.5.2 requires; the probe itself never initiates a ping.
func (p *Probe) pingReceived(_ context.Context, payload []byte) bool {
	p.log.Warn("received WebSocket ping from peer; idle measurements on this connection are invalid", "payload_len", len(payload))
	return true
}
