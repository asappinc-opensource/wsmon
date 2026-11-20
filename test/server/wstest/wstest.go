// Package wstest is a minimal fake client for exercising `wsmon server` in its
// own test suite. It implements only what server test cases need. It is not,
// and must not grow into, a probe implementation.
package wstest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// Capture records the raw bytes crossing a client connection in both
// directions, so tests can assert on frames actually on the wire rather than
// on library configuration.
type Capture struct {
	mu         sync.Mutex
	toServer   bytes.Buffer
	fromServer bytes.Buffer
}

type capConn struct {
	net.Conn
	c *Capture
}

func (cc capConn) Read(p []byte) (int, error) {
	n, err := cc.Conn.Read(p)
	cc.c.mu.Lock()
	cc.c.fromServer.Write(p[:n])
	cc.c.mu.Unlock()
	return n, err
}

func (cc capConn) Write(p []byte) (int, error) {
	n, err := cc.Conn.Write(p)
	cc.c.mu.Lock()
	cc.c.toServer.Write(p[:n])
	cc.c.mu.Unlock()
	return n, err
}

// Frame is one WebSocket frame as seen on the wire (payload unmasked).
type Frame struct {
	Fin     bool
	Opcode  byte
	Payload []byte
}

// WebSocket opcodes.
const (
	OpText  = 0x1
	OpClose = 0x8
	OpPing  = 0x9
	OpPong  = 0xA
)

// CloseCode returns the status code of a close frame, or -1.
func (f Frame) CloseCode() int {
	if f.Opcode != OpClose || len(f.Payload) < 2 {
		return -1
	}
	return int(binary.BigEndian.Uint16(f.Payload))
}

// FromServer parses every complete frame the server has sent so far.
func (c *Capture) FromServer() ([]Frame, error) {
	c.mu.Lock()
	b := bytes.Clone(c.fromServer.Bytes())
	c.mu.Unlock()
	return parseStream(b)
}

// ToServer parses every complete frame the client has sent so far.
func (c *Capture) ToServer() ([]Frame, error) {
	c.mu.Lock()
	b := bytes.Clone(c.toServer.Bytes())
	c.mu.Unlock()
	return parseStream(b)
}

// parseStream skips the HTTP handshake and decodes frames (RFC 6455 §5.2).
func parseStream(b []byte) ([]Frame, error) {
	i := bytes.Index(b, []byte("\r\n\r\n"))
	if i < 0 {
		return nil, errors.New("no HTTP handshake in capture")
	}
	b = b[i+4:]
	var frames []Frame
	for len(b) >= 2 {
		f := Frame{Fin: b[0]&0x80 != 0, Opcode: b[0] & 0x0f}
		masked := b[1]&0x80 != 0
		n := uint64(b[1] & 0x7f)
		hdr := 2
		switch n {
		case 126:
			if len(b) < 4 {
				return frames, nil
			}
			n, hdr = uint64(binary.BigEndian.Uint16(b[2:])), 4
		case 127:
			if len(b) < 10 {
				return frames, nil
			}
			n, hdr = binary.BigEndian.Uint64(b[2:]), 10
		}
		var key []byte
		if masked {
			if len(b) < hdr+4 {
				return frames, nil
			}
			key = b[hdr : hdr+4]
			hdr += 4
		}
		if uint64(len(b)-hdr) < n {
			return frames, nil // partial frame at the end of the capture
		}
		p := bytes.Clone(b[hdr : hdr+int(n)])
		for j := range p {
			if masked {
				p[j] ^= key[j%4]
			}
		}
		f.Payload = p
		frames = append(frames, f)
		b = b[hdr+int(n):]
	}
	return frames, nil
}

// DialOptions tweak the handshake. The zero value requests wsmon.v1 with no
// auth and no compression.
type DialOptions struct {
	Token        string
	Subprotocols []string
	Compression  bool
	HTTPClient   *http.Client
	// NoCapture skips recording the byte stream, for long or many-connection
	// runs where the capture would dominate memory.
	NoCapture bool
}

// Client is one test connection.
type Client struct {
	WS      *websocket.Conn
	Resp    *http.Response
	Capture *Capture
	ConnID  string
	seq     atomic.Int64
}

// Dial connects to url (ws:// or wss://), capturing the raw byte stream.
func Dial(ctx context.Context, url string, o DialOptions) (*Client, error) {
	capture := &Capture{}
	hc := o.HTTPClient
	if hc == nil {
		hc = &http.Client{}
	}
	base, _ := hc.Transport.(*http.Transport)
	if base == nil {
		base = http.DefaultTransport.(*http.Transport)
	}
	tr := base.Clone()
	d := &net.Dialer{}
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := d.DialContext(ctx, network, addr)
		if err != nil || o.NoCapture {
			return c, err
		}
		return capConn{Conn: c, c: capture}, nil
	}
	// Over wss:// the capture sits beneath TLS and records ciphertext; wss
	// tests only assert on the handshake outcome.
	hc2 := *hc
	hc2.Transport = tr

	subs := o.Subprotocols
	if subs == nil {
		subs = []string{protocol.Subprotocol}
	}
	hdr := http.Header{}
	if o.Token != "" {
		hdr.Set("Authorization", "Bearer "+o.Token)
	}
	mode := websocket.CompressionDisabled
	if o.Compression {
		mode = websocket.CompressionNoContextTakeover
	}
	ws, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPClient:      &hc2,
		HTTPHeader:      hdr,
		Subprotocols:    subs,
		CompressionMode: mode,
	})
	if err != nil {
		return &Client{Resp: resp, Capture: capture}, err
	}
	ws.SetReadLimit(-1)
	return &Client{WS: ws, Resp: resp, Capture: capture, ConnID: NewID(13)}, nil
}

// SendRaw writes one text frame.
func (c *Client) SendRaw(ctx context.Context, b []byte) error {
	return c.WS.Write(ctx, websocket.MessageText, b)
}

// Send JSON-encodes v and writes it as one text frame.
func (c *Client) Send(ctx context.Context, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.SendRaw(ctx, b)
}

// Recv reads one frame. Note the library closes the connection if ctx
// expires, so only use a deadline where the test ends on failure anyway.
func (c *Client) Recv(ctx context.Context) ([]byte, error) {
	_, b, err := c.WS.Read(ctx)
	return b, err
}

// RecvType reads one frame and returns it with its "type" field.
func (c *Client) RecvType(ctx context.Context) (string, []byte, error) {
	b, err := c.Recv(ctx)
	if err != nil {
		return "", nil, err
	}
	var env struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return "", b, fmt.Errorf("frame is not JSON: %w: %s", err, b)
	}
	return env.Type, b, nil
}

// Hello sends hello with opts and returns the hello_ack.
func (c *Client) Hello(ctx context.Context, client protocol.ClientInfo, opts protocol.Options) (protocol.HelloAck, error) {
	var ack protocol.HelloAck
	err := c.Send(ctx, protocol.Hello{V: 1, Type: protocol.TypeHello, ConnID: c.ConnID, Client: client, Options: opts})
	if err != nil {
		return ack, err
	}
	typ, b, err := c.RecvType(ctx)
	if err != nil {
		return ack, err
	}
	if typ != protocol.TypeHelloAck {
		return ack, fmt.Errorf("expected hello_ack, got %s", b)
	}
	return ack, json.Unmarshal(b, &ack)
}

// NextProbe builds the connection's next probe frame.
func (c *Client) NextProbe(client protocol.ClientInfo) protocol.Probe {
	traceID, spanID := NewID(16), NewID(8)
	return protocol.Probe{
		V:                 1,
		Type:              protocol.TypeProbe,
		TraceID:           traceID,
		SpanID:            spanID,
		Traceparent:       "00-" + traceID + "-" + spanID + "-01",
		ConnID:            c.ConnID,
		Seq:               c.seq.Add(1),
		ClientSendEpochNs: time.Now().UnixNano(),
		Client:            client,
	}
}

// RoundTrip sends p and reads the reply, which must be an echo.
func (c *Client) RoundTrip(ctx context.Context, p protocol.Probe) (protocol.Echo, []byte, error) {
	var e protocol.Echo
	if err := c.Send(ctx, p); err != nil {
		return e, nil, err
	}
	typ, b, err := c.RecvType(ctx)
	if err != nil {
		return e, nil, err
	}
	if typ != protocol.TypeEcho {
		return e, b, fmt.Errorf("expected echo, got %s", b)
	}
	return e, b, json.Unmarshal(b, &e)
}

// Close performs a normal client close.
func (c *Client) Close() {
	if c.WS != nil {
		_ = c.WS.Close(websocket.StatusNormalClosure, "")
	}
}

// NewID returns n random bytes as lowercase hex.
func NewID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Client info used by tests. The server must ignore all of it. The topology
// a deployment attaches travels in labels.
var (
	ClientSkyEdge   = protocol.ClientInfo{Mode: "pingpong", Version: "0.0.0-test", Labels: map[string]string{"target": "relay-prod-east", "entrypoint": "skyedge", "vantage": "nimbus-east1", "env": "test"}}
	ClientPodDirect = protocol.ClientInfo{Mode: "pingpong", Version: "0.0.0-test", Labels: map[string]string{"target": "relay-prod-east", "entrypoint": "pod_direct", "vantage": "nimbus-east1", "env": "test"}}
	ClientIdle      = protocol.ClientInfo{Mode: "idle", Version: "0.0.0-test", Labels: map[string]string{"target": "relay-prod-east", "entrypoint": "skyedge", "vantage": "nimbus-east1", "env": "test"}}
)
