// Package wirecap records the raw bytes crossing a net.Conn in both
// directions and parses them as RFC 6455 frames, so tests assert on frames
// actually on the wire rather than on library configuration (TC-IDLE-03).
// Test code only.
package wirecap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// WebSocket opcodes (RFC 6455 §5.2).
const (
	OpContinuation = 0x0
	OpText         = 0x1
	OpBinary       = 0x2
	OpClose        = 0x8
	OpPing         = 0x9
	OpPong         = 0xA
)

// Frame is one WebSocket frame as it appeared on the wire. Payload is
// unmasked; for RSV1 (compressed) frames it is still compressed.
type Frame struct {
	Fin     bool
	RSV1    bool
	Opcode  byte
	Masked  bool
	Length  uint64
	Payload []byte
}

// IsControl reports whether f is a close, ping or pong frame.
func (f Frame) IsControl() bool { return f.Opcode >= 0x8 }

// CloseCode returns the status code of a close frame, or -1.
func (f Frame) CloseCode() int {
	if f.Opcode != OpClose || len(f.Payload) < 2 {
		return -1
	}
	return int(binary.BigEndian.Uint16(f.Payload))
}

func (f Frame) String() string {
	p := f.Payload
	if len(p) > 80 {
		p = p[:80]
	}
	return fmt.Sprintf("{fin=%v rsv1=%v op=0x%x masked=%v len=%d payload=%q}", f.Fin, f.RSV1, f.Opcode, f.Masked, f.Length, p)
}

// Chunk is the bytes of one Read or Write call and when it happened.
type Chunk struct {
	At    time.Time
	Bytes []byte
}

// Conn is a net.Conn that records every byte. "To peer" is what this side
// wrote; "from peer" is what it read.
type Conn struct {
	net.Conn

	// NoRecord stops recording from now on (soak tests: bounded memory).
	NoRecord atomic.Bool

	mu       sync.Mutex
	toPeer   []Chunk
	fromPeer []Chunk
	closed   time.Time
	opened   time.Time
	onClose  func()
	closeMu  sync.Once
}

// Wrap starts recording c.
func Wrap(c net.Conn) *Conn { return &Conn{Conn: c, opened: time.Now()} }

// WrapNotify is Wrap with a callback run once, on the first Close.
func WrapNotify(c net.Conn, onClose func()) *Conn {
	w := Wrap(c)
	w.onClose = onClose
	return w
}

func (c *Conn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && !c.NoRecord.Load() {
		c.mu.Lock()
		c.fromPeer = append(c.fromPeer, Chunk{At: time.Now(), Bytes: bytes.Clone(p[:n])})
		c.mu.Unlock()
	}
	return n, err
}

func (c *Conn) Write(p []byte) (int, error) {
	at := time.Now()
	n, err := c.Conn.Write(p)
	if n > 0 && !c.NoRecord.Load() {
		c.mu.Lock()
		c.toPeer = append(c.toPeer, Chunk{At: at, Bytes: bytes.Clone(p[:n])})
		c.mu.Unlock()
	}
	return n, err
}

func (c *Conn) Close() error {
	err := c.Conn.Close()
	c.closeMu.Do(func() {
		c.mu.Lock()
		c.closed = time.Now()
		c.mu.Unlock()
		if c.onClose != nil {
			c.onClose()
		}
	})
	return err
}

// Opened and Closed return when the wrapper was created and first closed
// (zero if still open).
func (c *Conn) Opened() time.Time { return c.opened }
func (c *Conn) Closed() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// Writes returns every Write chunk (this side → peer).
func (c *Conn) Writes() []Chunk {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Chunk(nil), c.toPeer...)
}

// Reads returns every Read chunk (peer → this side).
func (c *Conn) Reads() []Chunk {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Chunk(nil), c.fromPeer...)
}

func join(cs []Chunk) []byte {
	var b []byte
	for _, c := range cs {
		b = append(b, c.Bytes...)
	}
	return b
}

// Frames parses both directions. A leading HTTP request or response (the
// upgrade) is skipped. A trailing partial frame is ignored.
func (c *Conn) Frames() (toPeer, fromPeer []Frame, err error) {
	c.mu.Lock()
	to, from := join(c.toPeer), join(c.fromPeer)
	c.mu.Unlock()
	if toPeer, err = ParseStream(to); err != nil {
		return nil, nil, fmt.Errorf("to peer: %w", err)
	}
	if fromPeer, err = ParseStream(from); err != nil {
		return nil, nil, fmt.Errorf("from peer: %w", err)
	}
	return toPeer, fromPeer, nil
}

// FrameAt is a frame plus the time of the Write that completed it.
type FrameAt struct {
	Frame
	At time.Time
}

// WrittenFrames parses this side's writes and timestamps each frame with the
// Write call in which its last byte went out.
func (c *Conn) WrittenFrames() ([]FrameAt, error) { return framesAt(c.Writes()) }

// ReadFrames parses the peer's bytes and timestamps each frame with the Read
// call that delivered its last byte.
func (c *Conn) ReadFrames() ([]FrameAt, error) { return framesAt(c.Reads()) }

// Head returns the leading HTTP message head of one direction (the upgrade
// request for Writes, the response for Reads), or nil.
func Head(chunks []Chunk) []byte {
	all := join(chunks)
	if n := httpPrefix(all); n > 0 {
		return all[:n]
	}
	return nil
}

func framesAt(chunks []Chunk) ([]FrameAt, error) {
	all := join(chunks)
	skip := httpPrefix(all)
	frames, ends, err := parse(all[skip:])
	if err != nil {
		return nil, err
	}
	out := make([]FrameAt, len(frames))
	for i, f := range frames {
		end := skip + ends[i]
		off := 0
		for _, ch := range chunks {
			off += len(ch.Bytes)
			if off >= end {
				out[i] = FrameAt{Frame: f, At: ch.At}
				break
			}
		}
	}
	return out, nil
}

// httpPrefix returns the length of a leading HTTP message head, or 0.
func httpPrefix(b []byte) int {
	s := string(b[:min(len(b), 8)])
	if !strings.HasPrefix(s, "GET ") && !strings.HasPrefix(s, "HTTP/") {
		return 0
	}
	i := bytes.Index(b, []byte("\r\n\r\n"))
	if i < 0 {
		return len(b)
	}
	return i + 4
}

// ParseStream parses a byte stream, skipping a leading HTTP head.
func ParseStream(b []byte) ([]Frame, error) {
	frames, _, err := parse(b[httpPrefix(b):])
	return frames, err
}

// ErrBadFrame reports a frame header that violates RFC 6455.
var ErrBadFrame = errors.New("wirecap: malformed frame")

// parse decodes frames (RFC 6455 §5.2) and returns each frame's end offset.
func parse(b []byte) (frames []Frame, ends []int, err error) {
	off := 0
	for len(b)-off >= 2 {
		h := b[off:]
		f := Frame{Fin: h[0]&0x80 != 0, RSV1: h[0]&0x40 != 0, Opcode: h[0] & 0x0f, Masked: h[1]&0x80 != 0}
		n := uint64(h[1] & 0x7f)
		hdr := 2
		switch n {
		case 126:
			if len(h) < 4 {
				return frames, ends, nil
			}
			n, hdr = uint64(binary.BigEndian.Uint16(h[2:])), 4
		case 127:
			if len(h) < 10 {
				return frames, ends, nil
			}
			n, hdr = binary.BigEndian.Uint64(h[2:]), 10
			if n>>63 != 0 {
				return frames, ends, fmt.Errorf("%w: 64-bit length has the high bit set", ErrBadFrame)
			}
		}
		if f.IsControl() && (n > 125 || !f.Fin) {
			return frames, ends, fmt.Errorf("%w: control frame op=0x%x len=%d fin=%v", ErrBadFrame, f.Opcode, n, f.Fin)
		}
		var key []byte
		if f.Masked {
			if len(h) < hdr+4 {
				return frames, ends, nil
			}
			key = h[hdr : hdr+4]
			hdr += 4
		}
		if uint64(len(h)-hdr) < n {
			return frames, ends, nil // partial frame at the end of the capture
		}
		p := bytes.Clone(h[hdr : hdr+int(n)])
		if f.Masked {
			for j := range p {
				p[j] ^= key[j%4]
			}
		}
		f.Length, f.Payload = n, p
		frames = append(frames, f)
		off += hdr + int(n)
		ends = append(ends, off)
	}
	return frames, ends, nil
}

// Encode builds a frame (for parser tests). key is used when non-nil.
func Encode(fin, rsv1 bool, op byte, key []byte, payload []byte) []byte {
	b0 := op
	if fin {
		b0 |= 0x80
	}
	if rsv1 {
		b0 |= 0x40
	}
	out := []byte{b0}
	var m byte
	if key != nil {
		m = 0x80
	}
	n := len(payload)
	switch {
	case n <= 125:
		out = append(out, m|byte(n))
	case n <= 0xffff:
		out = append(out, m|126, byte(n>>8), byte(n))
	default:
		out = append(out, m|127)
		out = binary.BigEndian.AppendUint64(out, uint64(n))
	}
	p := bytes.Clone(payload)
	if key != nil {
		out = append(out, key...)
		for i := range p {
			p[i] ^= key[i%4]
		}
	}
	return append(out, p...)
}

// Pings returns every ping or pong frame in fs.
func Pings(fs []Frame) []Frame {
	var out []Frame
	for _, f := range fs {
		if f.Opcode == OpPing || f.Opcode == OpPong {
			out = append(out, f)
		}
	}
	return out
}
