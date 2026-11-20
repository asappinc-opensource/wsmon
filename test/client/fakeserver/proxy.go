package fakeserver

import (
	"net"
	"sync"
	"testing"
	"time"
)

// IdleProxy is a TCP proxy that drops a connection (both sides, no close
// frame) after Idle with no bytes in either direction, like a load balancer
// idle timeout (TC-IDLE-02).
type IdleProxy struct {
	Addr string // 127.0.0.1:port to dial instead of the target

	ln     net.Listener
	target string
	idle   time.Duration

	mu    sync.Mutex
	drops int
}

// NewIdleProxy proxies to target (host:port).
func NewIdleProxy(t testing.TB, target string, idle time.Duration) *IdleProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &IdleProxy{Addr: ln.Addr().String(), ln: ln, target: target, idle: idle}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return p
}

// Drops counts idle-timeout drops.
func (p *IdleProxy) Drops() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.drops
}

func (p *IdleProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *IdleProxy) handle(client net.Conn) {
	server, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	var (
		mu   sync.Mutex
		last = time.Now()
		done = make(chan struct{})
		once sync.Once
	)
	stop := func() {
		once.Do(func() {
			close(done)
			_ = client.Close()
			_ = server.Close()
		})
	}
	pipe := func(dst, src net.Conn) {
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				mu.Lock()
				last = time.Now()
				mu.Unlock()
				if _, werr := dst.Write(buf[:n]); werr != nil {
					stop()
					return
				}
			}
			if err != nil {
				stop()
				return
			}
		}
	}
	go pipe(server, client)
	go pipe(client, server)
	for {
		mu.Lock()
		left := p.idle - time.Since(last)
		mu.Unlock()
		if left <= 0 {
			p.mu.Lock()
			p.drops++
			p.mu.Unlock()
			stop()
			return
		}
		select {
		case <-done:
			return
		case <-time.After(left):
		}
	}
}
