package wirecap

import (
	"bytes"
	"errors"
	"net"
	"testing"
)

func TestParseHandBuilt(t *testing.T) {
	key := []byte{1, 2, 3, 4}
	big := bytes.Repeat([]byte("x"), 70000) // needs the 64-bit length
	mid := bytes.Repeat([]byte("y"), 300)   // needs the 16-bit length
	var stream []byte
	stream = append(stream, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n"...)
	stream = append(stream, Encode(false, false, OpText, key, []byte("frag-1"))...)
	stream = append(stream, Encode(true, false, OpPing, key, []byte("p"))...) // control interleaved in a fragmented message
	stream = append(stream, Encode(true, false, OpContinuation, key, []byte("frag-2"))...)
	stream = append(stream, Encode(true, true, OpText, nil, []byte{0xf2, 0x48, 0xcd})...) // compressed (RSV1)
	stream = append(stream, Encode(true, false, OpBinary, nil, mid)...)
	stream = append(stream, Encode(true, false, OpText, key, big)...)
	stream = append(stream, Encode(true, false, OpPong, nil, nil)...)
	stream = append(stream, Encode(true, false, OpClose, key, []byte{0x03, 0xe8})...)
	stream = append(stream, 0x81, 0x05, 'a') // partial trailing frame: ignored

	fs, err := ParseStream(stream)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		fin, rsv1, masked bool
		op                byte
		n                 int
	}{
		{false, false, true, OpText, 6},
		{true, false, true, OpPing, 1},
		{true, false, true, OpContinuation, 6},
		{true, true, false, OpText, 3},
		{true, false, false, OpBinary, 300},
		{true, false, true, OpText, 70000},
		{true, false, false, OpPong, 0},
		{true, false, true, OpClose, 2},
	}
	if len(fs) != len(want) {
		t.Fatalf("got %d frames, want %d: %v", len(fs), len(want), fs)
	}
	for i, w := range want {
		f := fs[i]
		if f.Fin != w.fin || f.RSV1 != w.rsv1 || f.Masked != w.masked || f.Opcode != w.op || len(f.Payload) != w.n || f.Length != uint64(w.n) {
			t.Errorf("frame %d = %v, want %+v", i, f, w)
		}
	}
	if string(fs[0].Payload) != "frag-1" || string(fs[2].Payload) != "frag-2" {
		t.Errorf("unmasking failed: %q %q", fs[0].Payload, fs[2].Payload)
	}
	if !bytes.Equal(fs[5].Payload, big) {
		t.Error("64-bit-length payload corrupted")
	}
	if fs[7].CloseCode() != 1000 {
		t.Errorf("close code %d", fs[7].CloseCode())
	}
	if got := Pings(fs); len(got) != 2 {
		t.Errorf("Pings = %v", got)
	}
}

func TestParseRejectsBadControl(t *testing.T) {
	_, err := ParseStream(Encode(false, false, OpPing, nil, []byte("x")))
	if !errors.Is(err, ErrBadFrame) {
		t.Fatalf("fragmented ping: err = %v", err)
	}
}

func TestConnRecordsBothDirections(t *testing.T) {
	a, b := net.Pipe()
	w := Wrap(a)
	go func() {
		buf := make([]byte, 64)
		n, _ := b.Read(buf)
		_, _ = b.Write(Encode(true, false, OpText, nil, buf[:n]))
		_ = b.Close()
	}()
	if _, err := w.Write(Encode(true, false, OpText, []byte{9, 9, 9, 9}, []byte("hi"))); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	for {
		if _, err := w.Read(buf); err != nil {
			break
		}
	}
	_ = w.Close()
	to, from, err := w.Frames()
	if err != nil {
		t.Fatal(err)
	}
	if len(to) != 1 || string(to[0].Payload) != "hi" || !to[0].Masked {
		t.Errorf("to = %v", to)
	}
	if len(from) != 1 || from[0].Masked {
		t.Errorf("from = %v", from)
	}
	wf, err := w.WrittenFrames()
	if err != nil || len(wf) != 1 || wf[0].At.IsZero() {
		t.Errorf("WrittenFrames = %v, %v", wf, err)
	}
	if w.Closed().IsZero() {
		t.Error("Closed not recorded")
	}
}
