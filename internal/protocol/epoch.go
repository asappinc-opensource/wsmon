package protocol

import (
	"bytes"
	"strconv"
)

// EpochSlotWidth is the number of bytes reserved for a patched timestamp. It
// fits any int64, including the sign. The client uses the slot for t1
// (client_send_epoch_ns), the server for t3 (server_send_epoch_ns).
const EpochSlotWidth = 20

var epochPlaceholder = bytes.Repeat([]byte{' '}, EpochSlotWidth)

// PatchEpochNs writes ns into the slot at off reserved by an Append function.
// The value is left-aligned; unused slot bytes are left as spaces, which JSON
// permits between tokens. This lets either side read the clock after
// serialisation, immediately before the write.
func PatchEpochNs(frame []byte, off int, ns int64) {
	slot := frame[off : off+EpochSlotWidth]
	n := len(strconv.AppendInt(slot[:0], ns, 10))
	for i := n; i < EpochSlotWidth; i++ {
		slot[i] = ' '
	}
}

// StampT1 is the single place the client captures t1. It reads the clock
// only now, after the frame is fully serialised, patches the value into the
// slot and returns it. The caller writes frame immediately afterwards and
// records the returned t1 for RTT. now is time.Now().UnixNano in
// production; tests inject a recorder (TC-TIME-07).
func StampT1(frame []byte, t1Off int, now func() int64) int64 {
	t1 := now()
	PatchEpochNs(frame, t1Off, t1)
	return t1
}
