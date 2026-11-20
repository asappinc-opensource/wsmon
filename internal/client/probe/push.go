package probe

import (
	"time"

	"github.com/asappinc-opensource/wsmon/internal/protocol"
)

// pushState is per connection: the push_seq high-water mark and the last
// arrival time reset with every new connection.
type pushState struct {
	hwm  int64
	last time.Time
	have bool
}

// onPush handles one push frame. It runs on the reader
// goroutine; t4 is the arrival time taken right after Read.
func (c *conn) onPush(f *protocol.Push, t4 time.Time) {
	p := c.p
	p.rec.PushReceived()
	if !c.pushRequested {
		p.log.Warn("received a push that was not requested; acking it anyway",
			"conn_id", c.connID, "push_seq", f.PushSeq)
	}

	// Ack immediately via the writer, copying trace_id, conn_id and
	// push_seq. client_recv_epoch_ns is for span attributes only.
	t4ns := t4.UnixNano()
	c.enqueue(writeReq{frame: protocol.AppendPushAck(nil, f.TraceID, f.ConnID, f.PushSeq, t4ns), t1Off: -1})
	p.tr.PushAck(PushAckInfo{
		Traceparent: f.Traceparent, TraceID: f.TraceID, ConnID: f.ConnID,
		PushSeq: f.PushSeq, ClientRecvEpochNs: t4ns, Start: t4, End: time.Now(),
	})

	// Gap: client-clock, monotonic difference between consecutive arrivals
	// on this connection. Not observed for the first push.
	st := &c.push
	if st.have {
		p.rec.PushGap(t4.Sub(st.last))
	}
	st.last, st.have = t4, true

	// Loss and order. Counters never decrement: for 3, 1, 2 lost +2 and
	// out-of-order +2 both stand.
	switch {
	case f.PushSeq > st.hwm+1:
		p.rec.PushLost(f.PushSeq - st.hwm - 1)
	case f.PushSeq < st.hwm:
		p.rec.PushOutOfOrder()
	case f.PushSeq == st.hwm:
		p.log.Debug("duplicate push_seq", "conn_id", c.connID, "push_seq", f.PushSeq)
	}
	if f.PushSeq > st.hwm {
		st.hwm = f.PushSeq
	}
}
