package probe

import (
	"context"

	"github.com/asappinc-opensource/wsmon/internal/config"
)

// runPingpong: send → wait for the echo or MESSAGE_TIMEOUT → wait
// PINGPONG_INTERVAL → next. At most one probe in flight; payloads rotate
// one per probe.
func (c *conn) runPingpong(ctx context.Context) modeResult {
	for {
		if ctx.Err() != nil {
			return resShutdown
		}
		e, ok := c.sendProbe(c.p.nextPayload(), "", nil, false)
		if !ok {
			return resClosed
		}
		select {
		case <-e.done:
		case <-c.dead:
			return resReconnect
		case <-c.readerDone:
			return resClosed
		case <-ctx.Done():
			return resShutdown
		}
		if r := c.wait(ctx, c.p.cfg.PingpongInterval); r != resNone {
			return r
		}
	}
}

// runBurst: BURST_SIZE probes back to back with one payload (rotation is per
// burst), wait for all of them to resolve, then wait BURST_INTERVAL measured
// from the end of the burst. Completion is observed by the
// correlator when the burst's last entry resolves.
func (c *conn) runBurst(ctx context.Context) modeResult {
	for {
		if ctx.Err() != nil {
			return resShutdown
		}
		payload := c.p.nextPayload()
		g := newBurstGroup(payload, c.p.cfg.BurstSize)
		for i := 0; i < c.p.cfg.BurstSize; i++ {
			if _, ok := c.sendProbe(payload, "", g, i == 0); !ok {
				return resClosed
			}
		}
		select {
		case <-g.done:
		case <-c.dead:
			return resReconnect
		case <-c.readerDone:
			return resClosed
		case <-ctx.Done():
			return resShutdown
		}
		if r := c.wait(ctx, c.p.cfg.BurstInterval); r != resNone {
			return r
		}
	}
}

// runIdle walks the ladder: for each rung, write nothing for the
// rung's duration, then send one small probe. Survived → next rung on the
// same connection; after the last rung, start again on the same connection.
// A close during the silence or with the probe in flight drops the rung and
// ends the connection; the next connection restarts at rung 0. No echo and no
// close drops the rung with close_code "none", and the client closes 1000 to
// reconnect. Shutdown interrupts a rung and records nothing for it.
func (c *conn) runIdle(ctx context.Context) modeResult {
	rec := c.p.rec
	for {
		for _, rung := range c.p.cfg.IdleLadder {
			bucket := config.IdleBucketLabel(rung)
			switch c.wait(ctx, rung) {
			case resShutdown:
				return resShutdown
			case resClosed:
				c.idleDropped(bucket, c.remoteCodeLabel())
				return resClosed
			}
			e, ok := c.sendProbe(PayloadSmall, bucket, nil, false)
			if !ok {
				<-c.readerDone // the writer only stops when the socket is gone
				c.idleDropped(bucket, c.remoteCodeLabel())
				return resClosed
			}
			select {
			case o := <-e.done:
				if c.readerClosed() {
					c.idleDropped(bucket, c.remoteCodeLabel())
					return resClosed
				}
				if ctx.Err() != nil {
					return resShutdown
				}
				if o == OutcomeLost {
					c.idleDropped(bucket, IdleCloseNone)
					c.p.log.Warn("idle probe not echoed within message timeout; reconnecting",
						"conn_id", c.connID, "idle_bucket", bucket)
					return resReconnect
				}
				// ok, or mismatched: an echo arrived, so the connection
				// survived the silence. A mismatch is counted separately.
				rec.IdleProbe(bucket, IdleSurvived)
			case <-c.readerDone:
				c.idleDropped(bucket, c.remoteCodeLabel())
				return resClosed
			case <-ctx.Done():
				return resShutdown
			}
		}
	}
}

func (c *conn) idleDropped(bucket, closeCode string) {
	c.p.rec.IdleProbe(bucket, IdleDropped)
	c.p.rec.IdleClose(bucket, closeCode)
	c.p.log.Info("idle rung dropped", "conn_id", c.connID, "idle_bucket", bucket, "close_code", closeCode)
}
