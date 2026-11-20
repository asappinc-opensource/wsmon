package probe

import (
	"math/rand/v2"
	"time"
)

// backoffDelay is exponential backoff with full jitter:
// rand(0, min(cap, base·2^attempt)). attempt 0 gives rand(0, base), which is
// also the pre-first-connect delay.
func backoffDelay(r *rand.Rand, base, cap time.Duration, attempt int) time.Duration {
	ceil := cap
	if attempt < 62 {
		if d := base << attempt; d > 0 && d < cap && d>>attempt == base {
			ceil = d
		}
	}
	if ceil <= 0 {
		return 0
	}
	return time.Duration(r.Int64N(int64(ceil)))
}
