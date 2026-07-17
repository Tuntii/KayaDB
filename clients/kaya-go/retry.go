package kaya

import "time"

// RetryPolicy controls transport retries for a KayaClient.
//
// Leader redirects have a separate budget (maxRedirects) and do not consume
// a retry attempt. Matches crates/kaya-client RetryPolicy.
type RetryPolicy struct {
	// MaxAttempts is the maximum number of transport attempts per operation
	// (including the first). Minimum effective value is 1.
	MaxAttempts int
	// BaseBackoff is the delay before the first retry; doubles each subsequent retry.
	BaseBackoff time.Duration
	// MaxBackoff caps a single backoff interval.
	MaxBackoff time.Duration
	// Jitter applies full jitter (uniform in [0, computed]) when true.
	Jitter bool
	// RequestTimeout is the per-attempt deadline. Zero disables the timeout
	// (an attempt may block indefinitely on a slow server).
	RequestTimeout time.Duration
}

// DefaultRetryPolicy returns the Rust-client defaults: 4 attempts, 50ms base,
// 2s cap, jitter on, 5s per-attempt timeout.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    4,
		BaseBackoff:    50 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Jitter:         true,
		RequestTimeout: 5 * time.Second,
	}
}

// RetryPolicyNone returns a single-shot policy with no timeout (historical behavior).
func RetryPolicyNone() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    1,
		BaseBackoff:    0,
		MaxBackoff:     0,
		Jitter:         false,
		RequestTimeout: 0,
	}
}

// Backoff returns the sleep before the retry that follows retryIndex (0-based:
// 0 is the wait before the first retry). rngState is advanced for deterministic jitter.
func (p RetryPolicy) Backoff(retryIndex uint32, rngState *uint64) time.Duration {
	baseUs := uint64(p.BaseBackoff.Microseconds())
	var scaled uint64
	if retryIndex >= 63 {
		scaled = ^uint64(0) // saturate
	} else {
		// base * 2^retryIndex with saturation
		shift := uint64(1) << retryIndex
		if baseUs != 0 && shift > (^uint64(0))/baseUs {
			scaled = ^uint64(0)
		} else {
			scaled = baseUs * shift
		}
	}
	capUs := uint64(p.MaxBackoff.Microseconds())
	capped := scaled
	if capped > capUs {
		capped = capUs
	}
	if !p.Jitter || capped == 0 {
		return time.Duration(capped) * time.Microsecond
	}
	// Full jitter via xorshift64* (same family as the Rust client).
	x := *rngState
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	*rngState = x
	r := x * 0x2545_F491_4F6C_DD1D
	return time.Duration(r%(capped+1)) * time.Microsecond
}

func seedFromAddr(addr string) uint64 {
	seed := uint64(0x9e37_79b9_7f4a_7c15)
	for i := 0; i < len(addr); i++ {
		seed = (seed<<5 | seed>>(64-5)) ^ uint64(addr[i])
	}
	return seed | 1
}
