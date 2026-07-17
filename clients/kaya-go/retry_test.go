package kaya

import (
	"testing"
	"time"
)

func TestBackoffExponentialAndCapped(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts: 5,
		BaseBackoff: 100 * time.Millisecond,
		MaxBackoff:  400 * time.Millisecond,
		Jitter:      false,
	}
	var s uint64 = 1
	if got := policy.Backoff(0, &s); got != 100*time.Millisecond {
		t.Fatalf("backoff(0) = %v, want 100ms", got)
	}
	if got := policy.Backoff(1, &s); got != 200*time.Millisecond {
		t.Fatalf("backoff(1) = %v, want 200ms", got)
	}
	if got := policy.Backoff(2, &s); got != 400*time.Millisecond {
		t.Fatalf("backoff(2) = %v, want 400ms", got)
	}
	// Capped.
	if got := policy.Backoff(3, &s); got != 400*time.Millisecond {
		t.Fatalf("backoff(3) = %v, want 400ms (capped)", got)
	}
	if got := policy.Backoff(30, &s); got != 400*time.Millisecond {
		t.Fatalf("backoff(30) = %v, want 400ms (capped)", got)
	}
}

func TestJitterWithinBoundsAndDeterministic(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts: 4,
		BaseBackoff: 100 * time.Millisecond,
		MaxBackoff:  1000 * time.Millisecond,
		Jitter:      true,
	}
	var s1 uint64 = 42
	var s2 uint64 = 42
	for retry := uint32(0); retry < 6; retry++ {
		a := policy.Backoff(retry, &s1)
		b := policy.Backoff(retry, &s2)
		if a != b {
			t.Fatalf("same seed must yield same jitter: retry=%d a=%v b=%v", retry, a, b)
		}
		capMs := uint64(100) << retry
		if capMs > 1000 {
			capMs = 1000
		}
		if uint64(a.Milliseconds()) > capMs {
			t.Fatalf("jitter exceeded cap: got %v cap %dms", a, capMs)
		}
	}
}

func TestRetryPolicyNoneIsSingleShot(t *testing.T) {
	p := RetryPolicyNone()
	if p.MaxAttempts != 1 {
		t.Fatalf("max attempts = %d, want 1", p.MaxAttempts)
	}
	if p.RequestTimeout != 0 {
		t.Fatalf("request timeout = %v, want 0", p.RequestTimeout)
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	if p.MaxAttempts != 4 {
		t.Fatalf("max attempts = %d, want 4", p.MaxAttempts)
	}
	if p.BaseBackoff != 50*time.Millisecond {
		t.Fatalf("base backoff = %v, want 50ms", p.BaseBackoff)
	}
	if p.MaxBackoff != 2*time.Second {
		t.Fatalf("max backoff = %v, want 2s", p.MaxBackoff)
	}
	if !p.Jitter {
		t.Fatal("jitter should be true by default")
	}
	if p.RequestTimeout != 5*time.Second {
		t.Fatalf("request timeout = %v, want 5s", p.RequestTimeout)
	}
}

func TestSeedFromAddrNonZero(t *testing.T) {
	a := seedFromAddr("127.0.0.1:7379")
	b := seedFromAddr("127.0.0.1:7380")
	if a == 0 || b == 0 {
		t.Fatal("seed must be non-zero")
	}
	if a == b {
		t.Fatal("different addrs should produce different seeds")
	}
	if a&1 == 0 {
		t.Fatal("seed must have low bit set")
	}
}
