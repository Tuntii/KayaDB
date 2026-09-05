/**
 * Transport retry policy for KayaClient.
 *
 * Leader redirects have a separate budget (`maxRedirects`) and do not consume
 * a retry attempt. Matches crates/kaya-client and clients/kaya-go RetryPolicy.
 */

const U64_MASK = 0xffff_ffff_ffff_ffffn;
const XORSHIFT_MULT = 0x2545_f491_4f6c_dd1dn;
const SEED_MIX = 0x9e37_79b9_7f4a_7c15n;

export type RetryPolicy = {
  /** Maximum transport attempts per operation (including the first). Min 1. */
  maxAttempts: number;
  /** Delay before the first retry in milliseconds; doubles each subsequent retry. */
  baseBackoffMs: number;
  /** Cap on a single backoff interval in milliseconds. */
  maxBackoffMs: number;
  /** Full jitter: uniform in [0, computed] when true. */
  jitter: boolean;
  /**
   * Per-attempt deadline in milliseconds. 0 disables this timeout and falls
   * back to `KayaClient` `timeoutMs` (an attempt may still block if that is 0).
   */
  requestTimeoutMs: number;
};

export type RngState = { seed: bigint };

/** Rust/Go defaults: 4 attempts, 50ms base, 2s cap, jitter on, 5s per-attempt. */
export function defaultRetryPolicy(): RetryPolicy {
  return {
    maxAttempts: 4,
    baseBackoffMs: 50,
    maxBackoffMs: 2_000,
    jitter: true,
    requestTimeoutMs: 5_000,
  };
}

/** Single-shot policy with no timeout (historical behavior). */
export function retryPolicyNone(): RetryPolicy {
  return {
    maxAttempts: 1,
    baseBackoffMs: 0,
    maxBackoffMs: 0,
    jitter: false,
    requestTimeoutMs: 0,
  };
}

/**
 * Backoff in milliseconds before the retry that follows `retryIndex` (0-based:
 * 0 is the wait before the first retry). `rng` is advanced for deterministic jitter.
 */
export function backoffMs(policy: RetryPolicy, retryIndex: number, rng: RngState): number {
  const us = backoffUs(policy, retryIndex, rng);
  return Number(us) / 1000;
}

function backoffUs(policy: RetryPolicy, retryIndex: number, rng: RngState): bigint {
  const baseUs = BigInt(Math.max(0, Math.floor(policy.baseBackoffMs * 1000)));
  let scaled: bigint;
  if (retryIndex >= 63) {
    scaled = U64_MASK;
  } else {
    const shift = 1n << BigInt(retryIndex);
    if (baseUs !== 0n && shift > U64_MASK / baseUs) {
      scaled = U64_MASK;
    } else {
      scaled = baseUs * shift;
    }
  }
  const capUs = BigInt(Math.max(0, Math.floor(policy.maxBackoffMs * 1000)));
  let capped = scaled > capUs ? capUs : scaled;
  if (!policy.jitter || capped === 0n) {
    return capped;
  }
  let x = rng.seed & U64_MASK;
  x ^= x >> 12n;
  x ^= (x << 25n) & U64_MASK;
  x ^= x >> 27n;
  x &= U64_MASK;
  rng.seed = x;
  const r = (x * XORSHIFT_MULT) & U64_MASK;
  return r % (capped + 1n);
}

/** Deterministic seed from addr (xorshift family); low bit is always set. */
export function seedFromAddr(addr: string): bigint {
  let seed = SEED_MIX;
  const bytes = Buffer.from(addr, "utf8");
  for (let i = 0; i < bytes.length; i++) {
    seed = (((seed << 5n) | (seed >> 59n)) & U64_MASK) ^ BigInt(bytes[i]);
  }
  return seed | 1n;
}
