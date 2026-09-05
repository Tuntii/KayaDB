import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  backoffMs,
  defaultRetryPolicy,
  retryPolicyNone,
  seedFromAddr,
  type RetryPolicy,
  type RngState,
} from "../src/retry.ts";

describe("RetryPolicy", () => {
  it("default matches Go/Rust (4 attempts, 50ms, 2s, jitter, 5s timeout)", () => {
    const p = defaultRetryPolicy();
    assert.equal(p.maxAttempts, 4);
    assert.equal(p.baseBackoffMs, 50);
    assert.equal(p.maxBackoffMs, 2_000);
    assert.equal(p.jitter, true);
    assert.equal(p.requestTimeoutMs, 5_000);
  });

  it("none is single-shot with no timeout", () => {
    const p = retryPolicyNone();
    assert.equal(p.maxAttempts, 1);
    assert.equal(p.requestTimeoutMs, 0);
  });

  it("backoff is exponential and capped", () => {
    const policy: RetryPolicy = {
      maxAttempts: 5,
      baseBackoffMs: 100,
      maxBackoffMs: 400,
      jitter: false,
      requestTimeoutMs: 0,
    };
    const rng: RngState = { seed: 1n };
    assert.equal(backoffMs(policy, 0, rng), 100);
    assert.equal(backoffMs(policy, 1, rng), 200);
    assert.equal(backoffMs(policy, 2, rng), 400);
    assert.equal(backoffMs(policy, 3, rng), 400);
    assert.equal(backoffMs(policy, 30, rng), 400);
  });

  it("jitter stays within bounds and is deterministic", () => {
    const policy: RetryPolicy = {
      maxAttempts: 4,
      baseBackoffMs: 100,
      maxBackoffMs: 1000,
      jitter: true,
      requestTimeoutMs: 0,
    };
    const s1: RngState = { seed: 42n };
    const s2: RngState = { seed: 42n };
    for (let retry = 0; retry < 6; retry++) {
      const a = backoffMs(policy, retry, s1);
      const b = backoffMs(policy, retry, s2);
      assert.equal(a, b, `same seed must yield same jitter at retry=${retry}`);
      const cap = Math.min(100 * 2 ** retry, 1000);
      assert.ok(a <= cap, `jitter exceeded cap: got ${a} cap ${cap}`);
      assert.ok(a >= 0);
    }
  });

  it("seedFromAddr is non-zero, odd, and addr-sensitive", () => {
    const a = seedFromAddr("127.0.0.1:7379");
    const b = seedFromAddr("127.0.0.1:7380");
    assert.notEqual(a, 0n);
    assert.notEqual(b, 0n);
    assert.notEqual(a, b);
    assert.equal(a & 1n, 1n);
  });
});
