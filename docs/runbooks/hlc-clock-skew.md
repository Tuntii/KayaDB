# HLC Clock Skew (Uncertainty Bound Exceeded)

Applies when `use_hlc` is enabled (multi-group clusters enable it
automatically; see `spec/docs/multi-raft-spec.md` §8 and
`spec/docs/transactions-spec.md` §17.7).

## Symptom

A node logs / returns `KayaError::ClockSkew`:

```text
clock skew exceeded: remote clock offset 3200ms exceeds max_clock_offset_ms=500
  (local wall clock 1700000000000ms, remote physical 1700000003200ms)
```

This means a remote HLC sample (a peer's commit timestamp, observed via
`Engine::sync_clock`) was more than the configured uncertainty bound ahead of
this node's own wall clock. The sample was **rejected**, not merged — the
local clock is unaffected.

Separately, if a node's clock is genuinely (but plausibly) ahead of another's,
writes on the faster node briefly pause: `prepare_hlc_write_sequence` waits
out the lead (capped at the bound) before the WAL append that exposes the
commit_ts. A commit is never durable/visible before the wall clock has
actually caught up to its assigned timestamp — no operator action needed for
this case, it self-heals in at most `max_clock_offset_micros`.

## Diagnose

1. Compare wall clocks across nodes:
   ```bash
   for h in node1 node2 node3; do ssh "$h" date -u +%s%3N; done
   ```
2. Check NTP/chrony health on each node:
   ```bash
   chronyc tracking   # or: ntpq -p
   ```
   Look for a large `System time` offset or a `Leap status` other than
   `Normal`.
3. Confirm the configured bound:
   ```bash
   kayactl --server <addr> status --json | jq '.max_clock_offset_micros // empty'
   ```
   (falls back to the process's `--max-clock-offset-micros` /
   `KAYA_MAX_CLOCK_OFFSET_MICROS` if not surfaced in status).

## Fix

- **Preferred: fix the clock.** Restart/repair NTP (chrony/ntpd) on the
  skewed node so it re-syncs, then retry the operation that failed. A
  transient NTP hiccup usually self-resolves within seconds.
- **Only after confirming the skew is transient and expected** (e.g. a
  deliberately wide-area deployment with looser time sync), raise the bound:
  ```bash
  kayadb-server --max-clock-offset-micros 2000000 ...   # 2s
  # or: KAYA_MAX_CLOCK_OFFSET_MICROS=2000000
  ```
  Widening the bound trades safety for availability: it lets a more-skewed
  peer's timestamps merge, weakening the real-time ordering guarantee HLC
  commit_ts is meant to provide. Prefer fixing NTP over widening this.
- **Never** patch around this by having the operator manually set the system
  clock backwards on the fast node — that can make the local HLC (which is
  monotonic once ticked) diverge further from wall time, not less.

## Prevention

- Run NTP/chrony on every node with a low-drift, low-latency time source.
- Keep `max_clock_offset_micros` at its default (500ms, matching
  CockroachDB's `--max-offset`) unless the deployment's real network/clock
  topology requires otherwise.
- Alert on `ClockSkew` errors and on NTP offset directly, rather than
  discovering skew only when a rejection happens.

See also:
- `spec/docs/transactions-spec.md` §17.7 (HLC commit timestamps and
  uncertainty)
- `spec/docs/multi-raft-spec.md` §8 (HLC)
- `docs/runbooks/detecting-split-brain.md`
