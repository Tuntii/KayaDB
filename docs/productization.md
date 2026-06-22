# Productization north star

**Last updated:** 2026-06-14

KayaDB completed **M13 productization (2026-06-21)** and is deliberately evolved into a **trustworthy, deployable distributed database** — not a forever-demo.

Correctness-first milestones (local engine, simulation, Raft, membership, snapshots) are the foundation. **Productization is the planned next arc.** Do not lose sight of this when prioritizing work.

---

## Today vs target

| Today (honest) | Target (product) |
|---|---|
| Strong LSM engine, sim harness, Raft prototype | Same core, proven under chaos and restart |
| TCP cluster, snapshots, dynamic membership | Survives real restarts and operator workflows |
| Open admin RPCs on trusted localhost | TLS + auth on client and membership/admin paths |
| “Experimental” status badge | Documented deployment guide with explicit SLO/limit envelopes |

**We do not claim production-ready until the exit gates below are met.**

---

## M13 — Productization gates

1. **Durable Raft state** — log, term, vote, and snapshot metadata survive `kayadb-server` restart; cluster reforms without manual re-seeding.
2. **Authenticated transport** — TLS (or documented mTLS sidecar pattern) on Raft and client ports; `ADD_MEMBER` / `REMOVE_MEMBER` require operator credentials.
3. **Chaos proof** ✅ — PR smoke green (0 violations after fixing concurrent-clients + sequential-checker mismatch and adding resilience for kills). T7 + harness complete.
4. **Operations** ✅ — backup/restore, rolling restart, add/remove node, split-brain detection, mTLS sidecar runbooks in `docs/runbooks/`.
5. **Security audit pass** ✅ — [security.md](security.md) enforcement table + §7 accepted risks documented.
6. **Performance envelope** ✅ — published benchmark methodology + regression budget in CI ([BENCHMARKS.md](../BENCHMARKS.md) gates + `perf_gate` release assertion). CI regression gate runs on every PR/push.

### M13 exit (2026-06-21)

All six gates complete. Experimental label dropped. Deployment hardening gaps documented as accepted risks in [security.md §7](security.md#7-accepted-risks-and-future-hardening-m13-exit).

---

## Milestone sequence (reminder)

```text
M11 — Benchmarks, concurrent lin-check, snapshots, dynamic membership ✅
M12 — Jepsen prep + Linux observability experiments
M13 — Productization (this document)
```

The living milestone checklist also lives in local `ROADMAP.md` (may be gitignored in some clones).