# Productization north star

**Last updated:** 2026-06-14

KayaDB is **experimental today**, but the project is deliberately being evolved into a **trustworthy, deployable distributed database** — not a forever-demo.

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
4. **Operations** — backup/restore story, rolling restart procedure, `kayactl`/docs for day-2 tasks (add/remove node, detect split-brain symptoms).
   See `docs/runbooks/` for `add-remove-node.md`, `rolling-restart.md`, `backup-restore.md`, `detecting-split-brain.md`.
5. **Security audit pass** — [security.md](security.md) enforcement table expanded (operator credentials, mTLS sidecar, snapshot protections etc. cross-referenced to code and new runbooks).
6. **Performance envelope** ✅ — published benchmark methodology + regression budget in CI ([BENCHMARKS.md](../BENCHMARKS.md) gates + `perf_gate` release assertion). CI regression gate runs on every PR/push.

### Exit criteria (drop the “experimental” label)

- All six items above complete, with tests or documented runbooks.
- `cargo test --workspace` + distributed integration suite green.
- No known correctness gaps listed as accepted risk in [security.md](security.md).

---

## Milestone sequence (reminder)

```text
M11 — Benchmarks, concurrent lin-check, snapshots, dynamic membership ✅
M12 — Jepsen prep + Linux observability experiments
M13 — Productization (this document)
```

The living milestone checklist also lives in local `ROADMAP.md` (may be gitignored in some clones).