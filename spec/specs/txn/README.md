# TLA+ model: single-group transaction commit (M17)

Abstract Snapshot Isolation commit model for KayaDB single-group transactions.

| File | Role |
|------|------|
| `TxnCommit.tla` | States (Open / Committed / Aborted), intents, WW conflict, abstract atomic commit |
| `TxnCommit.cfg` | Small constants for TLC (`2` txns, `2` keys, `MaxTs=4`) |

## What is modeled

- **Begin** — assign `read_ts` from a global logical clock; txn becomes Open
- **StageWrite** — place a write intent; rejected if another Open txn holds the key
- **Commit** — either:
  - **abort** on write-write conflict (`committed[k] > read_ts`), or
  - **atomic materialization**: all of the txn's intents become committed versions at one new `commit_ts`
- **Abort** — clear intents; no committed versions for this txn

## Invariants (map to transactions-spec)

| Model invariant | Spec ID |
|-----------------|---------|
| `AtMostOneIntentPerKey` | TXN-3 |
| `IntentsOnlyForOpen` / `NoIntentsWhenFinished` | TXN-6 (rollback cleanup) + commit atomicity |
| `CommittedWithinClock` | commit_ts monotonicity at abstract level |

Not modeled: Raft replication, WAL durability, multi-group, client retries, RYW buffer details.

## Running TLC (if available)

Install [TLA+ Tools](https://github.com/tlaplus/tlaplus/releases) or the `tla2tools.jar` CLI.

From this directory:

```bash
# Using tla2tools.jar (Java required)
java -cp /path/to/tla2tools.jar tlc2.TLC -config TxnCommit.cfg TxnCommit.tla
```

With the VS Code TLA+ extension: open `TxnCommit.tla`, run **Check model with TLC** and select `TxnCommit.cfg`.

Expected: no invariant violations on the small constant set in `TxnCommit.cfg`.

## Documentation-grade use

If TLC is not installed in CI, treat this model as **executable documentation**:
it records the intended abstract semantics of M17 commit (intents + WW conflict +
atomic finish) for reviewers and for future formal checks.

Rust property tests and the Jepsen bank workload remain the primary automated
gates for transaction correctness.