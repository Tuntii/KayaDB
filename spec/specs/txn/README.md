# TLA+ models: transactions (M17 / M23)

Abstract commit models for KayaDB transactions.

| File | Role |
|------|------|
| `TxnCommit.tla` | Single-group SI: Open / Committed / Aborted, intents, WW conflict |
| `TxnCommit.cfg` | TLC constants (`2` txns, `2` keys, `MaxTs=4`) |
| `TwoPhaseCommit.tla` | Multi-group 2PC sketch: prepare / decide / commit-or-abort / recovery |
| `TwoPhaseCommit.cfg` | TLC constants (`Participants = {g1, g2}`) |

## TxnCommit (M17 single-group SI)

- **Begin** — assign `read_ts` from a global logical clock; txn becomes Open
- **StageWrite** — place a write intent; rejected if another Open txn holds the key
- **Commit** — abort on write-write conflict, or atomic materialization at one `commit_ts`
- **Abort** — clear intents; no committed versions for this txn

| Model invariant | Spec ID |
|-----------------|---------|
| `AtMostOneIntentPerKey` | TXN-3 |
| `IntentsOnlyForOpen` / `NoIntentsWhenFinished` | TXN-6 + commit atomicity |
| `CommittedWithinClock` | commit_ts monotonicity at abstract level |

Not modeled: Raft, WAL, multi-group, client retries, RYW buffer details.

## TwoPhaseCommit (M23 multi-group sketch)

- **StartPrepare** — coordinator marks all participants preparing
- **ParticipantPrepare / Fail** — each group prepares or aborts
- **CoordDecideCommit / Abort** — global decision only after all prepared (commit)
  or any prepare failure / recovery (abort)
- **ParticipantCommit / Abort** — deliver decision; terminal states uniform
- **RecoverAbort** — conservative crash recovery while preparing/prepared

| Model invariant | Spec ID |
|-----------------|---------|
| `NoCommitBeforeDecision` | TXN-2PC-1 |
| `NoCommitOnAbort` / `NoAbortOnCommit` | TXN-2PC-1 / TXN-2PC-3 |
| `TerminalUniform` / `CoordCommitConsistent` | TXN-2PC-2 |

Not modeled: Raft log, SI WW conflicts, client retries, partial network reorder.

## Running TLC (if available)

Install [TLA+ Tools](https://github.com/tlaplus/tlaplus/releases) or the `tla2tools.jar` CLI.

From this directory:

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC -config TxnCommit.cfg TxnCommit.tla
java -cp /path/to/tla2tools.jar tlc2.TLC -config TwoPhaseCommit.cfg TwoPhaseCommit.tla
```

With the VS Code TLA+ extension: open the `.tla` file, run **Check model with TLC**
and select the matching `.cfg`.

Expected: no invariant violations on the small constant sets.

## Documentation-grade use

If TLC is not installed in CI, treat these models as **executable documentation**:
they record intended abstract semantics for reviewers and future formal checks.

Rust integration tests (`test_cross_range_txn_commit`,
`test_multi_range_bank_sum_invariant`) and the Jepsen bank workload remain the
primary automated gates for transaction correctness.