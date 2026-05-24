# Contributing to KayaDB

KayaDB development is design-first and invariant-driven.

Before opening a PR:

1. Link the relevant roadmap item from [`ROADMAP.md`](ROADMAP.md), or explain the design context in the PR.
2. Mention the invariant IDs affected by the change when applicable.
3. Add or update tests for correctness behavior, especially crash/corruption paths.
4. Run:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Correctness beats cleverness. If a behavior can fail after crash, corruption, or reordering, prefer a deterministic regression test over a comment.
