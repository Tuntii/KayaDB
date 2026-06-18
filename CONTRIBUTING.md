# Contributing to KayaDB

Thank you for your interest in contributing to KayaDB!

KayaDB development is **design-first** and **invariant-driven**. We prioritize correctness, inspectability, and reproducible failure testing above all else.

## Code of Conduct

By participating, you agree to uphold our [Code of Conduct](CODE_OF_CONDUCT.md).

## Before you start

1. Check the [ROADMAP.md](ROADMAP.md) and open issues.
2. For larger changes, open an issue first or discuss in a GitHub Discussion to align on design.
3. Read the relevant sections of the documentation in `docs/` (especially `development.md`, `architecture.md`, and specs).

## Contribution expectations

Good contributions usually include:

- A linked roadmap item or clear design note in the PR description.
- Tests (especially crash/recovery, simulation, or malformed input tests when touching persistence).
- Updates to inspectors (`kayactl inspect`) or documentation when formats change.
- Passing the full pre-submit checklist.

## Development setup

```bash
git clone https://github.com/Tuntii/KayaDB.git
cd KayaDB

# Recommended toolchain is pinned
cat rust-toolchain.toml

cargo build --workspace
cargo test --workspace
```

### Required checks before every PR

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

On Windows (PowerShell):

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Pull Request process

1. Fork the repo and create a feature branch from `main`.
2. Make your changes + add tests.
3. Run the full check suite locally.
4. Update or add documentation when behavior or user-facing interfaces change.
5. Fill out the PR template completely (roadmap link, invariants, crash considerations).
6. Request review. Maintainers will merge once CI is green and correctness requirements are met.

## Good first issues

Look for labels or areas such as:

- CLI/UX polish and better JSON output
- Additional malformed input / error path tests
- Documentation improvements and examples
- New benchmark workloads
- Simulator trace coverage
- Inspector output formatting (`kayactl inspect`)

## Questions?

Open an issue with the `question` label or start a discussion.

We value thoughtful, well-tested contributions. Welcome aboard!
