# CI & GitHub Actions

KayaDB uses [GitHub Actions](https://github.com/Tuntii/KayaDB/actions) for continuous integration, correctness gates, releases, and documentation deployment.

**Actions tab empty or workflows never run?** See [Troubleshooting](#troubleshooting-actions-tab-empty).

---

## Workflow overview

| Workflow | File | Triggers | Purpose |
|---|---|---|---|
| **CI** | [`ci.yml`](../.github/workflows/ci.yml) | `push` to `main`, all `pull_request` | `fmt`, `clippy`, tests (excludes `kaya-jepsen-test` in PR path), smoke bench, `perf_gate` |
| **Audit** | [`audit.yml`](../.github/workflows/audit.yml) | `push` to `main`, `pull_request`, weekly cron | `cargo audit` + `cargo deny` |
| **Chaos matrix** | [`chaos-matrix.yml`](../.github/workflows/chaos-matrix.yml) | PR smoke, nightly cron | DiskFull / NetworkPartition / ClockSkew axes |
| **Jepsen** | [`jepsen.yml`](../.github/workflows/jepsen.yml) | PR smoke, nightly + tags | Rust-native T1–T7 full gate |
| **Docs (Pages)** | [`docs.yml`](../.github/workflows/docs.yml) | `push` to `main` when `docs/**` changes, `workflow_dispatch` | Deploy Docsify site to GitHub Pages |
| **Release** | [`release.yml`](../.github/workflows/release.yml) | `push` tags `v*` | Multi-platform binaries + release assets |
| **Publish** | [`publish.yml`](../.github/workflows/publish.yml) | Manual / release pipeline | crates.io publish helper |

Badges in the root [README](../README.md) link to the CI workflow. Other workflows are listed under **Actions** in the GitHub UI.

---

## What runs on every `main` push

After you push to `main`:

1. **CI** — format, clippy, unit/integration tests, smoke benchmark, performance regression gate.
2. **Audit** — dependency vulnerability scan.
3. **Docs** — only when files under `docs/` changed; rebuilds [https://tuntii.github.io/KayaDB/](https://tuntii.github.io/KayaDB/).

Chaos matrix and Jepsen full gate run on schedule and tags; PRs get smoke subsets.

---

## Branch protection note

`main` may reject **merge commits** (`GH013: This branch must not contain merge commits`). Use **squash merge** or rebase when integrating PRs — the M15 push used a single squash commit for this reason.

---

## Local checks (match CI)

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --exclude kaya-jepsen-test -- --test-threads=1
cargo test -p kaya-raft --features disk-storage -- --test-threads=1
```

### Windows tip

Parallel crate test binaries can contend for localhost ports. If tests flake with `AddrInUse`:

```bash
cargo test --workspace -j 1 -- --test-threads=1
```

---

## Troubleshooting: Actions tab empty

If **Settings → Actions** shows workflows disabled or the **Actions** tab has no runs:

1. **Enable Actions** — Repo **Settings → Actions → General → Allow all actions** (or org policy equivalent).
2. **Forks** — Actions do not run on forks until you enable them under **Actions** tab → “I understand my workflows, go ahead and enable them”.
3. **GitHub Pages** — For docs deploy: **Settings → Pages → Build and deployment → Source: GitHub Actions** (not “Deploy from branch”). Without this, `docs.yml` uploads an artifact but the site may not update.
4. **Billing / minutes** — Private repos need Actions minutes; public repos are free for standard workloads.
5. **First push after enable** — Open [Actions](https://github.com/Tuntii/KayaDB/actions) and confirm `CI` ran on the latest `main` commit.

### Manually trigger docs deploy

```text
Actions → Deploy Documentation (GitHub Pages) → Run workflow
```

---

## Related

- [Development guide](development.md) — test tiers, SimDisk, fuzzing
- [Publishing documentation](publishing.md) — local Docsify preview, maintainer flow
- [Jepsen design](jepsen-design.md) — what the Jepsen workflow exercises