# CI & GitHub Actions

KayaDB uses [GitHub Actions](https://github.com/Tuntii/KayaDB/actions) for continuous integration, correctness gates, releases, and documentation deployment.

**Actions tab empty or workflows never run?** See [Troubleshooting](#troubleshooting-actions-tab-empty).

---

## Workflow overview

| Workflow | File | Triggers | Purpose |
|---|---|---|---|
| **CI** | [`ci.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/ci.yml) | `push` to `main`, all `pull_request` | `fmt`, `clippy`, tests (excludes `kaya-jepsen-test` in PR path), smoke bench, `perf_gate` |
| **Audit** | [`audit.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/audit.yml) | `push` to `main`, `pull_request`, weekly cron | `cargo audit` + `cargo deny` |
| **Chaos matrix** | [`chaos-matrix.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/chaos-matrix.yml) | PR smoke, nightly cron | DiskFull / NetworkPartition / ClockSkew axes |
| **Jepsen** | [`jepsen.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/jepsen.yml) | PR smoke, nightly + tags | Rust-native T1–T7 full gate |
| **Docs (Pages)** | [`docs.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/docs.yml) | `push` to `main` (docs + companion files), `workflow_dispatch` | Deploy Docsify site to GitHub Pages |
| **Release** | [`release.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/release.yml) | `push` tags `v*` | Multi-platform binaries + release assets |
| **Publish** | [`publish.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/publish.yml) | Manual / after Release | crates.io publish helper |

Badges in the [repository README](https://github.com/Tuntii/KayaDB/blob/main/README.md) link to the CI workflow.

---

## What runs on every `main` push

1. **CI** — format, clippy, unit/integration tests, smoke benchmark, performance regression gate.
2. **Audit** — dependency vulnerability scan.
3. **Jepsen smoke** — scenario smoke on `main` pushes.
4. **Docs** — when `docs/**`, `ROADMAP.md`, `CHANGELOG.md`, deploy READMEs, or `spec/docs/**` change; rebuilds [https://tuntii.github.io/KayaDB/](https://tuntii.github.io/KayaDB/).

Chaos matrix and Jepsen full gate run on schedule and tags; PRs get smoke subsets.

---

## Branch protection note

`main` may reject **merge commits** (`GH013: This branch must not contain merge commits`). Use **squash merge** or rebase when integrating PRs.

---

## Local checks (match CI)

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --exclude kaya-jepsen-test -- --test-threads=1
```

On Windows, full parallel `cargo test --workspace` can hit `AddrInUse`; prefer:

```powershell
cargo test --workspace -j 1 -- --test-threads=1
```

---

## Troubleshooting: Actions tab empty

1. **Billing / plan** — Free tier minutes or account lock stops jobs before they start. Upgrade or fix billing under GitHub Settings → Billing.
2. **Actions disabled** — Repository → Settings → Actions → General → allow actions.
3. **Pages source** — Settings → Pages → Source: **GitHub Actions** (not “Deploy from branch” only).
4. **First-time Pages** — Run **Deploy Documentation** via Actions → workflow_dispatch once after enabling Pages.

---

## Related

- [Development guide](development.md)
- [Releases](releases.md)
- [Publishing](publishing.md)