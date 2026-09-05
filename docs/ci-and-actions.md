# CI & GitHub Actions

KayaDB uses [GitHub Actions](https://github.com/Tuntii/KayaDB/actions) for continuous integration, correctness gates, releases, and documentation deployment.

**Actions tab empty or workflows never run?** See [Troubleshooting](#troubleshooting-actions-tab-empty).

---

## Workflow overview

| Workflow | File | Triggers | Purpose |
|---|---|---|---|
| **CI** | [`ci.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/ci.yml) | `push` `main`, `pull_request`, **`workflow_dispatch`** | `fmt`, `clippy`, tests (excludes `kaya-jepsen-test` in PR path), smoke bench, `perf_gate`, TypeScript client `npm test` |
| **Audit** | [`audit.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/audit.yml) | `push` `main`, `pull_request`, weekly cron, **`workflow_dispatch`** | `cargo audit` + `cargo deny` |
| **Chaos matrix** | [`chaos-matrix.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/chaos-matrix.yml) | PR smoke, nightly cron, **`workflow_dispatch`** | DiskFull / NetworkPartition / ClockSkew axes |
| **Jepsen** | [`jepsen.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/jepsen.yml) | PR smoke, `push` `main` (smoke), nightly + tags (full), **`workflow_dispatch`** (`smoke`/`full`) | Rust-native T1–T7 full gate |
| **Docs (Pages)** | [`docs.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/docs.yml) | `push` `main` (docs + companion files), **`workflow_dispatch`** | Deploy Docsify site to GitHub Pages |
| **Release** | [`release.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/release.yml) | `push` tags `v*` | Multi-platform binaries + release assets |
| **Publish** | [`publish.yml`](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/publish.yml) | Manual / after Release | crates.io publish helper |

`workflow_dispatch` is the supported workaround when a `push` event does not create a check suite (org Actions enablement, billing, or GitHub delivery gaps). Keep it on CI, Jepsen, chaos, audit, and docs. Do not remove it.

### Trigger matrix (who runs when)

| Event | CI | Audit | Jepsen smoke | Jepsen full | Chaos smoke | Chaos full | Docs |
|---|---|---|---|---|---|---|---|
| `pull_request` | yes | yes | yes | no | yes | no | no |
| `push` to `main` | yes | yes | yes | no | no | no | path-filtered |
| `push` tag `v*` | no | no | no | yes | no | no | no |
| `schedule` | no | weekly | no | nightly | no | nightly | no |
| `workflow_dispatch` | yes | yes | input=`smoke` | input=`full` | no | yes | yes |

Badges in the [repository README](https://github.com/Tuntii/KayaDB/blob/main/README.md) link to the CI workflow.

---

## What runs on every `main` push

1. **CI** — format, clippy, unit/integration tests, smoke benchmark, performance regression gate, TypeScript client tests (`clients/kaya-ts`).
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

## Troubleshooting: `push` to `main` did not start CI

Symptom: a commit lands on `main` (or a PR branch) but the Actions tab shows no new **CI** / **Jepsen** run, while **schedule** and **workflow_dispatch** still work.

1. Confirm the workflow `on:` block still lists `push` (CI and Jepsen do). A missing `push:` is the only in-repo cause.
2. GitHub org/repo: Settings → Actions → General → allow GitHub Actions; check billing minutes.
3. If GitHub dropped the `push` delivery, run the workflow manually: Actions → **CI** → Run workflow (`workflow_dispatch`). Same for **Jepsen** (suite `smoke` or `full`).
4. Optional hardening: branch protection on `main` requiring the `rust` check. This repo currently documents squash-merge only (no merge commits); required checks are operator-configured, not in-tree.

`workflow_dispatch` stays on CI and Jepsen as the supported retry path. Do not treat a missing push suite as a reason to drop `push:` triggers.

---

## Related

- [Development guide](development.md)
- [Releases](releases.md)
- [Publishing](publishing.md)