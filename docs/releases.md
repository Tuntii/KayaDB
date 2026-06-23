# Releases & Versioning

KayaDB uses [Semantic Versioning](https://semver.org/) while in `0.1.x` pre-1.0 development. Breaking storage-format or wire-protocol changes bump the minor segment; patch releases add features and fixes within a milestone.

**Changelog:** [CHANGELOG.md](../CHANGELOG.md)  
**Roadmap:** [ROADMAP.md](../ROADMAP.md)

---

## Current release — v0.1.43

| Item | Detail |
|---|---|
| **Tag** | [`v0.1.43`](https://github.com/Tuntii/KayaDB/releases/tag/v0.1.43) |
| **Date** | 2026-06-23 |
| **Milestone** | M14 — correctness + algorithms |
| **Workspace version** | `0.1.43` in root `Cargo.toml` |

### Highlights

- **Compaction policies** — L0 merge, leveled, and size-tiered strategies via `CompactionPolicy`
- **SSTable bloom filter** — v2 footer with configurable `bloom_bits_per_key`
- **WAL group-commit batching** — `WalBatchWriter` with record/byte/time flush limits
- **Module splits** — `kaya-engine`, `kaya-server/cluster`, and `kayactl` decomposed for maintainability
- **CI gates** — chaos-matrix workflow, Jepsen T1–T7 nightly gate, `cargo audit` + `cargo deny`

### Install this version

```bash
cargo install kayactl --version 0.1.43
cargo install kaya-server --bin kayadb-server --version 0.1.43
```

Or download binaries from the [v0.1.43 GitHub Release](https://github.com/Tuntii/KayaDB/releases/tag/v0.1.43).

See [Installation](installation.md) for full options.

---

## Release artifacts

Each `v*` tag triggers [`.github/workflows/release.yml`](../.github/workflows/release.yml):

| Artifact | Contents |
|---|---|
| `kayadb-<tag>-<target>.tar.gz` / `.zip` | `kayadb-server`, `kayactl`, README, CHANGELOG, security guide |
| [crates.io](https://crates.io) packages | Workspace crates published via `scripts/smart_publish.ps1` after the tag |

Jepsen full gate (`T1–T7`) runs on tag pushes and nightly — see [jepsen-design](jepsen-design.md).

---

## Version numbering

| Source | Rule |
|---|---|
| **Tagged releases** | Explicit `v0.1.N` tags (e.g. `v0.1.43`) |
| **crates.io publish helper** | `scripts/smart_publish.ps1` can auto-bump to `0.1.<git-commit-count>` unless `-SkipVersionUpdate` |
| **Development** | `main` may be ahead of the latest tag; see [CHANGELOG Unreleased](../CHANGELOG.md) |

Pre-1.0 policy: treat every release as a correctness prototype until [M14 exit gates](../ROADMAP.md#m14--correctness--algorithms-) are met.

---

## Previous tags

| Tag | Notes |
|---|---|
| `v0.1.43` | M14 storage algorithms + CI correctness gates (current) |
| `v0.1.4` | Earlier prototype release |

Full history: [CHANGELOG](../CHANGELOG.md) and git tags on GitHub.

---

## For maintainers

### Cut a release

1. Ensure `CHANGELOG.md` has a dated section for the new version
2. Bump `[workspace.package] version` in root `Cargo.toml` if not using commit-count auto-bump
3. Merge CI fixes (e.g. from `tag/v0.1.43` branch when applicable)
4. Tag and push:

```bash
git tag -a v0.1.44 -m "v0.1.44: ..."
git push origin v0.1.44
```

5. CI builds multi-platform binaries and creates the GitHub Release
6. Publish crates: `.\scripts\smart_publish.ps1 -SkipVersionUpdate` (or let `publish.yml` run on tag)

### Release branches

Release-prep branches like `tag/v0.1.43` hold CI and publish fixes that land on `main` before or after the tag. Prefer merging those fixes to `main` so docs and workflows stay aligned.

See [Publishing](publishing.md) for docs deploy and crates.io details.

---

## Upgrade notes

- **Storage formats** — inspectable and versioned internally; pre-1.0 compatibility is best-effort. Run `kayactl recover --dry-run` after upgrading before serving traffic.
- **Cluster rolling upgrade** — follow [rolling restart runbook](runbooks/rolling-restart.md); verify leader health with `kayactl status` between nodes.
- **TLS** — enable `--features tls` on server and CLI when moving from plain TCP; see [security](security.md).