# Releases & Versioning

KayaDB uses [Semantic Versioning](https://semver.org/) while in `0.1.x` pre-1.0 development. Breaking storage-format or wire-protocol changes bump the minor segment; patch releases add features and fixes within a milestone.

**Changelog:** [CHANGELOG.md](../CHANGELOG.md)  
**Roadmap:** [ROADMAP.md](../ROADMAP.md)

---

## Current release — v0.1.46

| Item | Detail |
|---|---|
| **Tag** | `v0.1.46` (tag when published; `main` carries this version) |
| **Date** | 2026-06-30 |
| **Milestone** | M15 — remaining tracks (auth, ops, clients, deploy) ✅ |
| **Workspace version** | `0.1.46` in root `Cargo.toml` |

### Highlights

- **Client token auth** — `--client-token` / `KAYA_CLIENT_TOKEN` for PUT/GET/DELETE/SCAN/STATS (opcodes 1–4, 6)
- **Audit logging** — JSONL at `{data_dir}/audit.jsonl` (`--audit-log` / `--no-audit-log`)
- **Prometheus** — HTTP `/metrics` via `--metrics-addr` (default `127.0.0.1:9090`)
- **Go client** — `clients/kaya-go/` with leader redirect and client token
- **Conformance** — `docs/clients/conformance/vectors.json` + Rust runner
- **HELLO handshake** — opcode 0, `PROTO_VERSION = 1`
- **`kayactl watch`** — poll remote `status` / STATS on an interval
- **Deploy examples** — `deploy/docker/` (Compose) and `deploy/k8s/` (StatefulSet)
- **`kaya-ebpf`** — workspace stub crate restored

### Install this version

```bash
cargo install kayactl --version 0.1.46
cargo install kaya-server --bin kayadb-server --version 0.1.46
```

Or build from `main` / download binaries from the latest [GitHub Release](https://github.com/Tuntii/KayaDB/releases) when tagged.

---

## Previous release — v0.1.45

| Item | Detail |
|---|---|
| **Tag** | [`v0.1.45`](https://github.com/Tuntii/KayaDB/releases/tag/v0.1.45) |
| **Date** | 2026-06-27 |
| **Milestone** | Post-M14 tracks A/B/C — storage codecs + correctness artifacts ✅ |

Highlights: SSTable v3 codecs (LZ4/ZSTD/prefix), block cache stats, rich Jepsen nemesis, manifest TLA+.

```bash
cargo install kayactl --version 0.1.45
cargo install kaya-server --bin kayadb-server --version 0.1.45
```

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
| **Tagged releases** | Explicit `v0.1.N` tags (e.g. `v0.1.44`) |
| **crates.io publish helper** | `scripts/smart_publish.ps1` can auto-bump to `0.1.<git-commit-count>` unless `-SkipVersionUpdate` |
| **Development** | `main` may be ahead of the latest tag; see [CHANGELOG Unreleased](../CHANGELOG.md) |

---

## Release history (recent)

| Tag | Summary |
|---|---|
| `v0.1.45` | Post-M14: ZSTD/prefix/cache stats, rich nemesis, eBPF stub, manifest TLA+ (current) |
| `v0.1.44` | M14 closure: Jepsen full gate, honest partition, io_uring prototype |
| `v0.1.43` | M14 storage algorithms + CI correctness gates |
| `v0.1.4` | Early public prototype |

---

## Cutting a release

1. Finalize [CHANGELOG.md](../CHANGELOG.md) for the target version
2. Update this page and [installation.md](installation.md) version pins
3. Commit on `main`, then tag: `git tag v0.1.N && git push origin v0.1.N`
4. GitHub Actions builds multi-platform binaries and creates the GitHub Release
5. Publish crates: `powershell -File scripts/smart_publish.ps1 -SkipVersionUpdate`

Release-prep branches like `tag/v0.1.44` hold CI and publish fixes that land on `main` before or after the tag. Prefer merging those fixes to `main` so docs and workflows stay aligned.