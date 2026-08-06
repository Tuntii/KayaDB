# Publishing the Documentation on GitHub Pages

KayaDB publishes user-facing documentation as a **[Docsify](https://docsify.js.org/)** site on **GitHub Pages**.

**Live site:** https://tuntii.github.io/KayaDB/

---

## How the site is built

GitHub Pages serves only what we upload in the deploy workflow — not the entire repository. The `docs/` folder is the primary content, but several companion files live at the repo root and are **bundled at deploy time** by [`scripts/prepare_docs_site.sh`](https://github.com/Tuntii/KayaDB/blob/main/scripts/prepare_docs_site.sh):

| Source (repository) | Published as |
|---|---|
| `docs/**` | Site pages (navigation, guides, runbooks) |
| `ROADMAP.md`, `CHANGELOG.md`, `CONTRIBUTING.md`, `BENCHMARKS.md`, `CODE_OF_CONDUCT.md` | Top-level pages (sidebar **Project**) |
| `deploy/docker/README.md`, `deploy/k8s/README.md` | `deploy/docker/`, `deploy/k8s/` |
| `spec/docs/**`, `spec/issues/expanded-implementation-roadmap.md` | `spec/` tree for the specifications index |

Internal AI planning notes under `docs/superpowers/` are **excluded** from the public site.

### Link rules (important)

- Use **site-relative** paths: `ROADMAP.md`, `CHANGELOG.md`, `deploy/docker/README.md`, `spec/docs/wal-spec.md`.
- Do **not** use `../ROADMAP.md` — that breaks on GitHub Pages (404 / `#/../ROADMAP`).
- Link to repository-only paths (`.github/workflows/`, `crates/*/README.md`) via full GitHub URLs.

---

## Local preview

Serving only `docs/` with Python will **not** include ROADMAP or spec files. Use the prepare script first:

```powershell
# Windows
.\scripts\prepare_docs_site.ps1
cd build\docs-site
python -m http.server 3000
# http://localhost:3000
```

```bash
# Linux / macOS
bash scripts/prepare_docs_site.sh
cd build/docs-site
python3 -m http.server 3000
```

Alternative (Node): `npx docsify-cli serve build/docs-site` after running the prepare script.

---

## Key site files

| File | Role |
|---|---|
| `docs/index.html` | Docsify bootstrap, search, 404 page, edit footer |
| `docs/_sidebar.md` | Sidebar navigation |
| `docs/SUMMARY.md` | GitBook-compatible mirror |
| `docs/404.md` | Friendly not-found page |
| `docs/.nojekyll` | Disable Jekyll on GitHub Pages |

When adding a major page:

1. Add the `.md` file under `docs/` (or ensure it is copied by `prepare_docs_site.sh`).
2. Update `_sidebar.md` and `SUMMARY.md`.
3. Push to `main` — the [Docs workflow](ci-and-actions.md) redeploys automatically when relevant paths change.

---

## GitHub Pages setup (maintainers)

1. Repository → **Settings** → **Pages** → **Build and deployment** → Source: **GitHub Actions**.
2. Push doc changes to `main`, or run **Deploy Documentation (GitHub Pages)** manually from the Actions tab.
3. Confirm https://tuntii.github.io/KayaDB/ loads and sidebar **Project → Roadmap** works.

---

## Publishing crates to crates.io

Workspace crates publish in dependency order via `scripts/smart_publish.ps1`. Set repository secret `CARGO_REGISTRY_TOKEN` for the [Publish Crates](ci-and-actions.md) workflow.

```powershell
.\scripts\smart_publish.ps1 -SkipVersionUpdate   # after version is set in Cargo.toml
.\scripts\smart_publish.ps1 -DryRun -SkipVersionUpdate
```

**Current workspace version:** `0.1.113` (see [releases](releases.md)). Publish order includes `kaya-ebpf` and `kaya-server` before `kayactl`.

---

## Release checklist (maintainers)

1. Finalize [CHANGELOG.md](CHANGELOG.md) for the target version.
2. Add `docs/release-notes/v0.1.N.md` and point `release.yml` `body_path` at it.
3. Update [releases.md](releases.md) and [installation.md](installation.md) version pins.
4. `git tag -a v0.1.N -m "v0.1.N" && git push origin v0.1.N`
5. Verify **Release Binaries** and **Jepsen** tag workflows in Actions.
6. Publish crates (workflow or `smart_publish.ps1`) when `CARGO_REGISTRY_TOKEN` is configured.

Release binaries: `kayadb-v0.1.N-<target>.tar.gz` / `.zip` on [GitHub Releases](https://github.com/Tuntii/KayaDB/releases).

---

## GitHub repository topics

Set under **Repository → About → Topics** (not stored in git):

`lsm-tree`, `raft`, `storage-engine`, `correctness`, `deterministic-testing`, `jepsen`, `rust`, `database`, `distributed-systems`, `embedded-database`