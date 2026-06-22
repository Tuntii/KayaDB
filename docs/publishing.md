# Publishing the Documentation on GitHub Pages

We publish the documentation using **GitHub Pages** + **Docsify**. This gives us a fast, beautiful documentation site directly from the `docs/` folder with no heavy build step for content.

## Live Site

Once enabled, the documentation will be available at:

```
https://tuntii.github.io/KayaDB/
```

## Local Preview (Development)

The easiest way to preview the site locally:

```bash
# Option A: Using Python (recommended, no install)
cd docs
python -m http.server 3000

# Then open http://localhost:3000
```

```bash
# Option B: Using Node (if you have npx)
npx docsify-cli serve docs
```

Docsify will pick up `_sidebar.md` automatically.

## Key Files for the Documentation Site

- `docs/index.html` — Docsify bootstrap + configuration
- `docs/_sidebar.md` — Sidebar navigation (used by Docsify on GitHub Pages)
- `docs/SUMMARY.md` — Used by GitBook (kept for compatibility)
- `docs/.nojekyll` — Prevents GitHub from running Jekyll on the folder

## Updating the Documentation

- Edit any `.md` file inside `docs/`
- Push to `main`
- The site will redeploy automatically (via the workflow)

When adding new major pages:
1. Update `_sidebar.md` (for GitHub Pages / Docsify)
2. Also update `SUMMARY.md` (for GitBook users)
3. Optionally update `docs/specifications.md` if it's a new spec

## Keeping GitBook Option Alive

We still maintain:
- `book.json`
- `SUMMARY.md`

You can connect the repo to [gitbook.com](https://www.gitbook.com) at any time and point it at the `docs/` folder if you ever want a hosted GitBook experience in addition to GitHub Pages.

## Publishing crates to crates.io

Workspace crates are published in dependency order via `scripts/smart_publish.ps1` (see crate list inside the script). The helper bumps the `[workspace.package]` version from git commit count unless `-SkipVersionUpdate` is passed.

Manual publish (after version bump in root `Cargo.toml`):

```powershell
.\scripts\smart_publish.ps1 -SkipVersionUpdate
```

Dry run:

```powershell
.\scripts\smart_publish.ps1 -DryRun -SkipVersionUpdate
```

Release binaries are built automatically on `v*` tags via `.github/workflows/release.yml`.

## GitHub repository topics (set in GitHub UI)

Topics improve discoverability on GitHub and are **not** stored in the repo — maintainers set them under **Repository → About → Topics**. Recommended topics for KayaDB:

- `lsm-tree`
- `raft`
- `storage-engine`
- `correctness`
- `deterministic-testing`
- `jepsen`
- `rust`
- `database`
- `distributed-systems`
- `embedded-database`

Optional extras: `wal`, `consensus`, `chaos-engineering`, `property-testing`.