# Publishing the Documentation on GitHub Pages

We publish the documentation using **GitHub Pages** + **Docsify**. This gives us a fast, beautiful documentation site directly from the `docs/` folder with no heavy build step for content.

## Live Site

Once enabled, the documentation will be available at:

```
https://tuntii.github.io/KayaDB/
```

## How to Enable GitHub Pages (One-time setup)

### Option 1: Recommended — GitHub Actions (already configured)

We have a workflow at `.github/workflows/docs.yml`.

1. Go to your repository → **Settings → Pages**
2. Under **"Build and deployment"**:
   - **Source**: GitHub Actions
3. The workflow will automatically deploy on every push to `main` that touches `docs/`.

The site will be live at `https://tuntii.github.io/KayaDB/`.

### Option 2: Simple (no Actions) — Deploy from /docs folder

1. Go to **Settings → Pages**
2. Set **Source** to: `Deploy from a branch`
3. Branch: `main`
4. Folder: `/docs`
5. Save.

> Note: Option 1 (Actions) is preferred because it gives better control and works even if you later want a custom domain or more advanced processing.

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