# LogsGo documentation site

Hugo-based project site deployed to GitHub Pages at:

**https://saumya40-codes.github.io/LogsGO/**

## Local development

```bash
# Hugo extended required
cd website
hugo server -D
```

Site is served with the configured `baseURL`; for local preview Hugo uses `localhost`.

## Build

```bash
cd website
hugo --gc --minify
# output in public/
```

## Content layout

| Path | Purpose |
|------|---------|
| `content/docs/` | Architecture, storage, QL, auth, benchmarks |
| `content/guides/` | Getting started, Docker, client |
| `content/api/` | REST, gRPC, CLI |
| `content/deployment/` | Docker, Helm, config files |
| `static/images/` | Diagrams copied from repo `docs/` |
| `layouts/` | Custom theme (no external theme dependency) |
| `assets/css` | Site styles |

CI workflow: `.github/workflows/pages.yml` (builds on pushes to `main` that touch `website/`).
