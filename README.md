# skillsight

Skillsight v0 extracts and snapshots structured skill metadata from [skills.sh](https://skills.sh) with convergence-driven discovery, then exports daily artifacts for analytics.

## Highlights

- Primary discovery via `/api/skills/all-time/{page}` with convergence passes.
- Repo-page expansion to reduce discovery gaps.
- Structured-only extraction (no full markdown body persistence).
- Daily `skills_full.jsonl`, `skills_full.parquet`, and `metrics.parquet` snapshots.
- Daily `metrics.jsonl` snapshot for lightweight Worker time-series reads.
- Static web data pack exports for CDN/R2 delivery plus a tiny search Worker API.
- Frozen legacy Worker API contract and frontend fixtures for transition compatibility.

## Quickstart

```bash
uv sync --dev
uv run skillsight discover --passes-max 10 --converge-repos 2 --converge-growth 0.1
uv run skillsight extract --structured-only --resume
uv run skillsight verify-completeness --baseline-total 61210
```

## Full pipeline

```bash
uv run skillsight run --structured-only
```

## Backfill vs promote (R2 uploads)

Historical `export-web` uploads are pointer-safe by default.

- Backfill upload (versioned files only; does **not** move latest pointers):
  - `uv run skillsight export-web --date 2025-01-14 --upload-r2`
- Promote a historical snapshot intentionally (moves both `data/v1/latest.json` and `snapshots/latest.json`):
  - `uv run skillsight export-web --date 2025-01-14 --upload-r2 --publish-latest`

Current-day uploads remain ergonomic: `uv run skillsight export-web --upload-r2` still publishes latest pointers by default.

## Contract checks

```bash
uv run skillsight contract
uv run skillsight contract --surface legacy
uv run skillsight contract --surface all
```

`skillsight contract` now defaults to the active tiny search Worker contract (`worker_search_openapi.json`).
Use `--surface legacy` for the frozen transition contract (`worker_openapi.json`).

## Frontend

```bash
cd web
pnpm install
pnpm run dev
```

`VITE_USE_MOCKS` defaults to `false` (no automatic fallback to fixtures on API errors).
Set `VITE_USE_MOCKS=true` for fixture mode, or set:

- `VITE_DATA_BASE_URL` to point the UI at the static web data host (R2/CDN)
- `VITE_SEARCH_API_BASE_URL` to point the UI at the tiny search Worker

`VITE_API_BASE_URL` remains a transition fallback for legacy deployments.

## Deploy web on Vercel

1. Import this repository in Vercel.
2. Set **Root Directory** to `web`.
3. Keep build settings from `web/vercel.json` (`pnpm run build`, output `dist`).
4. Configure production env vars:
   - `VITE_DATA_BASE_URL=https://<your-data-domain>`
   - `VITE_SEARCH_API_BASE_URL=https://<your-search-worker-domain>`
   - `VITE_USE_MOCKS=false`

## Pipeline + API deployment

See [docs/deployment.md](docs/deployment.md).
