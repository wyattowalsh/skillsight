# Skillsight Web

Contract-first React + D3 frontend for Skillsight analytics.

## Start

```bash
cd web
pnpm install
pnpm run dev
```

## Build

```bash
cd web
pnpm run build
```

## Runtime configuration

- `VITE_USE_MOCKS=false` (default) calls the hybrid runtime (static data + search API) and surfaces request failures.
- `VITE_USE_MOCKS=true` explicitly enables local fixtures.
- `VITE_DATA_BASE_URL=<r2-or-cdn-base-url>` points the UI at the static web data pack host.
- `VITE_SEARCH_API_BASE_URL=<worker-url>` points the UI at the tiny search Worker.
- `VITE_API_BASE_URL=<legacy-worker-url>` remains as a transition fallback if `VITE_DATA_BASE_URL` / `VITE_SEARCH_API_BASE_URL` are unset.

## Deploy on Vercel

1. Import the repo into Vercel and set **Root Directory** to `web`.
2. Use the project config in `web/vercel.json` (Vite build + SPA route fallback).
3. Set env vars for both Preview and Production:
   - `VITE_DATA_BASE_URL=https://<r2-public-domain-or-cdn>`
   - `VITE_SEARCH_API_BASE_URL=https://<search-worker-domain>`
   - `VITE_USE_MOCKS=false`
4. Validate routes after deploy:
   - `/`
   - `/stats`
   - `/skill/vercel-labs%2Fskills%2Ffind-skills`

## Contract source

- Active search API contract: `../contracts/worker_search_openapi.json`
- Legacy full API contract (transition): `../contracts/worker_openapi.json`
- `uv run skillsight contract` prints the active search contract by default; use `--surface legacy` or `--surface all` for transition/dual views.
- Mock fixtures: `../contracts/fixtures/v1/*.json`
