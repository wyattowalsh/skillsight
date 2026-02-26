# Deployment Runbook (Static Data + Search Worker + Prefect)

## Runtime topology

- **Frontend**: Vercel project from `web/` (static Vite output; Cloudflare Pages is also supported).
- **Static data**: Web data pack JSON files published to Cloudflare R2 (public domain / CDN).
- **Search API**: Cloudflare Worker from `worker/` reading the slim search index from R2.
- **Pipeline**: Prefect worker on one dedicated VM (budget target: <$20/month).

## Required runtime configuration

- **Worker (`worker/wrangler.toml`)**: `SKILLSIGHT_DATA` R2 binding.
- **Web (Vercel env vars)**:
  - `VITE_DATA_BASE_URL`
  - `VITE_SEARCH_API_BASE_URL`
  - `VITE_USE_MOCKS=false`
- **Pipeline VM (`.env` or shell env)**:
  - `SKILLSIGHT_OUTPUT_DIR`
  - `SKILLSIGHT_R2_ENDPOINT_URL`
  - `SKILLSIGHT_R2_ACCESS_KEY_ID`
  - `SKILLSIGHT_R2_SECRET_ACCESS_KEY`
  - `SKILLSIGHT_R2_BUCKET_NAME`
  - `SKILLSIGHT_R2_PREFIX=snapshots` (keep default so Worker reads `snapshots/latest.json`).
  - `SKILLSIGHT_WEB_EXPORT_PREFIX=data/v1`
  - `SKILLSIGHT_WEB_EXPORT_PAGE_SIZE=12`

## 1) Pre-deploy checks

Run these from repo root:

```bash
uv run skillsight contract
uv run skillsight contract --surface legacy
pnpm --dir web test
pnpm --dir web run build
npx --prefix worker tsc --noEmit -p worker/tsconfig.json
uv run pytest -q
```

## 2) Deploy search API (Cloudflare Worker)

From repo root:

```bash
pnpm --dir worker install
pnpm --dir worker run deploy
```

### Exact unblock for Cloudflare R2 error `10042`

1. Cloudflare Dashboard → **R2 Object Storage** → click **Enable R2** and accept terms (this one-time account step is what clears error `10042`).
2. Create bucket `skillsight-data` (or the bucket name you bind in this repo).
3. Cloudflare Dashboard → **Workers & Pages** → Worker settings → **Bindings**:
   - Variable: `SKILLSIGHT_DATA`
   - Resource: the R2 bucket above
4. Wait ~1 minute for provisioning, then rerun deploy and verify endpoints:

```bash
pnpm --dir worker run deploy
export WORKER_URL="https://<worker-domain>"
curl -fsS "$WORKER_URL/healthz"
curl -fsS "$WORKER_URL/v1/search?q=skill&page=1&page_size=1"
```

Required binding in `worker/wrangler.toml`:

- `SKILLSIGHT_DATA` (R2 bucket name `skillsight-data` or your configured bucket)

## 3) Deploy web (Vercel)

In Vercel project settings:

1. Set **Root Directory** to `web`.
2. Use `web/vercel.json` (Vite build + SPA rewrite).
3. Configure env vars:
   - `VITE_DATA_BASE_URL=https://<r2-public-domain-or-cdn>`
   - `VITE_SEARCH_API_BASE_URL=https://<search-worker-domain>`
   - `VITE_USE_MOCKS=false`
4. If web was using mock fallback, set the static data and search env vars above, keep `VITE_USE_MOCKS=false`, then redeploy the latest Vercel build.

### Live cutover playbook (cheapest path)

1. Redeploy Worker from repo root:

```bash
pnpm --dir worker run deploy
export WORKER_URL="https://<worker-domain>"
```

2. In Vercel (`web/` root project), switch to live static data + search and redeploy:
   - `VITE_DATA_BASE_URL=https://<r2-public-domain-or-cdn>`
   - `VITE_SEARCH_API_BASE_URL=https://<search-worker-domain>`
   - `VITE_USE_MOCKS=false`
   - Trigger a production redeploy of the latest commit.
3. Post-cutover smoke checks:

```bash
curl -fsS "$WORKER_URL/healthz"
curl -fsS "$WORKER_URL/v1/search?q=skill&page=1&page_size=1"
```

   - Web: `/`, `/stats`, `/skill/vercel-labs%2Fskills%2Ffind-skills`
   - Confirm the app requests static data from `VITE_DATA_BASE_URL` and search queries from `VITE_SEARCH_API_BASE_URL` (not the legacy full API).
4. Cheapest-cost posture: keep the daily pipeline schedule, run one small Prefect VM, and avoid paid add-ons unless a ticket/PR explicitly requires them.

## 4) Deploy pipeline (Prefect + VM)

1. Install project and dependencies:

```bash
uv sync --dev
```

2. Configure environment variables:

```bash
export SKILLSIGHT_OUTPUT_DIR=/var/lib/skillsight/data
export SKILLSIGHT_R2_ENDPOINT_URL=...
export SKILLSIGHT_R2_ACCESS_KEY_ID=...
export SKILLSIGHT_R2_SECRET_ACCESS_KEY=...
export SKILLSIGHT_R2_BUCKET_NAME=skillsight-data
export SKILLSIGHT_R2_PREFIX=snapshots
```

3. Start Prefect worker:

```bash
uv run prefect worker start --pool skillsight-pool
```

4. Deploy flow:

```bash
uv run prefect deploy src/skillsight/pipeline/orchestrator.py:skillsight_pipeline -n daily -p skillsight-pool --cron "0 6 * * *"
```

5. Run one-off smoke execution:

```bash
uv run skillsight run --structured-only
```

### Backfill vs promote when uploading web exports

- Backfill (safe default): `uv run skillsight export-web --date 2025-01-14 --upload-r2`
  - Uploads versioned canonical artifacts and versioned web-pack files only.
  - Does **not** move `data/v1/latest.json` or `snapshots/latest.json`.
- Promote a historical snapshot intentionally:
  - `uv run skillsight export-web --date 2025-01-14 --upload-r2 --publish-latest`
  - Moves both latest pointers together (`data/v1/latest.json` and `snapshots/latest.json`).

## 5) Post-deploy smoke checks

- Web routes: `/`, `/stats`, `/skill/vercel-labs%2Fskills%2Ffind-skills`
- Search API routes: `/healthz`, `/v1/search`
- Static data paths: `/data/v1/latest.json`, `/data/v1/snapshots/<date>/stats/summary.json`
- Confirm web requests hit the configured static data and search endpoints

## 6) Cost guardrails checklist

- [ ] Keep deployment cadence daily (`prefect.yaml` cron + `prefect deploy --cron "0 6 * * *"`).
- [ ] Run a single small VM for the Prefect worker (start with 1 vCPU / 1-2 GB RAM); avoid standby replicas.
- [ ] Check storage growth after the daily run (`du -sh /var/lib/skillsight/data`).
- [ ] Avoid paid add-ons by default (Cloudflare/Vercel/Prefect); only enable with explicit ticket/PR justification.

## 7) Cheap live smoke monitoring (GitHub Actions)

- Workflow: `.github/workflows/live-smoke.yml` runs every 30 minutes and supports manual runs.
- Script: `scripts/live-smoke-check.sh` checks static data, search API, and web targets with plain `curl` (no paid monitoring service required).
- Configure in GitHub repository settings:
  - **Secrets**: `SMOKE_DATA_URL`, `SMOKE_SEARCH_URL`, `SMOKE_WEB_URL`
  - **Variables** (optional path overrides): `SMOKE_DATA_PATH` (default `/data/v1/latest.json`), `SMOKE_SEARCH_PATH` (default `/v1/search?q=skill&page=1&page_size=1`), `SMOKE_WEB_PATH` (default `/`)
- Manual execution:

```bash
export SMOKE_DATA_URL="https://<r2-public-domain-or-cdn>"
export SMOKE_SEARCH_URL="https://<search-worker-domain>"
export SMOKE_WEB_URL="https://<web-domain>"
# Optional:
# export SMOKE_DATA_PATH="/data/v1/latest.json"
# export SMOKE_SEARCH_PATH="/v1/search?q=skill&page=1&page_size=1"
# export SMOKE_WEB_PATH="/"
bash scripts/live-smoke-check.sh
```

## Alerts

Use Prefect notifications or a webhook for failed runs and non-converged discovery reports.
