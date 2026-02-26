# Skillsight Worker

Cloudflare Worker runtime for the tiny search API.

## Primary endpoints (active contract)

- `GET /v1/search`
- `GET /healthz`

Legacy `/v1/skills*`, `/v1/stats/summary`, and `/v1/metrics/*` endpoints may still be present in
transition deployments, but `skillsight contract` now defaults to the tiny search API contract.

The Worker reads `data/v1/latest.json` and `data/v1/snapshots/<date>/search/slim-index.json`
from the `SKILLSIGHT_DATA` R2 bucket binding and keeps the parsed search index cached in
memory for a short TTL.

The slim search index is an internal Worker artifact (optimized search projection), not the full
skill-detail payload contract.

## Contract inspection

```bash
uv run skillsight contract
uv run skillsight contract --surface legacy
uv run skillsight contract --surface all
```

`skillsight contract` defaults to the active tiny search Worker contract.

## Deploy

```bash
cd worker
npx wrangler deploy
```
