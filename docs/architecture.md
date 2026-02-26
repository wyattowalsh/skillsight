# Skillsight v0 Architecture

## Pipeline

1. Discovery uses `GET /api/skills/all-time/{page}` passes until convergence.
2. Repo pages `/{owner}/{repo}` expand hidden skill IDs.
3. Detail extraction reads structured fields from skill pages.
4. Validation and export produce daily JSONL + Parquet snapshots.
5. Optional R2 upload publishes snapshot artifacts.

## Convergence gates

- `repos_stable_for >= 2` consecutive passes.
- `new_ids_growth_pct <= 0.1`.
- `passes_max` hard stop, then search fallback if needed.

## Contracts

Frozen Worker API contract lives in `/contracts/worker_openapi.json`.
Frontend mock payloads live in `/contracts/fixtures/v1`.
