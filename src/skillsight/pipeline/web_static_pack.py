"""Generate a static web data pack from snapshot exports."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import UTC, date, datetime
from math import ceil
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from prefect import flow

from skillsight.models.skill import SkillMetrics, SkillRecord
from skillsight.storage.jsonl import read_jsonl

if TYPE_CHECKING:
    from skillsight.settings import Settings

SortMode = Literal["installs", "weekly", "name"]
SORT_MODES: tuple[SortMode, ...] = ("installs", "weekly", "name")
DEFAULT_PAGE_SIZE = 12
DEFAULT_EXPORT_PREFIX = "data/v1"
LOCAL_WEB_PACK_DIRNAME = "web_data"


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(",", ":"), default=str))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _to_list_item(record: SkillRecord) -> dict[str, Any]:
    return {
        "id": record.id,
        "skill_id": record.skill_id,
        "owner": record.owner,
        "repo": record.repo,
        "name": record.name,
        "canonical_url": str(record.canonical_url),
        "total_installs": record.total_installs,
        "weekly_installs": record.weekly_installs,
        "rank_at_fetch": record.rank_at_fetch,
        "description": record.description,
        "platform_installs": record.platform_installs.model_dump(mode="json")
        if record.platform_installs is not None
        else None,
    }


def _to_search_index_item(record: SkillRecord) -> dict[str, Any]:
    return {
        "id": record.id,
        "skill_id": record.skill_id,
        "owner": record.owner,
        "repo": record.repo,
        "name": record.name,
        "canonical_url": str(record.canonical_url),
        "total_installs": record.total_installs,
        "weekly_installs": record.weekly_installs,
        "rank_at_fetch": record.rank_at_fetch,
    }


def _sort_list_items(items: list[dict[str, Any]], sort: SortMode) -> list[dict[str, Any]]:
    if sort == "name":
        return sorted(items, key=lambda item: str(item.get("name") or "").lower())
    if sort == "weekly":
        return sorted(items, key=lambda item: int(item.get("weekly_installs") or 0), reverse=True)
    return sorted(items, key=lambda item: int(item.get("total_installs") or 0), reverse=True)


def _snapshot_dirs(snapshots_root: Path) -> list[Path]:
    if not snapshots_root.exists():
        return []
    dirs: list[Path] = []
    for child in snapshots_root.iterdir():
        if not child.is_dir():
            continue
        try:
            date.fromisoformat(child.name)
        except ValueError:
            continue
        dirs.append(child)
    dirs.sort(key=lambda p: p.name)
    return dirs


def _build_metrics_history(output_dir: Path, current_ids: set[str]) -> dict[str, list[dict[str, Any]]]:
    history: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for snapshot_dir in _snapshot_dirs(output_dir / "snapshots"):
        metrics_path = snapshot_dir / "metrics.jsonl"
        if not metrics_path.exists():
            continue
        for row in read_jsonl(metrics_path):
            metric = SkillMetrics.model_validate(row)
            if metric.id not in current_ids:
                continue
            history[metric.id].append(metric.model_dump(mode="json"))
    for rows in history.values():
        rows.sort(key=lambda item: str(item.get("snapshot_date") or ""))
    return history


def _build_summary(records: list[SkillRecord], snapshot_date: date) -> dict[str, Any]:
    repos = {f"{record.owner}/{record.repo}" for record in records}
    return {
        "total_skills": len(records),
        "total_repos": len(repos),
        "snapshot_date": snapshot_date.isoformat(),
    }


def _page_filename(page: int) -> str:
    return f"page-{page:04d}.json"


def build_web_static_pack(
    output_dir: Path,
    *,
    snapshot_date: date,
    page_size: int = DEFAULT_PAGE_SIZE,
    export_prefix: str = DEFAULT_EXPORT_PREFIX,
) -> dict[str, Any]:
    """Build static web data files for one snapshot date."""

    snapshot_dir = output_dir / "snapshots" / snapshot_date.isoformat()
    skills_path = snapshot_dir / "skills_full.jsonl"
    if not skills_path.exists():
        raise FileNotFoundError(f"Missing snapshot skills export: {skills_path}")

    local_root = output_dir / LOCAL_WEB_PACK_DIRNAME / Path(export_prefix)
    snapshot_root = local_root / "snapshots" / snapshot_date.isoformat()

    records = [SkillRecord.model_validate(row) for row in read_jsonl(skills_path)]
    list_items = [_to_list_item(record) for record in records]
    current_ids = {record.id for record in records}

    summary = _build_summary(records, snapshot_date)
    summary_path = snapshot_root / "stats" / "summary.json"
    _json_dump(summary_path, summary)

    # Leaderboard pages for each sort mode
    for sort in SORT_MODES:
        sorted_items = _sort_list_items(list_items, sort)
        total = len(sorted_items)
        page_count = max(1, ceil(total / page_size)) if page_size > 0 else 1
        for page in range(1, page_count + 1):
            start = (page - 1) * page_size
            payload = {
                "snapshot_date": snapshot_date.isoformat(),
                "sort": sort,
                "page": page,
                "page_size": page_size,
                "total": total,
                "items": sorted_items[start : start + page_size],
            }
            _json_dump(snapshot_root / "leaderboard" / sort / _page_filename(page), payload)

    # Skill detail files
    for record in records:
        _json_dump(
            snapshot_root / "skills" / "by-id" / record.owner / record.repo / f"{record.skill_id}.json",
            record.model_dump(mode="json"),
        )

    # Metrics history files (for current ids only)
    metrics_by_id = _build_metrics_history(output_dir, current_ids)
    for record in records:
        _json_dump(
            snapshot_root / "metrics" / "by-id" / record.owner / record.repo / f"{record.skill_id}.json",
            {
                "id": record.id,
                "items": metrics_by_id.get(record.id, []),
            },
        )

    # Slim search index for tiny Worker search endpoint
    slim_index_path = snapshot_root / "search" / "slim-index.json"
    search_index_items = [_to_search_index_item(record) for record in records]
    _json_dump(
        slim_index_path,
        {
            "snapshot_date": snapshot_date.isoformat(),
            "items": search_index_items,
        },
    )

    # latest.json manifest (small, stable metadata + key checksums)
    checksums = {
        f"/{export_prefix}/snapshots/{snapshot_date.isoformat()}/stats/summary.json": _sha256_file(summary_path),
        f"/{export_prefix}/snapshots/{snapshot_date.isoformat()}/search/slim-index.json": _sha256_file(slim_index_path),
    }
    for sort in SORT_MODES:
        page1 = snapshot_root / "leaderboard" / sort / _page_filename(1)
        if page1.exists():
            checksums[f"/{export_prefix}/snapshots/{snapshot_date.isoformat()}/leaderboard/{sort}/{page1.name}"] = (
                _sha256_file(page1)
            )

    generated_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    manifest = {
        "format_version": 1,
        "snapshot_date": snapshot_date.isoformat(),
        "generated_at": generated_at,
        "page_size": page_size,
        "sort_modes": list(SORT_MODES),
        "counts": {
            "total_skills": summary["total_skills"],
            "total_repos": summary["total_repos"],
        },
        "paths": {
            "stats_summary": f"/{export_prefix}/snapshots/{snapshot_date.isoformat()}/stats/summary.json",
            "leaderboard_page_template": f"/{export_prefix}/snapshots/{snapshot_date.isoformat()}/leaderboard/{{sort}}/page-{{page_zero_padded_4}}.json",
            "skill_detail_template": f"/{export_prefix}/snapshots/{snapshot_date.isoformat()}/skills/by-id/{{owner}}/{{repo}}/{{skill_id}}.json",
            "metrics_template": f"/{export_prefix}/snapshots/{snapshot_date.isoformat()}/metrics/by-id/{{owner}}/{{repo}}/{{skill_id}}.json",
            "search_slim_index": f"/{export_prefix}/snapshots/{snapshot_date.isoformat()}/search/slim-index.json",
        },
        "checksums": checksums,
    }
    latest_path = local_root / "latest.json"
    _json_dump(latest_path, manifest)

    return {
        "snapshot_date": snapshot_date.isoformat(),
        "local_root": str(local_root),
        "manifest_path": str(latest_path),
        "snapshot_root": str(snapshot_root),
        "total_skills": len(records),
        "total_repos": summary["total_repos"],
        "page_size": page_size,
    }


@flow(name="skillsight-web-static-pack")
def web_static_pack_flow(
    settings: Settings,
    *,
    snapshot_date: date | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    export_prefix: str = DEFAULT_EXPORT_PREFIX,
) -> dict[str, Any]:
    """Prefect flow wrapper for static web-pack generation."""

    return build_web_static_pack(
        settings.output_dir,
        snapshot_date=snapshot_date or date.today(),
        page_size=page_size,
        export_prefix=export_prefix,
    )
