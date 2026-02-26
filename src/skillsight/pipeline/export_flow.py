"""Export and upload flow."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from prefect import flow

from skillsight.clients.r2 import can_upload, upload_bytes, upload_file
from skillsight.pipeline.web_static_pack import LOCAL_WEB_PACK_DIRNAME

if TYPE_CHECKING:
    from skillsight.settings import Settings


def _web_pack_paths(settings: Settings) -> tuple[Path, Path]:
    base = settings.output_dir / LOCAL_WEB_PACK_DIRNAME
    root = base / Path(settings.web_export_prefix)
    return base, root


def _iter_web_pack_files(settings: Settings) -> list[Path]:
    base, root = _web_pack_paths(settings)
    if not root.exists():
        return []
    return sorted([p for p in base.rglob("*") if p.is_file()])


@flow(name="skillsight-export")
def export_flow(
    settings: Settings,
    *,
    upload_r2: bool = False,
    snapshot_date: date | None = None,
    publish_latest: bool | None = None,
) -> dict[str, str]:
    """Export artifact paths and optionally upload to R2."""

    today = date.today()
    target_date = snapshot_date or today
    is_backfill = target_date != today
    should_publish_latest = publish_latest if publish_latest is not None else (not is_backfill)
    snapshot_dir = settings.output_dir / "snapshots" / target_date.isoformat()
    artifacts = {
        "skills_jsonl": str(snapshot_dir / "skills_full.jsonl"),
        "skills_parquet": str(snapshot_dir / "skills_full.parquet"),
        "metrics_jsonl": str(snapshot_dir / "metrics.jsonl"),
        "metrics_parquet": str(snapshot_dir / "metrics.parquet"),
    }

    # Add SQLite if it exists
    sqlite_path = snapshot_dir / "skills.db"
    if sqlite_path.exists():
        artifacts["skills_sqlite"] = str(sqlite_path)

    _web_base, web_root = _web_pack_paths(settings)
    web_manifest = web_root / "latest.json"
    if web_manifest.exists():
        artifacts["web_manifest"] = str(web_manifest)
        artifacts["web_static_root"] = str(web_root)

    if upload_r2:
        if not can_upload(settings):
            raise RuntimeError("Cannot upload to R2: credentials missing.")
        uploads: dict[str, str] = {}
        for _, path in artifacts.items():
            local_path = Path(path)
            if not local_path.exists():
                continue
            if local_path == web_root:
                continue
            key = f"{settings.r2_prefix}/{target_date.isoformat()}/{local_path.name}"
            if local_path == web_manifest:
                if not should_publish_latest:
                    continue
                key = f"{settings.web_export_prefix}/latest.json"
            uploads[local_path.name] = upload_file(settings, local_path, key)

        # Upload static web pack recursively when present.
        for web_file in _iter_web_pack_files(settings):
            rel = web_file.relative_to(_web_base).as_posix()
            if rel == f"{settings.web_export_prefix}/latest.json" and not should_publish_latest:
                continue
            # latest.json already uploaded under the same key above if present; skip duplicate.
            if rel == f"{settings.web_export_prefix}/latest.json" and web_manifest.exists():
                continue
            uploads[rel] = upload_file(settings, web_file, rel)

        if should_publish_latest:
            latest_payload = json.dumps({"date": target_date.isoformat()}).encode()
            upload_bytes(settings, latest_payload, f"{settings.r2_prefix}/latest.json")
        return uploads

    return artifacts
