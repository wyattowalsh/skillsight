"""Dataset mirror publishing helpers (GitHub Releases / Kaggle)."""

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from skillsight.settings import Settings


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _build_artifact_manifest(snapshot_dir: Path) -> tuple[dict[str, Any], str]:
    files: list[dict[str, Any]] = []
    for name in ("skills_full.jsonl", "skills_full.parquet", "metrics.jsonl", "metrics.parquet"):
        path = snapshot_dir / name
        if not path.exists():
            continue
        files.append(
            {
                "name": name,
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    manifest = {
        "schema_version": 1,
        "generated_at": datetime.now().isoformat(),
        "snapshot_date": snapshot_dir.name,
        "files": files,
    }
    checksums_txt = "\n".join(f'{entry["sha256"]}  {entry["name"]}' for entry in files)
    if checksums_txt:
        checksums_txt += "\n"
    return manifest, checksums_txt


def _materialize_publish_bundle(output_dir: Path, snapshot_date: date) -> dict[str, str]:
    snapshot_dir = output_dir / "snapshots" / snapshot_date.isoformat()
    if not snapshot_dir.exists():
        raise FileNotFoundError(f"Snapshot not found: {snapshot_dir}")

    bundle_dir = output_dir / "publish" / snapshot_date.isoformat()
    bundle_dir.mkdir(parents=True, exist_ok=True)

    manifest, checksums_txt = _build_artifact_manifest(snapshot_dir)
    manifest_path = bundle_dir / "manifest.json"
    checksums_path = bundle_dir / "checksums.txt"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    checksums_path.write_text(checksums_txt)

    return {
        "snapshot_dir": str(snapshot_dir),
        "bundle_dir": str(bundle_dir),
        "manifest_path": str(manifest_path),
        "checksums_path": str(checksums_path),
    }


def _github_release_status(settings: Settings, snapshot_date: date, bundle: dict[str, str]) -> dict[str, Any]:
    if not settings.github_release_enabled:
        return {"status": "disabled"}
    if not settings.github_release_repo:
        return {"status": "error", "error": "github_release_repo is required when github_release_enabled=true"}
    gh_path = shutil.which("gh")
    if gh_path is None:
        return {"status": "error", "error": "gh CLI not found", "retryable": True}
    tag = f"data-{snapshot_date.isoformat()}"
    return {
        "status": "ready",
        "tag": tag,
        "repo": settings.github_release_repo,
        "gh_path": gh_path,
        "bundle_dir": bundle["bundle_dir"],
        "note": "Publishing command scaffolding is configured; invoke gh release in deployment automation.",
    }


def _kaggle_status(settings: Settings, bundle: dict[str, str]) -> dict[str, Any]:
    if not settings.kaggle_publish_enabled:
        return {"status": "disabled"}
    if not settings.kaggle_dataset_slug:
        return {"status": "error", "error": "kaggle_dataset_slug is required when kaggle_publish_enabled=true"}
    kaggle_path = shutil.which("kaggle")
    if kaggle_path is None:
        return {"status": "error", "error": "kaggle CLI not found", "retryable": True}
    return {
        "status": "ready",
        "dataset": settings.kaggle_dataset_slug,
        "kaggle_path": kaggle_path,
        "bundle_dir": bundle["bundle_dir"],
        "note": "Publishing command scaffolding is configured; invoke kaggle dataset version in deployment automation.",
    }


def publish_datasets(settings: Settings, *, snapshot_date: date | None = None) -> dict[str, Any]:
    """Prepare dataset mirror publishing artifacts and return mirror status."""

    target_date = snapshot_date or date.today()
    bundle = _materialize_publish_bundle(settings.output_dir, target_date)
    github = _github_release_status(settings, target_date, bundle)
    kaggle = _kaggle_status(settings, bundle)

    result: dict[str, Any] = {
        "snapshot_date": target_date.isoformat(),
        "bundle": bundle,
        "github_release": github,
        "kaggle": kaggle,
    }

    reports_dir = settings.output_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / f"publish_datasets_{target_date.isoformat()}.json"
    report_path.write_text(json.dumps(result, indent=2))
    result["report_path"] = str(report_path)
    return result

