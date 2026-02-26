"""Tests for dataset mirror publishing scaffolding."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from skillsight.pipeline.publish_datasets import publish_datasets
from skillsight.settings import Settings


def _write_snapshot_files(snapshot_dir: Path) -> None:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    for name in ("skills_full.jsonl", "skills_full.parquet", "metrics.jsonl", "metrics.parquet"):
        (snapshot_dir / name).write_text(f"{name}\n")


def test_publish_datasets_writes_bundle_and_report(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "snapshots" / "2025-01-15"
    _write_snapshot_files(snapshot_dir)
    settings = Settings(output_dir=tmp_path)

    result = publish_datasets(settings, snapshot_date=date(2025, 1, 15))

    assert result["snapshot_date"] == "2025-01-15"
    assert result["github_release"]["status"] == "disabled"
    assert result["kaggle"]["status"] == "disabled"
    manifest_path = Path(result["bundle"]["manifest_path"])
    checksums_path = Path(result["bundle"]["checksums_path"])
    report_path = Path(result["report_path"])
    assert manifest_path.exists()
    assert checksums_path.exists()
    assert report_path.exists()

    manifest = json.loads(manifest_path.read_text())
    assert manifest["snapshot_date"] == "2025-01-15"
    assert len(manifest["files"]) == 4


def test_publish_datasets_reports_config_errors_when_enabled_without_targets(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "snapshots" / "2025-01-15"
    _write_snapshot_files(snapshot_dir)
    settings = Settings(output_dir=tmp_path, github_release_enabled=True, kaggle_publish_enabled=True)

    result = publish_datasets(settings, snapshot_date=date(2025, 1, 15))

    assert result["github_release"]["status"] == "error"
    assert result["kaggle"]["status"] == "error"
