"""Tests for export flow module."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING
from unittest.mock import patch

from skillsight.pipeline.export_flow import export_flow

if TYPE_CHECKING:
    from pathlib import Path
from skillsight.settings import Settings


def test_export_flow_no_r2(tmp_path: Path) -> None:
    settings = Settings(output_dir=tmp_path)
    with patch("skillsight.pipeline.export_flow.date") as mock_date:
        mock_date.today.return_value = date(2025, 1, 15)
        result = export_flow.fn(settings, upload_r2=False)

    assert "skills_jsonl" in result
    assert "skills_parquet" in result
    assert "metrics_jsonl" in result
    assert "metrics_parquet" in result
    assert "2025-01-15" in result["skills_jsonl"]


def test_export_flow_includes_sqlite(tmp_path: Path) -> None:
    settings = Settings(output_dir=tmp_path)
    snapshot_dir = tmp_path / "snapshots" / date(2025, 1, 15).isoformat()
    snapshot_dir.mkdir(parents=True)
    (snapshot_dir / "skills.db").write_text("dummy")

    with patch("skillsight.pipeline.export_flow.date") as mock_date:
        mock_date.today.return_value = date(2025, 1, 15)
        result = export_flow.fn(settings, upload_r2=False)

    assert "skills_sqlite" in result


def test_export_flow_r2_no_credentials(tmp_path: Path) -> None:
    settings = Settings(output_dir=tmp_path)
    import pytest

    with (
        pytest.raises(RuntimeError, match="Cannot upload to R2"),
        patch("skillsight.pipeline.export_flow.date") as mock_date,
    ):
        mock_date.today.return_value = date(2025, 1, 15)
        export_flow.fn(settings, upload_r2=True)


def test_export_flow_includes_web_pack_when_present(tmp_path: Path) -> None:
    settings = Settings(output_dir=tmp_path)
    snapshot_dir = tmp_path / "snapshots" / "2025-01-15"
    snapshot_dir.mkdir(parents=True)
    (snapshot_dir / "skills_full.jsonl").write_text("")
    (snapshot_dir / "skills_full.parquet").write_text("")
    (snapshot_dir / "metrics.jsonl").write_text("")
    (snapshot_dir / "metrics.parquet").write_text("")

    web_root = tmp_path / "web_data" / "data" / "v1"
    web_snapshot = web_root / "snapshots" / "2025-01-15" / "stats"
    web_snapshot.mkdir(parents=True, exist_ok=True)
    (web_root / "latest.json").write_text("{}")
    (web_snapshot / "summary.json").write_text("{}")

    with patch("skillsight.pipeline.export_flow.date") as mock_date:
        mock_date.today.return_value = date(2025, 1, 15)
        result = export_flow.fn(settings, upload_r2=False)

    assert "web_manifest" in result
    assert result["web_manifest"].endswith("web_data/data/v1/latest.json")


def test_export_flow_uploads_web_pack_files_when_present(tmp_path: Path) -> None:
    settings = Settings(output_dir=tmp_path, r2_endpoint_url="https://r2.example.com")
    snapshot_dir = tmp_path / "snapshots" / "2025-01-15"
    snapshot_dir.mkdir(parents=True)
    for name in ("skills_full.jsonl", "skills_full.parquet", "metrics.jsonl", "metrics.parquet"):
        (snapshot_dir / name).write_text("x")

    web_root = tmp_path / "web_data" / "data" / "v1"
    web_summary = web_root / "snapshots" / "2025-01-15" / "stats"
    web_summary.mkdir(parents=True, exist_ok=True)
    (web_root / "latest.json").write_text("{}")
    (web_summary / "summary.json").write_text("{}")

    uploaded: list[tuple[str, str]] = []

    def _capture_upload(_settings: Settings, local_path: Path, key: str) -> str:
        uploaded.append((local_path.name, key))
        return f"s3://{_settings.r2_bucket_name}/{key}"

    latest_marker_calls: list[tuple[bytes, str]] = []

    def _capture_upload_bytes(_settings: Settings, payload: bytes, key: str) -> str:
        latest_marker_calls.append((payload, key))
        return f"s3://{_settings.r2_bucket_name}/{key}"

    with (
        patch("skillsight.pipeline.export_flow.date") as mock_date,
        patch("skillsight.pipeline.export_flow.can_upload", return_value=True),
        patch("skillsight.pipeline.export_flow.upload_file", side_effect=_capture_upload),
        patch("skillsight.pipeline.export_flow.upload_bytes", side_effect=_capture_upload_bytes),
    ):
        mock_date.today.return_value = date(2025, 1, 15)
        export_flow.fn(settings, upload_r2=True)

    keys = {key for _, key in uploaded}
    assert "data/v1/latest.json" in keys
    assert "data/v1/snapshots/2025-01-15/stats/summary.json" in keys
    assert latest_marker_calls
    assert latest_marker_calls[0][1] == "snapshots/latest.json"
    assert b"2025-01-15" in latest_marker_calls[0][0]


def test_export_flow_upload_uses_explicit_snapshot_date_for_keys(tmp_path: Path) -> None:
    settings = Settings(output_dir=tmp_path, r2_endpoint_url="https://r2.example.com")
    snapshot_dir = tmp_path / "snapshots" / "2025-01-14"
    snapshot_dir.mkdir(parents=True)
    for name in ("skills_full.jsonl", "skills_full.parquet", "metrics.jsonl", "metrics.parquet"):
        (snapshot_dir / name).write_text("x")

    web_root = tmp_path / "web_data" / "data" / "v1"
    web_summary = web_root / "snapshots" / "2025-01-14" / "stats"
    web_summary.mkdir(parents=True, exist_ok=True)
    (web_root / "latest.json").write_text("{}")
    (web_summary / "summary.json").write_text("{}")

    uploaded: list[tuple[str, str]] = []
    latest_markers: list[tuple[bytes, str]] = []

    def _capture_upload(_settings: Settings, local_path: Path, key: str) -> str:
        uploaded.append((local_path.name, key))
        return f"s3://{_settings.r2_bucket_name}/{key}"

    def _capture_upload_bytes(_settings: Settings, payload: bytes, key: str) -> str:
        latest_markers.append((payload, key))
        return f"s3://{_settings.r2_bucket_name}/{key}"

    with (
        patch("skillsight.pipeline.export_flow.date") as mock_date,
        patch("skillsight.pipeline.export_flow.can_upload", return_value=True),
        patch("skillsight.pipeline.export_flow.upload_file", side_effect=_capture_upload),
        patch("skillsight.pipeline.export_flow.upload_bytes", side_effect=_capture_upload_bytes),
    ):
        mock_date.today.return_value = date(2025, 1, 15)
        export_flow.fn(settings, upload_r2=True, snapshot_date=date(2025, 1, 14))

    keys = {key for _, key in uploaded}
    assert "snapshots/2025-01-14/skills_full.jsonl" in keys
    assert "data/v1/snapshots/2025-01-14/stats/summary.json" in keys
    assert "data/v1/latest.json" not in keys
    assert latest_markers == []


def test_export_flow_backfill_can_publish_latest_pointers_when_explicitly_enabled(tmp_path: Path) -> None:
    settings = Settings(output_dir=tmp_path, r2_endpoint_url="https://r2.example.com")
    snapshot_dir = tmp_path / "snapshots" / "2025-01-14"
    snapshot_dir.mkdir(parents=True)
    for name in ("skills_full.jsonl", "skills_full.parquet", "metrics.jsonl", "metrics.parquet"):
        (snapshot_dir / name).write_text("x")

    web_root = tmp_path / "web_data" / "data" / "v1"
    web_summary = web_root / "snapshots" / "2025-01-14" / "stats"
    web_summary.mkdir(parents=True, exist_ok=True)
    (web_root / "latest.json").write_text("{}")
    (web_summary / "summary.json").write_text("{}")

    uploaded: list[tuple[str, str]] = []
    latest_markers: list[tuple[bytes, str]] = []

    def _capture_upload(_settings: Settings, local_path: Path, key: str) -> str:
        uploaded.append((local_path.name, key))
        return f"s3://{_settings.r2_bucket_name}/{key}"

    def _capture_upload_bytes(_settings: Settings, payload: bytes, key: str) -> str:
        latest_markers.append((payload, key))
        return f"s3://{_settings.r2_bucket_name}/{key}"

    with (
        patch("skillsight.pipeline.export_flow.date") as mock_date,
        patch("skillsight.pipeline.export_flow.can_upload", return_value=True),
        patch("skillsight.pipeline.export_flow.upload_file", side_effect=_capture_upload),
        patch("skillsight.pipeline.export_flow.upload_bytes", side_effect=_capture_upload_bytes),
    ):
        mock_date.today.return_value = date(2025, 1, 15)
        export_flow.fn(settings, upload_r2=True, snapshot_date=date(2025, 1, 14), publish_latest=True)

    keys = {key for _, key in uploaded}
    assert "snapshots/2025-01-14/skills_full.jsonl" in keys
    assert "data/v1/snapshots/2025-01-14/stats/summary.json" in keys
    assert "data/v1/latest.json" in keys
    assert latest_markers
    assert latest_markers[0][1] == "snapshots/latest.json"
    assert b"2025-01-14" in latest_markers[0][0]
