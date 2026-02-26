"""Tests for timeseries flow module."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING
from unittest.mock import patch

from skillsight.models.skill import SkillMetrics

if TYPE_CHECKING:
    from pathlib import Path
from skillsight.pipeline.timeseries_flow import (
    append_discovery_log,
    compute_deltas,
    detect_anomalies,
    timeseries_flow,
)
from skillsight.storage.parquet import write_metrics_parquet


def _write_metrics(path: Path, metrics: list[SkillMetrics]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_metrics_parquet(path, metrics)


def test_timeseries_flow_no_current_metrics(tmp_path: Path) -> None:
    result = timeseries_flow.fn(tmp_path, date(2025, 1, 15))
    assert result["status"] == "no_current_metrics"


def test_timeseries_flow_no_previous_snapshot(tmp_path: Path) -> None:
    curr_metrics = [
        SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 15), total_installs=100),
    ]
    _write_metrics(tmp_path / "snapshots" / "2025-01-15" / "metrics.parquet", curr_metrics)

    result = timeseries_flow.fn(tmp_path, date(2025, 1, 15))
    assert result["status"] == "no_previous_snapshot"


def test_timeseries_flow_previous_metrics_missing(tmp_path: Path) -> None:
    curr_metrics = [
        SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 15), total_installs=100),
    ]
    _write_metrics(tmp_path / "snapshots" / "2025-01-15" / "metrics.parquet", curr_metrics)
    (tmp_path / "snapshots" / "2025-01-14").mkdir(parents=True)

    result = timeseries_flow.fn(tmp_path, date(2025, 1, 15))
    assert result["status"] == "previous_metrics_missing"


def test_timeseries_flow_full(tmp_path: Path) -> None:
    prev_metrics = [
        SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 14), total_installs=100),
        SkillMetrics(id="o/r/b", snapshot_date=date(2025, 1, 14), total_installs=200),
    ]
    curr_metrics = [
        SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 15), total_installs=150),
        SkillMetrics(id="o/r/b", snapshot_date=date(2025, 1, 15), total_installs=250),
        SkillMetrics(id="o/r/c", snapshot_date=date(2025, 1, 15), total_installs=50),
    ]
    _write_metrics(tmp_path / "snapshots" / "2025-01-14" / "metrics.parquet", prev_metrics)
    _write_metrics(tmp_path / "snapshots" / "2025-01-15" / "metrics.parquet", curr_metrics)

    # Patch the Prefect @task-decorated functions so they call .fn directly
    with (
        patch("skillsight.pipeline.timeseries_flow.compute_deltas", side_effect=compute_deltas.fn),
        patch("skillsight.pipeline.timeseries_flow.detect_anomalies", side_effect=detect_anomalies.fn),
        patch("skillsight.pipeline.timeseries_flow.append_discovery_log", side_effect=append_discovery_log.fn),
    ):
        result = timeseries_flow.fn(tmp_path, date(2025, 1, 15))

    assert result["current_date"] == "2025-01-15"
    assert result["previous_date"] == "2025-01-14"
    assert result["total_skills_compared"] == 3
    assert result["newly_discovered"] >= 1

    summary_path = tmp_path / "timeseries" / "summary_2025-01-15.json"
    assert summary_path.exists()


def test_timeseries_flow_explicit_previous_date(tmp_path: Path) -> None:
    prev_metrics = [
        SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 10), total_installs=100),
    ]
    curr_metrics = [
        SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 15), total_installs=200),
    ]
    _write_metrics(tmp_path / "snapshots" / "2025-01-10" / "metrics.parquet", prev_metrics)
    _write_metrics(tmp_path / "snapshots" / "2025-01-15" / "metrics.parquet", curr_metrics)

    with (
        patch("skillsight.pipeline.timeseries_flow.compute_deltas", side_effect=compute_deltas.fn),
        patch("skillsight.pipeline.timeseries_flow.detect_anomalies", side_effect=detect_anomalies.fn),
        patch("skillsight.pipeline.timeseries_flow.append_discovery_log", side_effect=append_discovery_log.fn),
    ):
        result = timeseries_flow.fn(tmp_path, date(2025, 1, 15), previous_date=date(2025, 1, 10))

    assert result["previous_date"] == "2025-01-10"
    assert result["total_skills_compared"] == 1


def test_timeseries_flow_auto_detect_previous(tmp_path: Path) -> None:
    """Auto-detects most recent previous snapshot."""
    prev1 = [SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 10), total_installs=50)]
    prev2 = [SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 12), total_installs=80)]
    curr = [SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 15), total_installs=100)]

    _write_metrics(tmp_path / "snapshots" / "2025-01-10" / "metrics.parquet", prev1)
    _write_metrics(tmp_path / "snapshots" / "2025-01-12" / "metrics.parquet", prev2)
    _write_metrics(tmp_path / "snapshots" / "2025-01-15" / "metrics.parquet", curr)

    with (
        patch("skillsight.pipeline.timeseries_flow.compute_deltas", side_effect=compute_deltas.fn),
        patch("skillsight.pipeline.timeseries_flow.detect_anomalies", side_effect=detect_anomalies.fn),
        patch("skillsight.pipeline.timeseries_flow.append_discovery_log", side_effect=append_discovery_log.fn),
    ):
        result = timeseries_flow.fn(tmp_path, date(2025, 1, 15))

    assert result["previous_date"] == "2025-01-12"


def test_timeseries_flow_with_anomalies(tmp_path: Path) -> None:
    """Test that anomaly files are written when anomalies detected."""
    # Create data with a big outlier
    prev_metrics = [
        SkillMetrics(id=f"o/r/s{i}", snapshot_date=date(2025, 1, 14), total_installs=100) for i in range(20)
    ]
    curr_metrics = [
        SkillMetrics(id=f"o/r/s{i}", snapshot_date=date(2025, 1, 15), total_installs=110) for i in range(20)
    ]
    # Add an anomalous skill
    curr_metrics.append(SkillMetrics(id="o/r/outlier", snapshot_date=date(2025, 1, 15), total_installs=100000))

    _write_metrics(tmp_path / "snapshots" / "2025-01-14" / "metrics.parquet", prev_metrics)
    _write_metrics(tmp_path / "snapshots" / "2025-01-15" / "metrics.parquet", curr_metrics)

    with (
        patch("skillsight.pipeline.timeseries_flow.compute_deltas", side_effect=compute_deltas.fn),
        patch("skillsight.pipeline.timeseries_flow.detect_anomalies", side_effect=detect_anomalies.fn),
        patch("skillsight.pipeline.timeseries_flow.append_discovery_log", side_effect=append_discovery_log.fn),
    ):
        result = timeseries_flow.fn(tmp_path, date(2025, 1, 15))

    assert result["anomalies_detected"] >= 1
    anomaly_path = tmp_path / "timeseries" / "anomalies_2025-01-15.json"
    assert anomaly_path.exists()
