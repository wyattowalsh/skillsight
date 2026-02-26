"""Tests for DuckDB query module."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from skillsight.models.skill import SkillMetrics, SkillRecord

if TYPE_CHECKING:
    from pathlib import Path
from skillsight.storage.duckdb_query import (
    duckdb_connection,
    run_dataset_stats,
    run_diff_query,
    run_stats_query,
    run_timeseries_delta,
)
from skillsight.storage.parquet import write_metrics_parquet, write_skills_parquet


def _make_record(**overrides) -> SkillRecord:
    defaults = {
        "id": "o/r/s",
        "skill_id": "s",
        "owner": "o",
        "repo": "r",
        "canonical_url": "https://skills.sh/o/r/s",
        "name": "Test Skill",
        "total_installs": 1000,
        "description": "A test skill",
        "run_id": "run-1",
        "fetched_at": datetime.now(UTC),
        "discovery_source": "search_api",
        "source_endpoint": "search_api",
    }
    defaults.update(overrides)
    return SkillRecord(**defaults)


def test_run_stats_query(tmp_path: Path) -> None:
    metrics_path = tmp_path / "metrics.parquet"
    metrics = [
        SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 15), total_installs=100),
        SkillMetrics(id="o/r/b", snapshot_date=date(2025, 1, 15), total_installs=200),
    ]
    write_metrics_parquet(metrics_path, metrics)
    results = run_stats_query(metrics_path)
    assert len(results) == 1
    assert results[0][1] == 2


def test_run_dataset_stats(tmp_path: Path) -> None:
    skills_path = tmp_path / "skills.parquet"
    records = [
        _make_record(id="o/r/a", name="A", description="Desc A", github_url="https://github.com/o/r"),
        _make_record(id="o/r/b", name="B", description=None, total_installs=None),
        _make_record(id="o2/r2/c", name="C", owner="o2", repo="r2"),
    ]
    write_skills_parquet(skills_path, records)
    result = run_dataset_stats(skills_path)
    assert result["total"] == 3
    assert result["has_name"] == 3
    assert result["unique_repos"] == 2
    assert result["unique_owners"] == 2
    assert "name_pct" in result


def test_run_diff_query(tmp_path: Path) -> None:
    skills_a = tmp_path / "skills_a.parquet"
    skills_b = tmp_path / "skills_b.parquet"
    records_a = [_make_record(id="o/r/a"), _make_record(id="o/r/b")]
    records_b = [_make_record(id="o/r/b"), _make_record(id="o/r/c")]
    write_skills_parquet(skills_a, records_a)
    write_skills_parquet(skills_b, records_b)

    result = run_diff_query(skills_a, skills_b)
    assert result["count_a"] == 2
    assert result["count_b"] == 2
    assert result["new_in_b"] == 1  # c is new
    assert result["removed_from_a"] == 1  # a was removed


def test_run_timeseries_delta(tmp_path: Path) -> None:
    prev_path = tmp_path / "prev_metrics.parquet"
    curr_path = tmp_path / "curr_metrics.parquet"
    prev_metrics = [
        SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 14), total_installs=100),
        SkillMetrics(id="o/r/b", snapshot_date=date(2025, 1, 14), total_installs=200),
    ]
    curr_metrics = [
        SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 15), total_installs=150),
        SkillMetrics(id="o/r/b", snapshot_date=date(2025, 1, 15), total_installs=250),
        SkillMetrics(id="o/r/c", snapshot_date=date(2025, 1, 15), total_installs=50),
    ]
    write_metrics_parquet(prev_path, prev_metrics)
    write_metrics_parquet(curr_path, curr_metrics)

    deltas = run_timeseries_delta(prev_path, curr_path)
    assert len(deltas) == 3
    ids = {d["id"] for d in deltas}
    assert "o/r/a" in ids
    assert "o/r/c" in ids
    # Verify delta for skill a
    a_delta = next(d for d in deltas if d["id"] == "o/r/a")
    assert a_delta["delta"] == 50
    assert a_delta["prev_installs"] == 100
    assert a_delta["curr_installs"] == 150


def test_run_stats_query_with_conn(tmp_path: Path) -> None:
    """Exercise the conn is not None branch of run_stats_query."""
    metrics_path = tmp_path / "metrics.parquet"
    metrics = [
        SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 15), total_installs=100),
        SkillMetrics(id="o/r/b", snapshot_date=date(2025, 1, 15), total_installs=200),
    ]
    write_metrics_parquet(metrics_path, metrics)
    with duckdb_connection() as conn:
        results = run_stats_query(metrics_path, conn=conn)
    assert len(results) == 1
    assert results[0][1] == 2


def test_run_dataset_stats_with_conn(tmp_path: Path) -> None:
    """Exercise the conn is not None branch of run_dataset_stats."""
    skills_path = tmp_path / "skills.parquet"
    records = [
        _make_record(id="o/r/a", name="A", description="Desc A", github_url="https://github.com/o/r"),
        _make_record(id="o/r/b", name="B", description=None, total_installs=None),
        _make_record(id="o2/r2/c", name="C", owner="o2", repo="r2"),
    ]
    write_skills_parquet(skills_path, records)
    with duckdb_connection() as conn:
        result = run_dataset_stats(skills_path, conn=conn)
    assert result["total"] == 3
    assert result["has_name"] == 3


def test_run_diff_query_with_conn(tmp_path: Path) -> None:
    """Exercise the conn is not None branch of run_diff_query."""
    skills_a = tmp_path / "skills_a.parquet"
    skills_b = tmp_path / "skills_b.parquet"
    records_a = [_make_record(id="o/r/a"), _make_record(id="o/r/b")]
    records_b = [_make_record(id="o/r/b"), _make_record(id="o/r/c")]
    write_skills_parquet(skills_a, records_a)
    write_skills_parquet(skills_b, records_b)
    with duckdb_connection() as conn:
        result = run_diff_query(skills_a, skills_b, conn=conn)
    assert result["count_a"] == 2
    assert result["count_b"] == 2
    assert result["new_in_b"] == 1
    assert result["removed_from_a"] == 1


def test_run_timeseries_delta_with_conn(tmp_path: Path) -> None:
    """Exercise the conn is not None branch of run_timeseries_delta."""
    prev_path = tmp_path / "prev_metrics.parquet"
    curr_path = tmp_path / "curr_metrics.parquet"
    prev_metrics = [
        SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 14), total_installs=100),
    ]
    curr_metrics = [
        SkillMetrics(id="o/r/a", snapshot_date=date(2025, 1, 15), total_installs=150),
    ]
    write_metrics_parquet(prev_path, prev_metrics)
    write_metrics_parquet(curr_path, curr_metrics)
    with duckdb_connection() as conn:
        deltas = run_timeseries_delta(prev_path, curr_path, conn=conn)
    assert len(deltas) == 1
    assert deltas[0]["delta"] == 50
