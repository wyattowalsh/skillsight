"""Tests for completeness module."""

from __future__ import annotations

import json
from datetime import date
from typing import TYPE_CHECKING

from skillsight.storage.completeness import _snapshot_dates, compare_with_previous_snapshot

if TYPE_CHECKING:
    from pathlib import Path
from skillsight.storage.jsonl import write_jsonl


def test_snapshot_dates_empty(tmp_path: Path) -> None:
    result = _snapshot_dates(tmp_path / "nonexistent")
    assert result == []


def test_snapshot_dates_finds_dates(tmp_path: Path) -> None:
    snapshots = tmp_path / "snapshots"
    (snapshots / "2025-01-10").mkdir(parents=True)
    (snapshots / "2025-01-15").mkdir(parents=True)
    (snapshots / "2025-01-12").mkdir(parents=True)
    (snapshots / "not-a-date").mkdir(parents=True)
    result = _snapshot_dates(snapshots)
    assert len(result) == 3
    assert result[0] == date(2025, 1, 10)
    assert result[-1] == date(2025, 1, 15)


def test_snapshot_dates_ignores_files(tmp_path: Path) -> None:
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir(parents=True)
    (snapshots / "2025-01-10").mkdir()
    (snapshots / "readme.txt").write_text("hello")
    result = _snapshot_dates(snapshots)
    assert len(result) == 1


def test_compare_with_previous_snapshot_no_baseline(tmp_path: Path) -> None:
    report = compare_with_previous_snapshot(tmp_path, date(2026, 2, 17), current_count=5)
    assert report["status"] == "no_baseline"
    assert report["previous_snapshot_date"] is None
    assert report["previous_parse_errors"] is None


def test_compare_with_previous_snapshot_ok(tmp_path: Path) -> None:
    output_dir = tmp_path
    prev_dir = output_dir / "snapshots" / "2025-01-10"
    prev_dir.mkdir(parents=True)
    records = [json.dumps({"id": f"o/r/s{i}"}) for i in range(80)]
    (prev_dir / "skills_full.jsonl").write_text("\n".join(records) + "\n")

    result = compare_with_previous_snapshot(output_dir, date(2025, 1, 15), 100)
    assert result["status"] == "ok"
    assert result["previous_count"] == 80
    assert result["previous_parse_errors"] == 0
    assert result["current_count"] == 100
    assert result["delta"] == 20


def test_compare_with_previous_snapshot_degraded_when_previous_has_parse_errors(tmp_path: Path) -> None:
    output_dir = tmp_path
    prev_dir = output_dir / "snapshots" / "2025-01-10"
    prev_dir.mkdir(parents=True)
    (prev_dir / "skills_full.jsonl").write_text('{"id":"a"}\n{"id":"b"}\ncorrupt\n')

    result = compare_with_previous_snapshot(output_dir, date(2025, 1, 15), 5)
    assert result["previous_count"] == 2
    assert result["previous_parse_errors"] == 1
    assert result["status"] == "degraded"


def test_compare_with_previous_snapshot_detects_regression(tmp_path: Path) -> None:
    output_dir = tmp_path
    prev_dir = output_dir / "snapshots" / "2026-02-16"
    curr_dir = output_dir / "snapshots" / "2026-02-17"

    write_jsonl(prev_dir / "skills_full.jsonl", [{"id": "a"}, {"id": "b"}, {"id": "c"}])
    write_jsonl(curr_dir / "skills_full.jsonl", [{"id": "a"}])

    report = compare_with_previous_snapshot(output_dir, date(2026, 2, 17), current_count=1)
    assert report["previous_snapshot_date"] == "2026-02-16"
    assert report["delta"] == -2
    assert report["status"] == "regression"


def test_compare_picks_most_recent_previous(tmp_path: Path) -> None:
    output_dir = tmp_path
    # Create two prior snapshots
    for d in ["2025-01-05", "2025-01-10"]:
        dir_path = output_dir / "snapshots" / d
        dir_path.mkdir(parents=True)
        write_jsonl(dir_path / "skills_full.jsonl", [{"id": "x"}])

    result = compare_with_previous_snapshot(output_dir, date(2025, 1, 15), 5)
    assert result["previous_snapshot_date"] == "2025-01-10"
