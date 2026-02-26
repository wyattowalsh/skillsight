"""Tests for timeseries flow module."""

from __future__ import annotations

import json
from datetime import date
from typing import TYPE_CHECKING

from skillsight.pipeline.timeseries_flow import append_discovery_log, detect_anomalies

if TYPE_CHECKING:
    from pathlib import Path


def test_detect_anomalies_empty() -> None:
    result = detect_anomalies.fn([])
    assert result == []


def test_detect_anomalies_too_few() -> None:
    deltas = [{"id": "a", "delta": 10}, {"id": "b", "delta": 20}]
    result = detect_anomalies.fn(deltas)
    assert result == []


def test_detect_anomalies_no_anomalies() -> None:
    deltas = [
        {"id": "a", "delta": 10},
        {"id": "b", "delta": 11},
        {"id": "c", "delta": 12},
        {"id": "d", "delta": 9},
    ]
    result = detect_anomalies.fn(deltas)
    assert result == []


def test_detect_anomalies_with_outlier() -> None:
    # Need enough normal values so stdev is small, making the outlier exceed 3 sigma
    deltas = [{"id": f"n{i}", "delta": 10} for i in range(20)]
    deltas.append({"id": "e", "delta": 10000})
    result = detect_anomalies.fn(deltas)
    assert len(result) >= 1
    assert any(a["id"] == "e" for a in result)
    assert "z_score" in result[0]


def test_detect_anomalies_all_same() -> None:
    deltas = [{"id": f"x{i}", "delta": 100} for i in range(5)]
    result = detect_anomalies.fn(deltas)
    # stdev == 0 means no anomalies
    assert result == []


def test_detect_anomalies_skips_zero() -> None:
    deltas = [
        {"id": "a", "delta": 0},
        {"id": "b", "delta": 10},
        {"id": "c", "delta": 11},
        {"id": "d", "delta": 12},
    ]
    result = detect_anomalies.fn(deltas)
    assert all(a["delta"] != 0 for a in result)


def test_detect_anomalies_skips_none() -> None:
    deltas = [
        {"id": "a", "delta": None},
        {"id": "b", "delta": 10},
        {"id": "c", "delta": 11},
        {"id": "d", "delta": 12},
    ]
    result = detect_anomalies.fn(deltas)
    assert all(a["delta"] is not None for a in result)


def test_append_discovery_log_no_new(tmp_path: Path) -> None:
    deltas = [{"id": "a", "prev_installs": 100, "curr_installs": 110}]
    count = append_discovery_log.fn(tmp_path, date(2025, 1, 15), deltas)
    assert count == 0


def test_append_discovery_log_with_new(tmp_path: Path) -> None:
    deltas = [
        {"id": "a", "prev_installs": None, "curr_installs": 50},
        {"id": "b", "prev_installs": None, "curr_installs": 100},
        {"id": "c", "prev_installs": 200, "curr_installs": 210},
    ]
    count = append_discovery_log.fn(tmp_path, date(2025, 1, 15), deltas)
    assert count == 2

    log_path = tmp_path / "timeseries" / "discovery_log.jsonl"
    assert log_path.exists()
    lines = [json.loads(line) for line in log_path.read_text().splitlines() if line.strip()]
    assert len(lines) == 2
    assert lines[0]["id"] == "a"
    assert lines[0]["initial_installs"] == 50
    assert lines[1]["id"] == "b"


def test_append_discovery_log_appends(tmp_path: Path) -> None:
    """Verify that discovery log appends rather than overwrites."""
    deltas1 = [{"id": "a", "prev_installs": None, "curr_installs": 50}]
    deltas2 = [{"id": "b", "prev_installs": None, "curr_installs": 100}]
    append_discovery_log.fn(tmp_path, date(2025, 1, 15), deltas1)
    append_discovery_log.fn(tmp_path, date(2025, 1, 16), deltas2)

    log_path = tmp_path / "timeseries" / "discovery_log.jsonl"
    lines = [json.loads(line) for line in log_path.read_text().splitlines() if line.strip()]
    assert len(lines) == 2
