"""Tests for static web data pack generation."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from skillsight.storage.jsonl import write_jsonl
from tests.factories import make_record


def _write_skills_snapshot(snapshot_dir: Path, records: list[dict]) -> None:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(snapshot_dir / "skills_full.jsonl", records)


def _write_metrics_snapshot(snapshot_dir: Path, rows: list[dict]) -> None:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(snapshot_dir / "metrics.jsonl", rows)


def test_build_web_static_pack_generates_expected_files(tmp_path: Path) -> None:
    from skillsight.pipeline.web_static_pack import build_web_static_pack

    curr_snapshot = tmp_path / "snapshots" / "2025-01-15"
    prev_snapshot = tmp_path / "snapshots" / "2025-01-14"

    a = make_record(
        id="o/r/a",
        skill_id="a",
        name="Alpha",
        total_installs=300,
        weekly_installs=30,
        fetched_at=datetime(2025, 1, 15, 12, 0, tzinfo=UTC),
        categories=["tools"],
    )
    b = make_record(
        id="o/r/b",
        skill_id="b",
        name="Beta",
        total_installs=100,
        weekly_installs=60,
        fetched_at=datetime(2025, 1, 15, 12, 0, tzinfo=UTC),
    )
    c = make_record(
        id="o/r/c",
        skill_id="c",
        name="Gamma",
        total_installs=200,
        weekly_installs=10,
        fetched_at=datetime(2025, 1, 15, 12, 0, tzinfo=UTC),
    )
    _write_skills_snapshot(curr_snapshot, [a.model_dump(mode="json"), b.model_dump(mode="json"), c.model_dump(mode="json")])

    _write_metrics_snapshot(
        prev_snapshot,
        [
            {
                "id": "o/r/a",
                "snapshot_date": "2025-01-14",
                "total_installs": 250,
                "weekly_installs": 20,
            },
            {
                "id": "o/r/b",
                "snapshot_date": "2025-01-14",
                "total_installs": 90,
                "weekly_installs": 55,
            },
        ],
    )
    _write_metrics_snapshot(
        curr_snapshot,
        [
            {
                "id": "o/r/a",
                "snapshot_date": "2025-01-15",
                "total_installs": 300,
                "weekly_installs": 30,
            },
            {
                "id": "o/r/b",
                "snapshot_date": "2025-01-15",
                "total_installs": 100,
                "weekly_installs": 60,
            },
            {
                "id": "o/r/c",
                "snapshot_date": "2025-01-15",
                "total_installs": 200,
                "weekly_installs": 10,
            },
        ],
    )

    result = build_web_static_pack(tmp_path, snapshot_date=date(2025, 1, 15), page_size=2)

    assert result["snapshot_date"] == "2025-01-15"

    root = tmp_path / "web_data" / "data" / "v1"
    latest_path = root / "latest.json"
    assert latest_path.exists()
    manifest = json.loads(latest_path.read_text())
    assert manifest["format_version"] == 1
    assert manifest["snapshot_date"] == "2025-01-15"
    assert manifest["page_size"] == 2
    assert manifest["counts"]["total_skills"] == 3
    assert manifest["counts"]["total_repos"] == 1
    assert (
        manifest["paths"]["leaderboard_page_template"]
        == "/data/v1/snapshots/2025-01-15/leaderboard/{sort}/page-{page_zero_padded_4}.json"
    )
    assert manifest["generated_at"].endswith("Z")
    assert "checksums" in manifest

    summary = json.loads((root / "snapshots" / "2025-01-15" / "stats" / "summary.json").read_text())
    assert summary == {
        "total_skills": 3,
        "total_repos": 1,
        "snapshot_date": "2025-01-15",
    }

    installs_page1 = json.loads(
        (root / "snapshots" / "2025-01-15" / "leaderboard" / "installs" / "page-0001.json").read_text()
    )
    assert installs_page1["page"] == 1
    assert installs_page1["page_size"] == 2
    assert installs_page1["total"] == 3
    assert [item["id"] for item in installs_page1["items"]] == ["o/r/a", "o/r/c"]

    weekly_page1 = json.loads(
        (root / "snapshots" / "2025-01-15" / "leaderboard" / "weekly" / "page-0001.json").read_text()
    )
    assert [item["id"] for item in weekly_page1["items"]] == ["o/r/b", "o/r/a"]

    name_page1 = json.loads((root / "snapshots" / "2025-01-15" / "leaderboard" / "name" / "page-0001.json").read_text())
    assert [item["name"] for item in name_page1["items"]] == ["Alpha", "Beta"]

    detail = json.loads((root / "snapshots" / "2025-01-15" / "skills" / "by-id" / "o" / "r" / "a.json").read_text())
    assert detail["id"] == "o/r/a"
    assert detail["name"] == "Alpha"
    assert detail["categories"] == ["tools"]

    metrics = json.loads((root / "snapshots" / "2025-01-15" / "metrics" / "by-id" / "o" / "r" / "a.json").read_text())
    assert metrics["id"] == "o/r/a"
    assert [item["snapshot_date"] for item in metrics["items"]] == ["2025-01-14", "2025-01-15"]

    slim_index = json.loads((root / "snapshots" / "2025-01-15" / "search" / "slim-index.json").read_text())
    assert slim_index["snapshot_date"] == "2025-01-15"
    assert len(slim_index["items"]) == 3
    assert {"id", "name", "owner", "repo", "skill_id"} <= set(slim_index["items"][0])
    assert "description" not in slim_index["items"][0]
    assert "platform_installs" not in slim_index["items"][0]


def test_build_web_static_pack_requires_current_snapshot(tmp_path: Path) -> None:
    from skillsight.pipeline.web_static_pack import build_web_static_pack

    with pytest.raises(FileNotFoundError):
        build_web_static_pack(tmp_path, snapshot_date=date(2025, 1, 15))
