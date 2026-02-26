"""Tests for storage modules."""

import json
from datetime import UTC, date, datetime
from pathlib import Path

from skillsight.models.skill import PlatformInstalls, SkillMetrics
from skillsight.storage.jsonl import count_jsonl_rows, count_jsonl_rows_with_errors, read_jsonl, write_jsonl
from skillsight.storage.parquet import write_metrics_parquet, write_skills_parquet
from skillsight.storage.sqlite import read_skills_sqlite, write_skills_sqlite
from tests.factories import make_record as _make_record


def test_jsonl_roundtrip(tmp_path) -> None:
    path = tmp_path / "test.jsonl"
    rows = [{"id": "a", "name": "A"}, {"id": "b", "name": "B"}]
    write_jsonl(path, rows)
    loaded = read_jsonl(path)
    assert len(loaded) == 2
    assert loaded[0]["id"] == "a"
    assert loaded[1]["id"] == "b"


def test_jsonl_empty_file(tmp_path) -> None:
    path = tmp_path / "empty.jsonl"
    assert read_jsonl(path) == []


def test_jsonl_with_blanks(tmp_path) -> None:
    path = tmp_path / "blanks.jsonl"
    path.write_text('{"id": "a"}\n\n{"id": "b"}\n')
    loaded = read_jsonl(path)
    assert len(loaded) == 2


def test_jsonl_count_rows_without_full_load(tmp_path) -> None:
    path = tmp_path / "count.jsonl"
    path.write_text('{"id":"a"}\n\n{"id":"b"}\n["not-a-dict"]\ncorrupt\n{"id":"c"}\n')
    assert count_jsonl_rows(path) == 3


def test_jsonl_count_rows_with_errors_reports_parse_error_count(tmp_path) -> None:
    path = tmp_path / "count_with_errors.jsonl"
    path.write_text('{"id":"a"}\ncorrupt\n{"id":"b"}\n{"id":\n')
    count, parse_errors = count_jsonl_rows_with_errors(path)
    assert count == 2
    assert parse_errors == 2


def test_parquet_skills_roundtrip(tmp_path) -> None:
    path = tmp_path / "skills.parquet"
    records = [_make_record(id="o/r/a"), _make_record(id="o/r/b", name="B")]
    write_skills_parquet(path, records)
    assert path.exists()
    assert path.stat().st_size > 0


def test_parquet_metrics_roundtrip(tmp_path) -> None:
    path = tmp_path / "metrics.parquet"
    metrics = [
        SkillMetrics(id="o/r/a", snapshot_date=date.today(), total_installs=100),
        SkillMetrics(id="o/r/b", snapshot_date=date.today(), total_installs=200),
    ]
    write_metrics_parquet(path, metrics)
    assert path.exists()
    assert path.stat().st_size > 0


def test_parquet_with_platform_installs(tmp_path) -> None:
    path = tmp_path / "skills.parquet"
    record = _make_record(platform_installs=PlatformInstalls(opencode=100, codex=50))
    write_skills_parquet(path, [record])
    assert path.exists()


def test_sqlite_write_read(tmp_path) -> None:
    path = tmp_path / "skills.db"
    records = [_make_record(id="o/r/a"), _make_record(id="o/r/b", name="Skill B")]
    write_skills_sqlite(path, records)

    loaded = read_skills_sqlite(path)
    assert len(loaded) == 2
    ids = {r["id"] for r in loaded}
    assert "o/r/a" in ids
    assert "o/r/b" in ids


def test_sqlite_with_platform_installs(tmp_path) -> None:
    path = tmp_path / "skills.db"
    record = _make_record(platform_installs=PlatformInstalls(opencode=100, codex=50))
    write_skills_sqlite(path, [record])

    loaded = read_skills_sqlite(path)
    assert len(loaded) == 1


def test_sqlite_upsert(tmp_path) -> None:
    path = tmp_path / "skills.db"
    records = [_make_record(id="o/r/a", name="Original")]
    write_skills_sqlite(path, records)

    updated = [_make_record(id="o/r/a", name="Updated")]
    write_skills_sqlite(path, updated)

    loaded = read_skills_sqlite(path)
    assert len(loaded) == 1
    assert loaded[0]["name"] == "Updated"


def test_persist_discovery_results(tmp_path: Path) -> None:
    """Verify persist_discovery_results writes both output files."""
    from skillsight.models.skill import DiscoveredSkill
    from skillsight.storage import persist_discovery_results

    skill = DiscoveredSkill(
        id="o/r/s",
        skill_id="s",
        owner="o",
        repo="r",
        name="Test",
        discovered_via="search_api",
        source_endpoint="search_api",
        discovered_at=datetime.now(UTC),
    )
    persist_discovery_results(tmp_path, {"o/r/s": skill}, ["o/r"], "run-1")

    jsonl_path = tmp_path / "discovery" / "discovered_skills.jsonl"
    assert jsonl_path.exists()
    rows = read_jsonl(jsonl_path)
    assert len(rows) == 1
    assert rows[0]["id"] == "o/r/s"

    repos_path = tmp_path / "discovery" / "repos.json"
    assert repos_path.exists()
    data = json.loads(repos_path.read_text())
    assert data["repos"] == ["o/r"]
    assert data["run_id"] == "run-1"
