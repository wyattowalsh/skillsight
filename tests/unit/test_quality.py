"""Tests for quality report module."""

from datetime import UTC, datetime

from skillsight.models.skill import SkillRecord
from skillsight.storage.quality import build_quality_report


def _make_record(**overrides) -> SkillRecord:
    defaults = {
        "id": "o/r/s",
        "skill_id": "s",
        "owner": "o",
        "repo": "r",
        "canonical_url": "https://skills.sh/o/r/s",
        "name": "Test",
        "total_installs": 100,
        "description": "A test skill",
        "run_id": "run-1",
        "fetched_at": datetime.now(UTC),
        "discovery_source": "search_api",
        "source_endpoint": "search_api",
    }
    defaults.update(overrides)
    return SkillRecord(**defaults)


def test_quality_report_full_coverage() -> None:
    records = [_make_record(id="o/r/a"), _make_record(id="o/r/b")]
    report = build_quality_report(records, {})
    assert report["total_records"] == 2
    assert report["failures"] == 0
    assert report["coverage"]["name"] == 100.0
    assert report["coverage"]["description"] == 100.0
    assert report["coverage"]["total_installs"] == 100.0


def test_quality_report_with_missing_fields() -> None:
    records = [
        _make_record(id="o/r/a"),
        _make_record(id="o/r/b", description=None, total_installs=None),
    ]
    report = build_quality_report(records, {})
    assert report["coverage"]["description"] == 50.0
    assert report["coverage"]["total_installs"] == 50.0


def test_quality_report_with_failures() -> None:
    records = [_make_record()]
    failures = {"o/r/bad": "timeout"}
    report = build_quality_report(records, failures)
    assert report["failures"] == 1
    assert "o/r/bad" in report["failure_ids"]


def test_quality_report_empty() -> None:
    report = build_quality_report([], {})
    assert report["total_records"] == 0
    assert report["coverage"]["name"] == 0.0
