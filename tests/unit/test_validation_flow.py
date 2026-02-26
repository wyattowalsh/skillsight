"""Tests for validation flow module."""

from __future__ import annotations

from datetime import UTC, datetime

from skillsight.models.skill import SkillRecord
from skillsight.pipeline.validation_flow import validation_flow, verify_completeness


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


def test_validation_flow_basic() -> None:
    records = [_make_record(id="o/r/a"), _make_record(id="o/r/b")]
    summary = {"total_skills": 100, "total_repos": 10, "by_source": {"search_api": 80, "sitemap": 20}}
    result = validation_flow.fn(records, {}, summary)
    assert result["total_records"] == 2
    assert result["total_discovered"] == 100
    assert result["total_repos"] == 10
    assert result["discovery_by_source"] == {"search_api": 80, "sitemap": 20}


def test_validation_flow_with_failures() -> None:
    records = [_make_record()]
    failures = {"o/r/bad": "timeout"}
    summary = {"total_skills": 2, "total_repos": 1}
    result = validation_flow.fn(records, failures, summary)
    assert result["failures"] == 1
    assert result["total_discovered"] == 2


def test_validation_flow_empty_summary() -> None:
    records = [_make_record()]
    result = validation_flow.fn(records, {}, {})
    assert result["total_discovered"] == 0
    assert result["total_repos"] == 0
    assert result["discovery_by_source"] == {}


def test_verify_completeness_ok() -> None:
    result = verify_completeness(current_total=100, baseline_total=90)
    assert result["status"] == "ok"
    assert result["delta"] == 10
    assert result["delta_pct"] > 0


def test_verify_completeness_regression() -> None:
    result = verify_completeness(current_total=80, baseline_total=100)
    assert result["status"] == "regression"
    assert result["delta"] == -20


def test_verify_completeness_zero_baseline() -> None:
    result = verify_completeness(current_total=50, baseline_total=0)
    assert result["status"] == "ok"
    assert result["delta_pct"] == 0.0


def test_verify_completeness_equal() -> None:
    result = verify_completeness(current_total=100, baseline_total=100)
    assert result["status"] == "ok"
    assert result["delta"] == 0
