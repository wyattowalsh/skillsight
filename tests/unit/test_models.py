"""Tests for data models."""

from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from skillsight.models.checkpoint import DiscoveryCheckpoint, ExtractionCheckpoint, FailureRecord
from skillsight.models.skill import (
    ConvergencePassSummary,
    ConvergenceReport,
    DiscoveredSkill,
    PlatformInstalls,
    SkillMetrics,
    SkillRecord,
)


def test_discovered_skill_creation() -> None:
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
    assert skill.id == "o/r/s"
    assert skill.discovered_via == "search_api"


def test_discovered_skill_all_sources() -> None:
    for source in ("all_time_api", "search_api", "sitemap", "leaderboard", "repo_page", "browser"):
        skill = DiscoveredSkill(
            id="o/r/s",
            skill_id="s",
            owner="o",
            repo="r",
            name="Test",
            discovered_via=source,
            source_endpoint=source,
            discovered_at=datetime.now(UTC),
        )
        assert skill.discovered_via == source


def test_discovered_skill_invalid_source() -> None:
    with pytest.raises(ValidationError):
        DiscoveredSkill(
            id="o/r/s",
            skill_id="s",
            owner="o",
            repo="r",
            name="Test",
            discovered_via="invalid_source",
            source_endpoint="invalid_source",
            discovered_at=datetime.now(UTC),
        )


def test_skill_record_with_new_fields() -> None:
    record = SkillRecord(
        id="o/r/s",
        skill_id="s",
        owner="o",
        repo="r",
        canonical_url="https://skills.sh/o/r/s",
        name="Test",
        skill_md_content="Some markdown",
        skill_md_frontmatter={"title": "Test"},
        install_command="npx skills add o/r",
        categories=["dev", "ai"],
        run_id="run-1",
        fetched_at=datetime.now(UTC),
        discovery_source="search_api",
        source_endpoint="search_api",
    )
    assert record.skill_md_content == "Some markdown"
    assert record.categories == ["dev", "ai"]
    assert record.install_command == "npx skills add o/r"


def test_skill_record_default_categories() -> None:
    record = SkillRecord(
        id="o/r/s",
        skill_id="s",
        owner="o",
        repo="r",
        canonical_url="https://skills.sh/o/r/s",
        name="Test",
        run_id="run-1",
        fetched_at=datetime.now(UTC),
        discovery_source="search_api",
        source_endpoint="search_api",
    )
    assert record.categories == []


def test_platform_installs_extra_fields() -> None:
    p = PlatformInstalls(opencode=100, codex=50, future_platform=25)
    assert p.opencode == 100
    assert p.codex == 50
    assert p.model_dump().get("future_platform") == 25


def test_skill_metrics() -> None:
    m = SkillMetrics(
        id="o/r/s",
        snapshot_date=date.today(),
        total_installs=1000,
        weekly_installs=50,
    )
    assert m.total_installs == 1000


def test_convergence_pass_summary() -> None:
    s = ConvergencePassSummary(
        pass_number=1, ids_seen=100, repos_seen=10, new_ids=100, new_repos=10, new_ids_growth_pct=0.0
    )
    assert s.pass_number == 1


def test_convergence_report() -> None:
    r = ConvergenceReport(
        run_id="run-1",
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
        passes_executed=3,
        converged=True,
        converged_reason="test",
        total_ids=100,
        total_repos=10,
        pass_summaries=[],
    )
    assert r.converged is True


def test_failure_record() -> None:
    f = FailureRecord(error="timeout", attempts=3, last_attempt=datetime.now(UTC), http_status=408)
    assert f.attempts == 3
    assert f.http_status == 408


def test_discovery_checkpoint_sets() -> None:
    c = DiscoveryCheckpoint(
        run_id="run-1",
        search_queries_completed={"aa", "ab"},
        repos_crawled={"o/r"},
        discovered_skill_ids={"o/r/s"},
        started_at=datetime.now(UTC),
        last_updated=datetime.now(UTC),
    )
    assert "aa" in c.search_queries_completed
    assert isinstance(c.repos_crawled, set)


def test_extraction_checkpoint_sets() -> None:
    c = ExtractionCheckpoint(
        run_id="run-1",
        completed={"a", "b", "c"},
        total=5,
        started_at=datetime.now(UTC),
        last_updated=datetime.now(UTC),
    )
    assert len(c.completed) == 3
    assert isinstance(c.completed, set)
