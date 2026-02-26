"""Shared test factory helpers for creating model instances."""

from __future__ import annotations

from datetime import UTC, datetime

from skillsight.models.skill import DiscoveredSkill, SkillRecord


def make_record(**overrides) -> SkillRecord:
    """Create a SkillRecord with sensible defaults. Override any field via kwargs."""
    defaults = {
        "id": "o/r/s",
        "skill_id": "s",
        "owner": "o",
        "repo": "r",
        "canonical_url": "https://skills.sh/o/r/s",
        "name": "Test Skill",
        "total_installs": 1000,
        "weekly_installs": 50,
        "description": "A test skill",
        "run_id": "run-1",
        "fetched_at": datetime.now(UTC),
        "discovery_source": "search_api",
        "source_endpoint": "search_api",
    }
    defaults.update(overrides)
    return SkillRecord(**defaults)


def make_discovered(skill_id: str = "test-skill", owner: str = "o", repo: str = "r", **overrides) -> DiscoveredSkill:
    """Create a DiscoveredSkill with sensible defaults. Override any field via kwargs."""
    defaults = {
        "id": f"{owner}/{repo}/{skill_id}",
        "skill_id": skill_id,
        "owner": owner,
        "repo": repo,
        "name": skill_id,
        "discovered_via": "search_api",
        "source_endpoint": "search_api",
        "discovered_at": datetime.now(UTC),
    }
    defaults.update(overrides)
    return DiscoveredSkill(**defaults)
