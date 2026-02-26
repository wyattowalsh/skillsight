"""Persistence helpers for discovery results."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from .jsonl import write_jsonl

if TYPE_CHECKING:
    from pathlib import Path

    from skillsight.models.skill import DiscoveredSkill


def persist_discovery_results(
    output_dir: Path,
    skills: dict[str, DiscoveredSkill],
    repos: list[str],
    run_id: str,
) -> None:
    """Write discovered_skills.jsonl and repos.json to output_dir/discovery/."""
    discovery_dir = output_dir / "discovery"
    write_jsonl(
        discovery_dir / "discovered_skills.jsonl",
        [skill.model_dump(mode="json") for skill in skills.values()],
    )
    repos_path = discovery_dir / "repos.json"
    repos_path.parent.mkdir(parents=True, exist_ok=True)
    repos_path.write_text(json.dumps({"repos": repos, "run_id": run_id}, indent=2))
