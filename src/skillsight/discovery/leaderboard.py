"""Homepage leaderboard discovery: extract skills from RSC payload."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
from loguru import logger

from skillsight.clients.http import RequestContext, fetch_text
from skillsight.extraction.rsc_parser import extract_json_objects, extract_rsc_chunks
from skillsight.models.skill import DiscoveredSkill
from skillsight.utils.parsing import canonical_skill_id, split_source

HOMEPAGE_URL = "https://skills.sh/"


def _extract_rsc_skills(html_content: str) -> list[dict]:
    """Extract skill-like JSON objects from RSC payload chunks in HTML."""
    skills: list[dict] = []
    for chunk in extract_rsc_chunks(html_content):
        for obj in extract_json_objects(chunk):
            if "skillId" in obj:
                skills.append(obj)
    return skills


def parse_leaderboard_html(html_content: str) -> list[DiscoveredSkill]:
    """Parse homepage HTML for leaderboard skill data."""
    raw_skills = _extract_rsc_skills(html_content)

    result: list[DiscoveredSkill] = []
    seen: set[str] = set()
    now = datetime.now(UTC)

    for rank, raw in enumerate(raw_skills, start=1):
        source = str(raw.get("source", "")).strip()
        skill_id = str(raw.get("skillId", "")).strip()
        name = str(raw.get("name", skill_id or "")).strip()
        installs = raw.get("installs")

        split = split_source(source)
        if split is None or not skill_id:
            continue

        owner, repo = split
        canonical_id = canonical_skill_id(owner, repo, skill_id)

        if canonical_id in seen:
            continue
        seen.add(canonical_id)

        result.append(
            DiscoveredSkill(
                id=canonical_id,
                skill_id=skill_id.lower(),
                owner=owner,
                repo=repo,
                name=name or skill_id,
                installs=installs if isinstance(installs, int) else None,
                discovered_via="leaderboard",
                source_endpoint="leaderboard",
                discovery_pass=1,
                rank_at_fetch=rank,
                discovered_at=now,
            )
        )

    logger.info("Leaderboard parsed: {} skills found", len(result))
    return result


async def run_leaderboard_discovery(
    client: httpx.AsyncClient,
    ctx: RequestContext,
) -> tuple[dict[str, DiscoveredSkill], set[str]]:
    """Fetch homepage and extract leaderboard skills."""
    try:
        html_content = await fetch_text(client, ctx, HOMEPAGE_URL)
    except (httpx.HTTPError, httpx.TimeoutException):
        logger.warning("Failed to fetch homepage for leaderboard discovery")
        return {}, set()

    skills = parse_leaderboard_html(html_content)

    result: dict[str, DiscoveredSkill] = {}
    repos: set[str] = set()
    for skill in skills:
        result[skill.id] = skill
        repos.add(f"{skill.owner}/{skill.repo}")

    return result, repos
