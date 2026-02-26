"""Search API discovery: 2-char alphanumeric enumeration."""

from __future__ import annotations

import asyncio
import string
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx
from loguru import logger

from skillsight.clients.http import RequestContext, RetryableStatusError, fetch_with_retry
from skillsight.models.skill import DiscoveredSkill
from skillsight.utils.parsing import canonical_skill_id, split_source

if TYPE_CHECKING:
    from skillsight.settings import Settings

CHARS = string.ascii_lowercase + string.digits


def generate_two_char_queries() -> list[str]:
    """Generate all 36x36=1296 two-character alphanumeric queries."""
    return [a + b for a in CHARS for b in CHARS]


async def _search_one_query(
    client: httpx.AsyncClient,
    ctx: RequestContext,
    query: str,
    limit: int,
) -> tuple[dict[str, DiscoveredSkill], set[str]]:
    """Run a single search query and return discovered skills + repos."""
    url = f"https://skills.sh/api/search?q={query}&limit={limit}"
    try:
        response = await fetch_with_retry(client, ctx, url)
    except RetryableStatusError as exc:
        logger.warning("Retries exhausted for query '{}': HTTP {}", query, exc.status_code)
        return {}, set()
    except (httpx.HTTPError, httpx.TimeoutException):
        logger.warning("Search query '{}' failed", query)
        return {}, set()
    if response.status_code == 400:
        return {}, set()
    if response.status_code != 200:
        logger.warning("Search query '{}' returned status {}", query, response.status_code)
        return {}, set()

    try:
        payload = response.json()
    except ValueError:
        logger.warning("Search query '{}' returned invalid JSON", query)
        return {}, set()

    raw_items = payload.get("skills", [])
    if not isinstance(raw_items, list):
        return {}, set()

    skills: dict[str, DiscoveredSkill] = {}
    repos: set[str] = set()
    now = datetime.now(UTC)

    for rank, raw in enumerate(raw_items, start=1):
        if not isinstance(raw, dict):
            continue
        source = str(raw.get("source", "")).strip()
        skill_id = str(raw.get("skillId", "")).strip()
        name = str(raw.get("name", skill_id or "")).strip()
        installs = raw.get("installs")

        split = split_source(source)
        if split is None or not skill_id:
            continue

        owner, repo = split
        canonical_id = canonical_skill_id(owner, repo, skill_id)
        repos.add(f"{owner}/{repo}")

        skills.setdefault(
            canonical_id,
            DiscoveredSkill(
                id=canonical_id,
                skill_id=skill_id.lower(),
                owner=owner,
                repo=repo,
                name=name or skill_id,
                installs=installs if isinstance(installs, int) else None,
                discovered_via="search_api",
                source_endpoint="search_api",
                discovery_pass=1,
                rank_at_fetch=rank,
                discovered_at=now,
            ),
        )

    return skills, repos


async def run_search_api_sweep(
    client: httpx.AsyncClient,
    ctx: RequestContext,
    settings: Settings,
    *,
    completed_queries: set[str] | None = None,
    sample: int | None = None,
) -> tuple[dict[str, DiscoveredSkill], set[str], list[str]]:
    """Run 2-char enumeration sweep across the search API.

    Returns (discovered_skills, repos, completed_queries).
    """
    all_queries = generate_two_char_queries()
    completed = completed_queries or set()
    remaining = [q for q in all_queries if q not in completed]

    if sample is not None and sample > 0:
        remaining = remaining[:sample]

    all_skills: dict[str, DiscoveredSkill] = {}
    all_repos: set[str] = set()
    newly_completed: list[str] = []

    sem = asyncio.Semaphore(settings.search_batch_size)

    async def _bounded_search(query: str) -> tuple[str, dict[str, DiscoveredSkill], set[str]]:
        async with sem:
            skills, repos = await _search_one_query(client, ctx, query, settings.search_query_limit)
            return query, skills, repos

    tasks = [asyncio.create_task(_bounded_search(q)) for q in remaining]

    for coro in asyncio.as_completed(tasks):
        query, skills, repos = await coro
        for cid, skill in skills.items():
            all_skills.setdefault(cid, skill)
        all_repos.update(repos)
        newly_completed.append(query)

    logger.info(
        "Search API sweep: {} queries, {} skills, {} repos",
        len(newly_completed),
        len(all_skills),
        len(all_repos),
    )
    return all_skills, all_repos, newly_completed
