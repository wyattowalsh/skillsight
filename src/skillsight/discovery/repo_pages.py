"""Repo page expansion for skill enumeration."""

from __future__ import annotations

import asyncio
import re
from datetime import UTC, datetime

import httpx
from loguru import logger
from lxml import html

from skillsight.clients.http import RequestContext, fetch_text
from skillsight.models.skill import DiscoveredSkill
from skillsight.utils.parsing import canonical_skill_id

_SKILL_PATH_RE = re.compile(r"^/([a-zA-Z0-9_.-]+)/([a-zA-Z0-9_.-]+)/([a-zA-Z0-9_.-]+)$")


def parse_repo_page(owner: str, repo: str, page_html: str) -> dict[str, DiscoveredSkill]:
    """Extract skill ids from one repo page."""

    tree = html.fromstring(page_html)
    found: dict[str, DiscoveredSkill] = {}

    for anchor in tree.xpath("//a[@href]"):
        href = anchor.get("href", "")
        match = _SKILL_PATH_RE.match(href)
        if not match:
            continue
        page_owner, page_repo, skill_id = match.groups()
        if page_owner.lower() != owner.lower() or page_repo.lower() != repo.lower():
            continue

        canonical_id = canonical_skill_id(page_owner, page_repo, skill_id)
        name = " ".join(anchor.xpath(".//text()"))
        found.setdefault(
            canonical_id,
            DiscoveredSkill(
                id=canonical_id,
                skill_id=skill_id.lower(),
                owner=page_owner.lower(),
                repo=page_repo.lower(),
                name=name.strip() or skill_id,
                installs=None,
                discovered_via="repo_page",
                source_endpoint="repo_page",
                discovery_pass=1,
                rank_at_fetch=None,
                discovered_at=datetime.now(UTC),
            ),
        )
    return found


async def expand_from_repo_pages(
    client: httpx.AsyncClient,
    ctx: RequestContext,
    repos: set[str],
    *,
    concurrency: int = 20,
) -> dict[str, DiscoveredSkill]:
    """Expand skill frontier using repo pages."""

    discovered: dict[str, DiscoveredSkill] = {}
    sem = asyncio.Semaphore(concurrency)

    async def _fetch_one(owner: str, repo: str) -> dict[str, DiscoveredSkill]:
        async with sem:
            url = f"https://skills.sh/{owner}/{repo}"
            try:
                page_html = await fetch_text(client, ctx, url)
            except httpx.HTTPError as exc:
                logger.warning("Failed to fetch repo page {}/{}: {}", owner, repo, exc)
                return {}
            return parse_repo_page(owner, repo, page_html)

    tasks = []
    for source in sorted(repos):
        owner, repo = source.split("/", maxsplit=1)
        tasks.append(asyncio.create_task(_fetch_one(owner, repo)))

    for coro in asyncio.as_completed(tasks):
        parsed = await coro
        for cid, skill in parsed.items():
            discovered.setdefault(cid, skill)

    return discovered
