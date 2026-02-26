"""Sitemap-based discovery: parse sitemap.xml for skill URLs."""

from __future__ import annotations

import re
from datetime import UTC, datetime

import httpx
from loguru import logger
from lxml import etree

from skillsight.clients.http import RequestContext, fetch_text
from skillsight.models.skill import DiscoveredSkill
from skillsight.utils.parsing import canonical_skill_id

SITEMAP_URL = "https://skills.sh/sitemap.xml"
_SKILL_URL_RE = re.compile(r"https?://skills\.sh/([a-zA-Z0-9_.-]+)/([a-zA-Z0-9_.-]+)/([a-zA-Z0-9_.-]+)/?$")


def parse_sitemap_xml(xml_content: str) -> list[DiscoveredSkill]:
    """Parse sitemap XML and extract skill URLs into DiscoveredSkill records."""
    try:
        parser = etree.XMLParser(resolve_entities=False, no_network=True)
        root = etree.fromstring(xml_content.encode("utf-8"), parser=parser)
    except etree.XMLSyntaxError:
        logger.error("Failed to parse sitemap XML")
        return []

    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = root.findall(".//sm:url/sm:loc", namespaces=ns)
    if not urls:
        urls = root.findall(".//sm:loc", namespaces=ns)

    skills: list[DiscoveredSkill] = []
    seen: set[str] = set()
    now = datetime.now(UTC)

    for url_elem in urls:
        url = (url_elem.text or "").strip()
        match = _SKILL_URL_RE.match(url)
        if not match:
            continue

        owner, repo, skill_id = match.groups()
        owner = owner.lower()
        repo = repo.lower()
        skill_id_lower = skill_id.lower()
        canonical_id = canonical_skill_id(owner, repo, skill_id_lower)

        if canonical_id in seen:
            continue
        seen.add(canonical_id)

        skills.append(
            DiscoveredSkill(
                id=canonical_id,
                skill_id=skill_id_lower,
                owner=owner,
                repo=repo,
                name=skill_id,
                installs=None,
                discovered_via="sitemap",
                source_endpoint="sitemap",
                discovery_pass=1,
                rank_at_fetch=None,
                discovered_at=now,
            )
        )

    logger.info("Sitemap parsed: {} skill URLs found", len(skills))
    return skills


async def run_sitemap_discovery(
    client: httpx.AsyncClient,
    ctx: RequestContext,
) -> tuple[dict[str, DiscoveredSkill], set[str]]:
    """Fetch and parse sitemap.xml, returning discovered skills + repos."""
    try:
        xml_content = await fetch_text(client, ctx, SITEMAP_URL)
    except (httpx.HTTPError, httpx.TimeoutException):
        logger.warning("Failed to fetch sitemap.xml")
        return {}, set()

    skills = parse_sitemap_xml(xml_content)

    result: dict[str, DiscoveredSkill] = {}
    repos: set[str] = set()
    for skill in skills:
        result[skill.id] = skill
        repos.add(f"{skill.owner}/{skill.repo}")

    return result, repos
