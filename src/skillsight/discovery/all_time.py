"""Primary discovery strategy using /api/skills/all-time/{page}."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx
from loguru import logger

from skillsight.clients.http import RequestContext, fetch_json
from skillsight.models.skill import ConvergencePassSummary, ConvergenceReport, DiscoveredSkill
from skillsight.utils.parsing import canonical_skill_id, split_source

if TYPE_CHECKING:
    from skillsight.settings import Settings


async def _fetch_all_time_page(
    client: httpx.AsyncClient,
    ctx: RequestContext,
    page: int,
) -> dict[str, object]:
    url = f"https://skills.sh/api/skills/all-time/{page}"
    return await fetch_json(client, ctx, url)


async def _crawl_all_time_once(
    client: httpx.AsyncClient,
    ctx: RequestContext,
    run_id: str,
    pass_number: int,
) -> tuple[dict[str, DiscoveredSkill], set[str]]:
    all_skills: dict[str, DiscoveredSkill] = {}
    repos: set[str] = set()

    page = 1
    rank = 1
    while True:
        payload = await _fetch_all_time_page(client, ctx, page)
        raw_items = payload.get("skills", [])
        if not isinstance(raw_items, list):
            raise TypeError(f"Unexpected payload shape on page {page}")

        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            source = str(raw.get("source", "")).strip()
            skill_id = str(raw.get("skillId", "")).strip()
            name = str(raw.get("name", skill_id or "")).strip()
            installs = raw.get("installs")

            split = split_source(source)
            if split is None or not skill_id:
                rank += 1
                continue

            owner, repo = split
            canonical_id = canonical_skill_id(owner, repo, skill_id)
            repos.add(f"{owner}/{repo}")

            all_skills.setdefault(
                canonical_id,
                DiscoveredSkill(
                    id=canonical_id,
                    skill_id=skill_id.lower(),
                    owner=owner,
                    repo=repo,
                    name=name or skill_id,
                    installs=installs if isinstance(installs, int) else None,
                    discovered_via="all_time_api",
                    source_endpoint="all_time_api",
                    discovery_pass=pass_number,
                    rank_at_fetch=rank,
                    discovered_at=datetime.now(UTC),
                ),
            )
            rank += 1

        has_more = bool(payload.get("hasMore", False))
        if not has_more:
            break
        page += 1

    return all_skills, repos


async def _search_fallback(
    client: httpx.AsyncClient,
    ctx: RequestContext,
    existing: dict[str, DiscoveredSkill],
    pass_number: int,
    *,
    search_query_limit: int = 1000,
) -> tuple[dict[str, DiscoveredSkill], set[str]]:
    queries = ["sk", "er", "co", "in", "de", "th", "re", "-a", "a-", "__", "--"]
    repos: set[str] = set()

    for query in queries:
        url = f"https://skills.sh/api/search?q={query}&limit={search_query_limit}"
        payload = await fetch_json(client, ctx, url)
        raw_items = payload.get("skills", [])
        if not isinstance(raw_items, list):
            continue

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
            repos.add(f"{owner}/{repo}")
            canonical_id = canonical_skill_id(owner, repo, skill_id)
            if canonical_id in existing:
                continue

            existing[canonical_id] = DiscoveredSkill(
                id=canonical_id,
                skill_id=skill_id.lower(),
                owner=owner,
                repo=repo,
                name=name or skill_id,
                installs=installs if isinstance(installs, int) else None,
                discovered_via="search_api",
                source_endpoint="search_api",
                discovery_pass=pass_number,
                rank_at_fetch=rank,
                discovered_at=datetime.now(UTC),
            )

        await asyncio.sleep(0)

    return existing, repos


async def run_convergence_discovery(
    client: httpx.AsyncClient,
    ctx: RequestContext,
    settings: Settings,
    run_id: str,
) -> tuple[dict[str, DiscoveredSkill], set[str], ConvergenceReport]:
    """Run repeated all-time passes until convergence gates are met."""

    started_at = datetime.now(UTC)
    aggregate: dict[str, DiscoveredSkill] = {}
    repo_union: set[str] = set()
    summaries: list[ConvergencePassSummary] = []

    stable_repo_streak = 0
    prev_repo_count = 0
    fallback_used = False

    converged = False
    converged_reason = "max_passes_reached"

    for pass_number in range(1, settings.passes_max + 1):
        try:
            discovered_this_pass, repos_this_pass = await _crawl_all_time_once(client, ctx, run_id, pass_number)
        except (httpx.HTTPError, httpx.TimeoutException, TypeError, ValueError) as exc:
            logger.warning("All-time pass {} failed, continuing with empty results: {}", pass_number, exc)
            discovered_this_pass, repos_this_pass = {}, set()

        previous_id_count = len(aggregate)
        previous_repo_count = len(repo_union)

        for canonical_id, skill in discovered_this_pass.items():
            aggregate.setdefault(canonical_id, skill)
        repo_union.update(repos_this_pass)

        new_ids = len(aggregate) - previous_id_count
        new_repos = len(repo_union) - previous_repo_count
        growth_pct = 0.0 if previous_id_count == 0 else new_ids * 100.0 / previous_id_count

        summaries.append(
            ConvergencePassSummary(
                pass_number=pass_number,
                ids_seen=len(aggregate),
                repos_seen=len(repo_union),
                new_ids=new_ids,
                new_repos=new_repos,
                new_ids_growth_pct=growth_pct,
            )
        )

        if len(repo_union) == prev_repo_count:
            stable_repo_streak += 1
        else:
            stable_repo_streak = 1
        prev_repo_count = len(repo_union)

        if stable_repo_streak >= settings.converge_repos and growth_pct <= settings.converge_growth:
            converged = True
            converged_reason = "repos_stable_and_growth_threshold_met"
            break

    if not converged:
        fallback_used = True
        try:
            aggregate, fallback_repos = await _search_fallback(
                client, ctx, aggregate, settings.passes_max + 1, search_query_limit=settings.search_query_limit
            )
            repo_union.update(fallback_repos)
        except (httpx.HTTPError, httpx.TimeoutException, TypeError, ValueError) as exc:
            logger.warning("Search fallback failed, continuing without fallback results: {}", exc)

        post_fallback_passes = max(settings.converge_repos, 2)
        for offset in range(post_fallback_passes):
            pass_number = settings.passes_max + 2 + offset
            try:
                discovered_this_pass, repos_this_pass = await _crawl_all_time_once(client, ctx, run_id, pass_number)
            except (httpx.HTTPError, httpx.TimeoutException, TypeError, ValueError) as exc:
                logger.warning("Post-fallback pass {} failed, continuing with empty results: {}", pass_number, exc)
                discovered_this_pass, repos_this_pass = {}, set()

            previous_id_count = len(aggregate)
            previous_repo_count = len(repo_union)

            for canonical_id, skill in discovered_this_pass.items():
                aggregate.setdefault(canonical_id, skill)
            repo_union.update(repos_this_pass)

            new_ids = len(aggregate) - previous_id_count
            new_repos = len(repo_union) - previous_repo_count
            growth_pct = 0.0 if previous_id_count == 0 else new_ids * 100.0 / previous_id_count

            summaries.append(
                ConvergencePassSummary(
                    pass_number=pass_number,
                    ids_seen=len(aggregate),
                    repos_seen=len(repo_union),
                    new_ids=new_ids,
                    new_repos=new_repos,
                    new_ids_growth_pct=growth_pct,
                )
            )

            if len(repo_union) == prev_repo_count:
                stable_repo_streak += 1
            else:
                stable_repo_streak = 1
            prev_repo_count = len(repo_union)

            if stable_repo_streak >= settings.converge_repos and growth_pct <= settings.converge_growth:
                converged = True
                converged_reason = "converged_after_search_fallback"
                break

    report = ConvergenceReport(
        run_id=run_id,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        passes_executed=len(summaries),
        converged=converged,
        converged_reason=converged_reason,
        total_ids=len(aggregate),
        total_repos=len(repo_union),
        pass_summaries=summaries,
        fallback_used=fallback_used,
    )
    return aggregate, repo_union, report
