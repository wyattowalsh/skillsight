"""Discovery flow: three-phase skill enumeration."""

from __future__ import annotations

from datetime import UTC, datetime

from loguru import logger
from prefect import flow, task

from skillsight.clients.http import RequestContext  # noqa: TC001 – needed at runtime by Prefect
from skillsight.discovery.leaderboard import run_leaderboard_discovery
from skillsight.discovery.merger import merge_discovered
from skillsight.discovery.repo_pages import expand_from_repo_pages
from skillsight.discovery.search_api import run_search_api_sweep
from skillsight.discovery.sitemap import run_sitemap_discovery
from skillsight.models.checkpoint import DiscoveryCheckpoint
from skillsight.models.skill import DiscoveredSkill  # noqa: TC001 – needed at runtime by Prefect
from skillsight.settings import Settings  # noqa: TC001 – needed at runtime by Prefect
from skillsight.storage import persist_discovery_results
from skillsight.storage.checkpoint import load_checkpoint, save_checkpoint


@task(name="sitemap-discovery")
async def _run_sitemap(client, ctx: RequestContext) -> tuple[dict[str, DiscoveredSkill], set[str]]:
    return await run_sitemap_discovery(client, ctx)


@task(name="leaderboard-discovery")
async def _run_leaderboard(client, ctx: RequestContext) -> tuple[dict[str, DiscoveredSkill], set[str]]:
    return await run_leaderboard_discovery(client, ctx)


@task(name="search-api-sweep")
async def _run_search_api(
    client,
    ctx: RequestContext,
    settings: Settings,
    completed_queries: set[str] | None = None,
    sample: int | None = None,
) -> tuple[dict[str, DiscoveredSkill], set[str], list[str]]:
    return await run_search_api_sweep(
        client,
        ctx,
        settings,
        completed_queries=completed_queries,
        sample=sample,
    )


@task(name="repo-page-expansion")
async def _run_repo_expansion(client, ctx: RequestContext, repos: set[str]) -> dict[str, DiscoveredSkill]:
    return await expand_from_repo_pages(client, ctx, repos)


@flow(name="skillsight-discovery", timeout_seconds=3600)
async def discovery_flow(
    settings: Settings,
    run_id: str,
    client,
    ctx: RequestContext,
    *,
    sample: int | None = None,
) -> tuple[dict[str, DiscoveredSkill], dict]:
    """Three-phase discovery:
    1. Sitemap + Leaderboard + Search API sweep (parallel)
    2. Repo page expansion for all discovered repos
    3. Merge and deduplicate
    """
    started_at = datetime.now(UTC)
    checkpoints_dir = settings.output_dir / "checkpoints"

    # Load checkpoint for resume
    checkpoint = None
    if settings.resume:
        checkpoint = load_checkpoint(checkpoints_dir / "discovery_state.json", DiscoveryCheckpoint)

    completed_queries = checkpoint.search_queries_completed if checkpoint else None

    # Phase 1: Run sitemap, leaderboard, and search API in parallel
    logger.info("Phase 1: Running sitemap + leaderboard + search API sweep")
    sitemap_future = _run_sitemap.submit(client, ctx)
    leaderboard_future = _run_leaderboard.submit(client, ctx)
    search_future = _run_search_api.submit(client, ctx, settings, completed_queries=completed_queries, sample=sample)

    sitemap_skills, sitemap_repos = sitemap_future.result(timeout=1800)
    leaderboard_skills, leaderboard_repos = leaderboard_future.result(timeout=1800)
    search_skills, search_repos, completed_search_queries = search_future.result(timeout=1800)

    logger.info(
        "Phase 1 complete: sitemap={} leaderboard={} search={}",
        len(sitemap_skills),
        len(leaderboard_skills),
        len(search_skills),
    )

    # Phase 2: Merge phase 1 repos and expand via repo pages
    all_repos = sitemap_repos | leaderboard_repos | search_repos
    logger.info("Phase 2: Expanding {} repos via repo pages", len(all_repos))
    repo_skills = await _run_repo_expansion(client, ctx, all_repos)
    logger.info("Phase 2 complete: {} additional skills from repo pages", len(repo_skills))

    # Phase 3: Merge and dedup
    merged = merge_discovered(search_skills, sitemap_skills, leaderboard_skills, repo_skills)
    logger.info("Phase 3: Merged total = {} unique skills", len(merged))

    all_discovered_repos = sorted({f"{s.owner}/{s.repo}" for s in merged.values()})
    persist_discovery_results(settings.output_dir, merged, all_discovered_repos, run_id)

    # Save discovery summary
    summary = {
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "total_skills": len(merged),
        "total_repos": len(all_discovered_repos),
        "by_source": {
            "sitemap": len(sitemap_skills),
            "leaderboard": len(leaderboard_skills),
            "search_api": len(search_skills),
            "repo_page": len(repo_skills),
        },
    }

    # Save checkpoint
    save_checkpoint(
        checkpoints_dir / "discovery_state.json",
        DiscoveryCheckpoint(
            run_id=run_id,
            search_queries_completed=set(completed_search_queries),
            repos_crawled=set(all_discovered_repos),
            discovered_skill_ids=set(merged.keys()),
            started_at=started_at,
            last_updated=datetime.now(UTC),
        ),
    )

    return merged, summary
