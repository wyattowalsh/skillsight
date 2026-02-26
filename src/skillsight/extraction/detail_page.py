"""Structured extraction for individual skill pages."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx
from loguru import logger
from lxml import html
from pydantic import HttpUrl

from skillsight.clients.http import RequestContext, SoftErrorDetected, fetch_text
from skillsight.extraction.html_parser import (
    parse_canonical_url,
    parse_categories,
    parse_first_seen,
    parse_github_url,
    parse_install_command,
    parse_og_image,
    parse_platform_installs,
    parse_skill_description,
    parse_skill_md_content,
    parse_skill_name,
    parse_weekly_installs,
    validate_skill_page,
)
from skillsight.models.checkpoint import ExtractionCheckpoint, FailureRecord
from skillsight.models.skill import DiscoveredSkill, SkillRecord
from skillsight.storage.checkpoint import save_checkpoint
from skillsight.utils.parsing import parse_first_seen_date

if TYPE_CHECKING:
    from pathlib import Path

    from skillsight.settings import Settings

DEFAULT_BATCH_SIZE = 1000


def extract_skill_record(
    discovered: DiscoveredSkill,
    page_html: str,
    settings: Settings,
    run_id: str,
    *,
    fetched_at: datetime | None = None,
    http_status: int | None = 200,
) -> SkillRecord:
    """Parse one skill HTML page into a structured record."""

    tree = html.fromstring(page_html)
    fetched_at = fetched_at or datetime.now(UTC)

    if not validate_skill_page(tree):
        raise SoftErrorDetected(f"Skill page validation failed for {discovered.id}")

    canonical = parse_canonical_url(tree)
    if canonical is None:
        canonical = f"https://skills.sh/{discovered.owner}/{discovered.repo}/{discovered.skill_id}"

    title = parse_skill_name(tree) or discovered.name
    description = parse_skill_description(tree)
    og_image = parse_og_image(tree)
    github_url_raw = parse_github_url(tree)

    # Compute full text once and reuse across parsers
    full_text = " ".join(tree.xpath("//text()"))
    weekly_raw, weekly_installs = parse_weekly_installs(tree, full_text=full_text)
    first_seen_raw = parse_first_seen(tree, full_text=full_text)

    return SkillRecord(
        id=discovered.id,
        skill_id=discovered.skill_id,
        owner=discovered.owner,
        repo=discovered.repo,
        canonical_url=HttpUrl(canonical),
        total_installs=discovered.installs,
        weekly_installs_raw=weekly_raw,
        weekly_installs=weekly_installs,
        platform_installs=parse_platform_installs(tree, full_text=full_text),
        name=title,
        description=description,
        first_seen_date=parse_first_seen_date(first_seen_raw),
        github_url=HttpUrl(github_url_raw) if github_url_raw else None,
        og_image_url=HttpUrl(og_image) if og_image else None,
        skill_md_content=parse_skill_md_content(tree),
        install_command=parse_install_command(tree),
        categories=parse_categories(tree),
        run_id=run_id,
        fetched_at=fetched_at,
        discovery_source=discovered.discovered_via,
        source_endpoint=discovered.source_endpoint,
        discovery_pass=discovered.discovery_pass,
        rank_at_fetch=discovered.rank_at_fetch,
        http_status=http_status,
        parser_version=settings.parser_version,
        raw_html_hash=hashlib.sha256(page_html.encode("utf-8")).hexdigest(),
    )


async def extract_skill_records(
    client: httpx.AsyncClient,
    ctx: RequestContext,
    discovered: dict[str, DiscoveredSkill],
    settings: Settings,
    run_id: str,
    *,
    checkpoint_dir: Path | None = None,
    completed_ids: set[str] | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> tuple[list[SkillRecord], dict[str, str]]:
    """Extract structured records for all discovered skills with batch checkpointing."""

    records: list[SkillRecord] = []
    failures: dict[str, str] = {}
    completed = completed_ids or set()
    sem = asyncio.Semaphore(settings.concurrency)

    pending = {sid: skill for sid, skill in discovered.items() if sid not in completed}
    items = list(pending.values())
    total = len(items)
    logger.info("Extracting {} skills ({} already completed)", total, len(completed))

    async def _extract_one(skill: DiscoveredSkill) -> tuple[SkillRecord | None, str | None]:
        url = f"https://skills.sh/{skill.owner}/{skill.repo}/{skill.skill_id}"
        async with sem:
            try:
                page_html = await fetch_text(client, ctx, url)
                record = extract_skill_record(skill, page_html, settings, run_id)
                return record, None
            except SoftErrorDetected as exc:
                logger.warning("Soft error for {}: {}", skill.id, exc)
                return None, str(exc)
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                return None, str(exc)
            except Exception as exc:
                logger.error("Unexpected error extracting {}: {}", skill.id, exc)
                return None, f"unexpected: {exc}"

    started_at = datetime.now(UTC)

    # Process in batches with intermediate checkpoints
    for batch_start in range(0, total, batch_size):
        batch = items[batch_start : batch_start + batch_size]

        tasks: list[asyncio.Task[tuple[SkillRecord | None, str | None]]] = []
        async with asyncio.TaskGroup() as tg:
            for skill in batch:
                tasks.append(tg.create_task(_extract_one(skill)))

        for skill, task in zip(batch, tasks, strict=True):
            record, error = task.result()
            if record is not None:
                records.append(record)
                completed.add(skill.id)
            elif error is not None:
                failures[skill.id] = error

        # Save intermediate checkpoint
        if checkpoint_dir is not None and batch_start + batch_size < total:
            _save_extraction_checkpoint(
                checkpoint_dir, run_id, completed, failures, len(discovered), started_at=started_at
            )
            logger.info(
                "Checkpoint saved: {}/{} completed, {} failures",
                len(completed),
                len(discovered),
                len(failures),
            )

    return records, failures


def _save_extraction_checkpoint(
    checkpoint_dir: Path,
    run_id: str,
    completed: set[str],
    failures: dict[str, str],
    total: int,
    *,
    started_at: datetime,
) -> None:
    """Save intermediate extraction checkpoint."""
    now = datetime.now(UTC)
    checkpoint = ExtractionCheckpoint(
        run_id=run_id,
        completed=completed,
        failed={key: FailureRecord(error=msg, attempts=1, last_attempt=now) for key, msg in failures.items()},
        total=total,
        started_at=started_at,
        last_updated=now,
    )
    save_checkpoint(checkpoint_dir / "extraction_state.json", checkpoint)
