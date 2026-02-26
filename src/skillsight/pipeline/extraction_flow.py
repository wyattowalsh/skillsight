"""Extraction flow with batch checkpointing and resume support."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from prefect import flow

from skillsight.extraction.detail_page import extract_skill_records
from skillsight.models.checkpoint import ExtractionCheckpoint, FailureRecord
from skillsight.models.skill import DiscoveredSkill, SkillMetrics, SkillRecord
from skillsight.storage.checkpoint import load_checkpoint, save_checkpoint
from skillsight.storage.jsonl import write_jsonl
from skillsight.storage.parquet import write_metrics_parquet, write_skills_parquet
from skillsight.storage.sqlite import write_skills_sqlite

if TYPE_CHECKING:
    from skillsight.clients.http import RequestContext
    from skillsight.settings import Settings


@flow(name="skillsight-extraction")
async def extraction_flow(
    settings: Settings,
    run_id: str,
    client,
    ctx: RequestContext,
    discovered: dict[str, DiscoveredSkill],
) -> tuple[list[SkillRecord], list[SkillMetrics], dict[str, str]]:
    checkpoints_dir = settings.output_dir / "checkpoints"

    # Load checkpoint for resume
    completed_ids: set[str] = set()
    if settings.resume:
        existing = load_checkpoint(checkpoints_dir / "extraction_state.json", ExtractionCheckpoint)
        if existing:
            completed_ids = set(existing.completed)

    records, failures = await extract_skill_records(
        client,
        ctx,
        discovered,
        settings,
        run_id,
        checkpoint_dir=checkpoints_dir,
        completed_ids=completed_ids,
    )

    snapshot_date = date.today()
    metrics = [
        SkillMetrics(
            id=record.id,
            snapshot_date=snapshot_date,
            total_installs=record.total_installs,
            weekly_installs=record.weekly_installs,
            platform_installs=record.platform_installs,
        )
        for record in records
    ]

    snapshot_dir = settings.output_dir / "snapshots" / snapshot_date.isoformat()
    write_jsonl(snapshot_dir / "skills_full.jsonl", [record.model_dump(mode="json") for record in records])
    write_jsonl(snapshot_dir / "metrics.jsonl", [item.model_dump(mode="json") for item in metrics])
    write_skills_parquet(snapshot_dir / "skills_full.parquet", records)
    write_metrics_parquet(snapshot_dir / "metrics.parquet", metrics)
    write_skills_sqlite(snapshot_dir / "skills.db", records)

    # Save final checkpoint
    now = datetime.now(UTC)
    checkpoint = ExtractionCheckpoint(
        run_id=run_id,
        completed=completed_ids | {record.id for record in records},
        failed={
            key: FailureRecord(error=message, attempts=1, last_attempt=now, http_status=None)
            for key, message in failures.items()
        },
        total=len(discovered),
        started_at=now,
        last_updated=now,
    )
    save_checkpoint(checkpoints_dir / "extraction_state.json", checkpoint)

    return records, metrics, failures
