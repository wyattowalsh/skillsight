"""Top-level pipeline orchestrator."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from aiolimiter import AsyncLimiter
from filelock import FileLock
from loguru import logger
from prefect import flow

from skillsight.clients.http import AdaptiveBlockMonitor, RequestContext, create_http_client
from skillsight.pipeline.discovery_flow import discovery_flow
from skillsight.pipeline.extraction_flow import extraction_flow
from skillsight.pipeline.publish_datasets import publish_datasets
from skillsight.pipeline.timeseries_flow import timeseries_flow
from skillsight.pipeline.validation_flow import validation_flow
from skillsight.pipeline.web_static_pack import web_static_pack_flow
from skillsight.storage.completeness import compare_with_previous_snapshot

if TYPE_CHECKING:
    from skillsight.settings import Settings


@flow(name="skillsight-pipeline", log_prints=True, retries=1, retry_delay_seconds=300, persist_result=True)
async def skillsight_pipeline(settings: Settings) -> dict[str, object]:
    """Run discovery, extraction, validation, and timeseries analysis."""

    lock_path = settings.output_dir / ".pipeline.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(lock_path), timeout=10)

    # Acquire lock without blocking the async event loop
    await asyncio.to_thread(lock.acquire)
    try:
        logger.info("Pipeline lock acquired")

        run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        limiter = AsyncLimiter(settings.rate_limit_per_second, 1)
        monitor = AdaptiveBlockMonitor(
            window=settings.browser_block_window,
            threshold_percent=settings.browser_block_threshold_percent,
        )
        ctx = RequestContext(limiter=limiter, monitor=monitor)

        async with await create_http_client(settings) as client:
            discovered, discovery_summary = await discovery_flow(settings, run_id, client, ctx)

            # Discovery viability gate — abort if too few skills to prevent data loss
            if len(discovered) < 10:
                raise RuntimeError(f"Discovery returned only {len(discovered)} skills, aborting to prevent data loss")

            records, metrics, failures = await extraction_flow(settings, run_id, client, ctx, discovered)

        quality = validation_flow(records, failures, discovery_summary)
        snapshot_date = metrics[0].snapshot_date if metrics else date.today()
        completeness = compare_with_previous_snapshot(settings.output_dir, snapshot_date, len(records)) if metrics else {}
        quality["run_id"] = run_id
        quality["browser_escalation_recommended"] = monitor.should_escalate
        quality["completeness"] = completeness

        web_pack_result = web_static_pack_flow.fn(
            settings,
            snapshot_date=snapshot_date,
            page_size=settings.web_export_page_size,
            export_prefix=settings.web_export_prefix,
        )
        quality["web_static_pack"] = web_pack_result

        # Run timeseries analysis
        ts_summary = timeseries_flow(settings.output_dir, snapshot_date)
        quality["timeseries"] = ts_summary

        if settings.github_release_enabled or settings.kaggle_publish_enabled:
            try:
                quality["dataset_mirrors"] = publish_datasets(settings, snapshot_date=snapshot_date)
            except Exception as exc:  # pragma: no cover - non-blocking path
                quality["dataset_mirrors"] = {"status": "error", "error": str(exc), "retryable": True}

        reports_dir = settings.output_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        (reports_dir / "quality_report.json").write_text(json.dumps(quality, indent=2, default=str))
    finally:
        lock.release()

    return quality
