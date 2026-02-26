"""Tests for extraction_flow Prefect flow."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from skillsight.clients.http import RequestContext, create_http_client
from skillsight.models.checkpoint import ExtractionCheckpoint
from skillsight.pipeline.extraction_flow import extraction_flow
from skillsight.settings import Settings
from skillsight.storage.checkpoint import load_checkpoint, save_checkpoint
from tests.factories import make_discovered


def _make_skill_html(name: str) -> str:
    return f"""<html>
    <head>
        <link rel="canonical" href="https://skills.sh/o/r/{name}">
        <meta name="description" content="A {name} skill">
    </head>
    <body><h1>{name}</h1></body>
    </html>"""


@pytest.mark.asyncio
async def test_extraction_flow_basic(request_context: RequestContext, tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=_make_skill_html("test"))

    settings = Settings(output_dir=tmp_path)
    discovered = {"o/r/test": make_discovered("test")}

    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        records, metrics, failures = await extraction_flow.fn(settings, "run-1", client, request_context, discovered)

    assert len(records) == 1
    assert len(metrics) == 1
    assert len(failures) == 0

    # Check output files
    from datetime import date

    snapshot_dir = tmp_path / "snapshots" / date.today().isoformat()
    assert (snapshot_dir / "skills_full.jsonl").exists()
    assert (snapshot_dir / "skills_full.parquet").exists()
    assert (snapshot_dir / "metrics.jsonl").exists()
    assert (snapshot_dir / "metrics.parquet").exists()

    # Check checkpoint
    assert (tmp_path / "checkpoints" / "extraction_state.json").exists()


@pytest.mark.asyncio
async def test_extraction_flow_with_failures(request_context: RequestContext, tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html><body>Invalid</body></html>")

    settings = Settings(output_dir=tmp_path)
    discovered = {"o/r/bad": make_discovered("bad")}

    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        records, metrics, failures = await extraction_flow.fn(settings, "run-1", client, request_context, discovered)

    assert len(records) == 0
    assert len(failures) == 1
    assert "o/r/bad" in failures


@pytest.mark.asyncio
async def test_extraction_flow_resume_keeps_completed_ids_across_run_id_change(
    request_context: RequestContext, tmp_path
) -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        skill_id = request.url.path.rstrip("/").split("/")[-1]
        return httpx.Response(200, text=_make_skill_html(skill_id))

    save_checkpoint(
        tmp_path / "checkpoints" / "extraction_state.json",
        ExtractionCheckpoint(
            run_id="previous-run",
            completed={"o/r/done"},
            total=2,
            started_at=datetime.now(UTC),
            last_updated=datetime.now(UTC),
        ),
    )

    settings = Settings(output_dir=tmp_path, resume=True)
    discovered = {
        "o/r/done": make_discovered("done"),
        "o/r/new": make_discovered("new"),
    }

    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        records, _, _ = await extraction_flow.fn(settings, "new-run", client, request_context, discovered)

    assert call_count == 1
    assert {record.id for record in records} == {"o/r/new"}

    checkpoint = load_checkpoint(tmp_path / "checkpoints" / "extraction_state.json", ExtractionCheckpoint)
    assert checkpoint is not None
    assert checkpoint.completed == {"o/r/done", "o/r/new"}
