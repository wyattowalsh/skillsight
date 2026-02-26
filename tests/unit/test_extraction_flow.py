"""Tests for extraction flow module."""

from __future__ import annotations

import httpx
import pytest

from skillsight.clients.http import RequestContext, create_http_client
from skillsight.extraction.detail_page import extract_skill_records
from skillsight.settings import Settings
from tests.factories import make_discovered


def _make_skill_html(name: str = "test") -> str:
    return f"""<html>
    <head>
        <link rel="canonical" href="https://skills.sh/o/r/{name}">
        <meta name="description" content="A {name} skill">
    </head>
    <body>
        <h1>{name}</h1>
    </body>
    </html>"""


@pytest.mark.asyncio
async def test_extract_skill_records_success(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=_make_skill_html("skill-a"))

    settings = Settings()
    discovered = {"o/r/skill-a": make_discovered("skill-a")}

    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        records, failures = await extract_skill_records(client, request_context, discovered, settings, "run-1")

    assert len(records) == 1
    assert records[0].id == "o/r/skill-a"
    assert len(failures) == 0


@pytest.mark.asyncio
async def test_extract_skill_records_with_failure(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        # Return an invalid page that fails validation
        return httpx.Response(200, text="<html><body><p>Not a skill page</p></body></html>")

    settings = Settings()
    discovered = {"o/r/bad": make_discovered("bad")}

    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        records, failures = await extract_skill_records(client, request_context, discovered, settings, "run-1")

    assert len(records) == 0
    assert len(failures) == 1
    assert "o/r/bad" in failures


@pytest.mark.asyncio
async def test_extract_skill_records_skips_completed(request_context: RequestContext) -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(200, text=_make_skill_html("skill-a"))

    settings = Settings()
    discovered = {
        "o/r/skill-a": make_discovered("skill-a"),
        "o/r/skill-b": make_discovered("skill-b"),
    }

    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        records, failures = await extract_skill_records(
            client,
            request_context,
            discovered,
            settings,
            "run-1",
            completed_ids={"o/r/skill-a"},
        )

    assert call_count == 1  # Only skill-b was fetched


@pytest.mark.asyncio
async def test_extract_skill_records_batch_checkpoint(request_context: RequestContext, tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=_make_skill_html("skill"))

    settings = Settings()
    # Create enough skills to trigger batch checkpointing
    discovered = {f"o/r/s{i}": make_discovered(f"s{i}") for i in range(3)}

    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        records, failures = await extract_skill_records(
            client,
            request_context,
            discovered,
            settings,
            "run-1",
            checkpoint_dir=tmp_path,
            batch_size=2,  # Small batch to trigger checkpoint
        )

    assert len(records) == 3
    # Checkpoint should have been saved for intermediate batch
    checkpoint_path = tmp_path / "extraction_state.json"
    assert checkpoint_path.exists()


@pytest.mark.asyncio
async def test_extract_skill_records_http_error(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused")

    settings = Settings()
    discovered = {"o/r/fail": make_discovered("fail")}

    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        records, failures = await extract_skill_records(client, request_context, discovered, settings, "run-1")

    assert len(records) == 0
    assert len(failures) == 1
