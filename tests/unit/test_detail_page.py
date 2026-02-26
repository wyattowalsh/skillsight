"""Tests for detail page extraction module."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import httpx
import pytest
from aiolimiter import AsyncLimiter

from skillsight.clients.http import (
    AdaptiveBlockMonitor,
    RequestContext,
    SoftErrorDetected,
    create_http_client,
)
from skillsight.extraction.detail_page import extract_skill_record, extract_skill_records
from skillsight.settings import Settings
from tests.factories import make_discovered


def _make_discovered(**overrides):
    """Local wrapper to preserve test defaults for this module."""
    defaults = {
        "skill_id": "test-skill",
        "owner": "testowner",
        "repo": "testrepo",
    }
    defaults.update(overrides)
    return make_discovered(**defaults)


def test_extract_skill_record(skill_detail_html: str) -> None:
    settings = Settings()
    discovered = _make_discovered()
    record = extract_skill_record(discovered, skill_detail_html, settings, "run-1", http_status=200)
    assert record.id == "testowner/testrepo/test-skill"
    assert record.name == "test-skill"
    assert record.description == "A test skill for unit testing"
    assert record.run_id == "run-1"
    assert record.http_status == 200
    assert record.parser_version == settings.parser_version
    assert record.raw_html_hash is not None


def test_extract_skill_record_uses_discovered_installs(skill_detail_html: str) -> None:
    settings = Settings()
    discovered = _make_discovered(installs=5000)
    record = extract_skill_record(discovered, skill_detail_html, settings, "run-1")
    assert record.total_installs == 5000  # from discovered, not HTML


def test_extract_skill_record_invalid_page() -> None:
    settings = Settings()
    discovered = _make_discovered()
    invalid_html = "<html><body><p>Not a skill page</p></body></html>"
    with pytest.raises(SoftErrorDetected, match="validation failed"):
        extract_skill_record(discovered, invalid_html, settings, "run-1")


def test_extract_skill_record_fallback_canonical() -> None:
    """When canonical URL is missing, a default is constructed."""
    settings = Settings()
    discovered = _make_discovered()
    html = """<html>
    <head><title>test</title></head>
    <body><h1>test-skill</h1></body>
    </html>"""
    record = extract_skill_record(discovered, html, settings, "run-1")
    assert "testowner/testrepo/test-skill" in str(record.canonical_url)


def test_extract_skill_record_with_custom_fetched_at() -> None:
    settings = Settings()
    discovered = _make_discovered()
    custom_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)
    html = """<html>
    <head><title>test</title></head>
    <body><h1>test-skill</h1></body>
    </html>"""
    record = extract_skill_record(discovered, html, settings, "run-1", fetched_at=custom_time)
    assert record.fetched_at == custom_time


@pytest.mark.asyncio
async def test_extract_one_unexpected_exception() -> None:
    """When extract_skill_record raises an unexpected exception, _extract_one catches it gracefully."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html><body>ok</body></html>")

    settings = Settings()
    ctx = RequestContext(
        limiter=AsyncLimiter(100, 1),
        monitor=AdaptiveBlockMonitor(window=10, threshold_percent=50.0),
    )
    discovered = _make_discovered()
    discovered_map = {discovered.id: discovered}

    with patch(
        "skillsight.extraction.detail_page.extract_skill_record",
        side_effect=RuntimeError("boom"),
    ):
        async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
            records, failures = await extract_skill_records(client, ctx, discovered_map, settings, "run-1")

    assert len(records) == 0
    assert discovered.id in failures
    assert "unexpected" in failures[discovered.id]
