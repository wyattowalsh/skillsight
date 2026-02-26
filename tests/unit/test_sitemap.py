"""Tests for sitemap discovery module."""

from __future__ import annotations

import httpx
import pytest

from skillsight.clients.http import RequestContext, create_http_client
from skillsight.discovery.sitemap import run_sitemap_discovery
from skillsight.settings import Settings


@pytest.mark.asyncio
async def test_run_sitemap_discovery_success(request_context: RequestContext, sitemap_xml: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=sitemap_xml)

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await run_sitemap_discovery(client, request_context)

    assert len(skills) >= 1
    assert len(repos) >= 1


@pytest.mark.asyncio
async def test_run_sitemap_discovery_failure(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection failed")

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await run_sitemap_discovery(client, request_context)

    assert skills == {}
    assert repos == set()
