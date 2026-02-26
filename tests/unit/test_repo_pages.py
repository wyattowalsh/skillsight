"""Tests for repo pages discovery module."""

from __future__ import annotations

import httpx
import pytest

from skillsight.clients.http import RequestContext, create_http_client
from skillsight.discovery.repo_pages import expand_from_repo_pages, parse_repo_page
from skillsight.settings import Settings


def test_parse_repo_page_empty() -> None:
    result = parse_repo_page("owner", "repo", "<html><body>no links</body></html>")
    assert result == {}


def test_parse_repo_page_filters_other_repos() -> None:
    html = '<html><body><a href="/other/repo/skill-a">A</a></body></html>'
    result = parse_repo_page("owner", "myrepo", html)
    assert result == {}


def test_parse_repo_page_extracts_skills() -> None:
    html = (
        '<html><body><a href="/owner/repo/skill-a">Skill A</a><a href="/owner/repo/skill-b">Skill B</a></body></html>'
    )
    result = parse_repo_page("owner", "repo", html)
    assert len(result) == 2
    assert "owner/repo/skill-a" in result
    assert "owner/repo/skill-b" in result


def test_parse_repo_page_deduplicates() -> None:
    html = '<html><body><a href="/owner/repo/skill-a">A</a><a href="/owner/repo/skill-a">A again</a></body></html>'
    result = parse_repo_page("owner", "repo", html)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_expand_from_repo_pages_success(request_context: RequestContext) -> None:
    html = '<html><body><a href="/owner/repo/skill-a">A</a></body></html>'

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html)

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        result = await expand_from_repo_pages(client, request_context, {"owner/repo"})

    assert len(result) == 1
    assert "owner/repo/skill-a" in result


@pytest.mark.asyncio
async def test_expand_from_repo_pages_failure(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("fail")

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        result = await expand_from_repo_pages(client, request_context, {"owner/repo"})

    assert result == {}
