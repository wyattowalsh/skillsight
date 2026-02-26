"""Tests for leaderboard discovery module."""

from __future__ import annotations

import httpx
import pytest

from skillsight.clients.http import RequestContext, create_http_client
from skillsight.discovery.leaderboard import _extract_rsc_skills, run_leaderboard_discovery
from skillsight.settings import Settings


def test_extract_rsc_skills_from_array() -> None:
    html = '<script>self.__next_f.push([1,"[{\\"skillId\\":\\"s1\\",\\"name\\":\\"S1\\",\\"source\\":\\"o/r\\"}]"])</script>'
    skills = _extract_rsc_skills(html)
    assert len(skills) >= 1
    assert any(s["skillId"] == "s1" for s in skills)


def test_extract_rsc_skills_from_object() -> None:
    html = (
        '<script>self.__next_f.push([1,"{\\"skillId\\":\\"s1\\",\\"name\\":\\"S1\\",\\"source\\":\\"o/r\\"}"])</script>'
    )
    skills = _extract_rsc_skills(html)
    assert len(skills) == 1


def test_extract_rsc_skills_no_match() -> None:
    html = "<html><body>No RSC data</body></html>"
    skills = _extract_rsc_skills(html)
    assert skills == []


def test_extract_rsc_skills_invalid_json() -> None:
    html = '<script>self.__next_f.push([1,"not json at all"])</script>'
    skills = _extract_rsc_skills(html)
    assert skills == []


@pytest.mark.asyncio
async def test_run_leaderboard_discovery_success(request_context: RequestContext) -> None:
    rsc_html = '<script>self.__next_f.push([1,"[{\\"skillId\\":\\"s1\\",\\"name\\":\\"S1\\",\\"installs\\":100,\\"source\\":\\"owner/repo\\"}]"])</script>'

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=rsc_html)

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await run_leaderboard_discovery(client, request_context)

    assert len(skills) == 1
    assert "owner/repo/s1" in skills
    assert "owner/repo" in repos


@pytest.mark.asyncio
async def test_run_leaderboard_discovery_failure(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection failed")

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await run_leaderboard_discovery(client, request_context)

    assert skills == {}
    assert repos == set()
