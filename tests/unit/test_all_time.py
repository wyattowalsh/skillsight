"""Tests for all_time discovery module."""

from __future__ import annotations

import httpx
import pytest

from skillsight.clients.http import RequestContext, create_http_client
from skillsight.discovery.all_time import _crawl_all_time_once, _search_fallback
from skillsight.settings import Settings


@pytest.mark.asyncio
async def test_crawl_all_time_once_single_page(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "skills": [
                    {"skillId": "s1", "name": "S1", "source": "o/r", "installs": 100},
                    {"skillId": "s2", "name": "S2", "source": "o/r", "installs": 200},
                ],
                "hasMore": False,
            },
        )

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await _crawl_all_time_once(client, request_context, "run-1", 1)

    assert len(skills) == 2
    assert "o/r/s1" in skills
    assert "o/r" in repos


@pytest.mark.asyncio
async def test_crawl_all_time_once_multi_page(request_context: RequestContext) -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(
                200,
                json={
                    "skills": [{"skillId": "s1", "name": "S1", "source": "o/r", "installs": 100}],
                    "hasMore": True,
                },
            )
        return httpx.Response(
            200,
            json={
                "skills": [{"skillId": "s2", "name": "S2", "source": "o/r", "installs": 200}],
                "hasMore": False,
            },
        )

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await _crawl_all_time_once(client, request_context, "run-1", 1)

    assert len(skills) == 2
    assert call_count == 2


@pytest.mark.asyncio
async def test_crawl_all_time_once_bad_source(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "skills": [{"skillId": "s1", "name": "S1", "source": "noseparator"}],
                "hasMore": False,
            },
        )

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await _crawl_all_time_once(client, request_context, "run-1", 1)

    assert len(skills) == 0


@pytest.mark.asyncio
async def test_crawl_all_time_once_non_dict_item(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "skills": ["not_a_dict", {"skillId": "s1", "name": "S1", "source": "o/r"}],
                "hasMore": False,
            },
        )

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await _crawl_all_time_once(client, request_context, "run-1", 1)

    assert len(skills) == 1


@pytest.mark.asyncio
async def test_crawl_all_time_once_bad_payload_shape(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"skills": "not a list"})

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(TypeError, match="Unexpected payload shape"):
            await _crawl_all_time_once(client, request_context, "run-1", 1)


@pytest.mark.asyncio
async def test_search_fallback(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "skills": [
                    {"skillId": "new-skill", "name": "New", "source": "o/r", "installs": 50},
                ],
            },
        )

    settings = Settings()
    existing: dict = {}
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        result, repos = await _search_fallback(client, request_context, existing, 1)

    assert len(result) >= 1
    assert "o/r" in repos


@pytest.mark.asyncio
async def test_search_fallback_skips_existing(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "skills": [
                    {"skillId": "existing", "name": "Existing", "source": "o/r", "installs": 50},
                ],
            },
        )

    from datetime import UTC, datetime

    from skillsight.models.skill import DiscoveredSkill

    existing = {
        "o/r/existing": DiscoveredSkill(
            id="o/r/existing",
            skill_id="existing",
            owner="o",
            repo="r",
            name="Existing",
            discovered_via="all_time_api",
            source_endpoint="all_time_api",
            discovered_at=datetime.now(UTC),
        )
    }
    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        result, repos = await _search_fallback(client, request_context, existing, 1)

    # Existing skill should not be replaced
    assert result["o/r/existing"].discovered_via == "all_time_api"


@pytest.mark.asyncio
async def test_search_fallback_non_list_skills(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"skills": "not a list"})

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        result, repos = await _search_fallback(client, request_context, {}, 1)

    assert result == {}
