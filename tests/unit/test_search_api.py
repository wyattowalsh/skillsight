"""Tests for search API discovery module."""

from __future__ import annotations

import httpx
import pytest

from skillsight.clients.http import RequestContext, create_http_client
from skillsight.discovery.search_api import _search_one_query, run_search_api_sweep
from skillsight.settings import Settings


@pytest.mark.asyncio
async def test_search_one_query_success(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "skills": [
                    {"skillId": "skill-a", "name": "Skill A", "source": "owner/repo", "installs": 100},
                    {"skillId": "skill-b", "name": "Skill B", "source": "owner/repo", "installs": 200},
                ]
            },
        )

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await _search_one_query(client, request_context, "ab", 1000)

    assert len(skills) == 2
    assert "owner/repo/skill-a" in skills
    assert "owner/repo" in repos


@pytest.mark.asyncio
async def test_search_one_query_400(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400)

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await _search_one_query(client, request_context, "zz", 1000)

    assert skills == {}
    assert repos == set()


@pytest.mark.asyncio
async def test_search_one_query_invalid_json(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="not json")

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await _search_one_query(client, request_context, "xy", 1000)

    assert skills == {}


@pytest.mark.asyncio
async def test_search_one_query_no_skills_key(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"results": []})

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await _search_one_query(client, request_context, "ab", 1000)

    assert skills == {}


@pytest.mark.asyncio
async def test_search_one_query_bad_source(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"skills": [{"skillId": "x", "name": "X", "source": "noseparator"}]})

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await _search_one_query(client, request_context, "ab", 1000)

    assert skills == {}


@pytest.mark.asyncio
async def test_search_one_query_non_int_installs(request_context: RequestContext) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"skills": [{"skillId": "x", "name": "X", "source": "o/r", "installs": "many"}]}
        )

    settings = Settings()
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos = await _search_one_query(client, request_context, "ab", 1000)

    assert len(skills) == 1
    assert skills["o/r/x"].installs is None


@pytest.mark.asyncio
async def test_run_search_api_sweep_sample(request_context: RequestContext) -> None:
    """Test search API sweep with sample limiting."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(200, json={"skills": []})

    settings = Settings(search_batch_size=10, search_query_limit=100)
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos, completed = await run_search_api_sweep(client, request_context, settings, sample=3)

    assert len(completed) == 3
    assert call_count == 3
    assert skills == {}
    assert repos == set()


@pytest.mark.asyncio
async def test_search_one_query_retryable_status(request_context: RequestContext) -> None:
    """When fetch_with_retry exhausts retries on 429, _search_one_query catches RetryableStatusError."""
    from tenacity import wait_none

    from skillsight.clients.http import fetch_with_retry

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429)

    settings = Settings()
    original_wait = fetch_with_retry.retry.wait
    fetch_with_retry.retry.wait = wait_none()
    try:
        async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
            skills, repos = await _search_one_query(client, request_context, "zz", 1000)
    finally:
        fetch_with_retry.retry.wait = original_wait

    assert skills == {}
    assert repos == set()


@pytest.mark.asyncio
async def test_run_search_api_sweep_with_completed(request_context: RequestContext) -> None:
    """Test search API sweep skips completed queries."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(200, json={"skills": []})

    settings = Settings(search_batch_size=10, search_query_limit=100)
    from skillsight.discovery.search_api import generate_two_char_queries

    all_queries = generate_two_char_queries()
    completed_set = set(all_queries[:-2])

    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        skills, repos, completed = await run_search_api_sweep(
            client, request_context, settings, completed_queries=completed_set
        )

    assert call_count == 2
    assert len(completed) == 2
