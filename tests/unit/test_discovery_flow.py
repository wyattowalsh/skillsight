"""Tests for discovery flow module."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from skillsight.clients.http import RequestContext, create_http_client
from skillsight.models.checkpoint import DiscoveryCheckpoint
from skillsight.pipeline.discovery_flow import discovery_flow
from skillsight.settings import Settings
from skillsight.storage.checkpoint import save_checkpoint


@pytest.mark.asyncio
async def test_discovery_flow_basic(request_context: RequestContext, tmp_path) -> None:
    """Test discovery flow with mocked HTTP returning minimal data."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "sitemap.xml" in url:
            xml = """<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>https://skills.sh/o/r/sitemap-skill</loc></url>
            </urlset>"""
            return httpx.Response(200, text=xml)
        if "api/search" in url:
            return httpx.Response(
                200, json={"skills": [{"skillId": "search-skill", "name": "Search", "source": "o/r", "installs": 50}]}
            )
        # Homepage for leaderboard
        if url.rstrip("/") == "https://skills.sh":
            return httpx.Response(200, text="<html><body>No RSC</body></html>")
        # Repo page expansion
        if "/o/r" in url and "api" not in url:
            return httpx.Response(200, text='<html><body><a href="/o/r/repo-skill">RS</a></body></html>')
        return httpx.Response(200, text="<html></html>")

    settings = Settings(output_dir=tmp_path, search_batch_size=5)
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        discovered, summary = await discovery_flow(settings, "run-1", client, request_context, sample=2)

    assert len(discovered) >= 1
    assert "total_skills" in summary
    assert "by_source" in summary

    # Check output files
    assert (tmp_path / "discovery" / "discovered_skills.jsonl").exists()
    assert (tmp_path / "discovery" / "repos.json").exists()
    assert (tmp_path / "checkpoints" / "discovery_state.json").exists()


@pytest.mark.asyncio
async def test_discovery_flow_resumes_when_checkpoint_run_id_differs(
    request_context: RequestContext, tmp_path
) -> None:
    searched_queries: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "sitemap.xml" in url:
            xml = """<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"></urlset>"""
            return httpx.Response(200, text=xml)
        if "api/search" in url:
            searched_queries.append(request.url.params.get("q", ""))
            return httpx.Response(200, json={"skills": []})
        if url.rstrip("/") == "https://skills.sh":
            return httpx.Response(200, text="<html><body>No RSC</body></html>")
        return httpx.Response(200, text="<html></html>")

    save_checkpoint(
        tmp_path / "checkpoints" / "discovery_state.json",
        DiscoveryCheckpoint(
            run_id="previous-run",
            search_queries_completed={"aa"},
            started_at=datetime.now(UTC),
            last_updated=datetime.now(UTC),
        ),
    )

    settings = Settings(output_dir=tmp_path, search_batch_size=1)
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        await discovery_flow(settings, "new-run", client, request_context, sample=1)

    assert searched_queries == ["ab"]
