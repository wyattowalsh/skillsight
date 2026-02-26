"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest
from aiolimiter import AsyncLimiter

from skillsight.clients.http import AdaptiveBlockMonitor, RequestContext

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def request_context() -> RequestContext:
    return RequestContext(
        limiter=AsyncLimiter(100, 1),
        monitor=AdaptiveBlockMonitor(window=10, threshold_percent=50.0),
    )


@pytest.fixture
def tmp_output_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture
def skill_detail_html() -> str:
    return (FIXTURES_DIR / "skill_detail_page.html").read_text()


@pytest.fixture
def repo_page_html() -> str:
    return (FIXTURES_DIR / "repo_page.html").read_text()


@pytest.fixture
def sitemap_xml() -> str:
    return (FIXTURES_DIR / "sitemap.xml").read_text()


@pytest.fixture
def rsc_payload() -> str:
    return (FIXTURES_DIR / "rsc_payload.txt").read_text()


@pytest.fixture
def search_response_json() -> dict:
    import json

    return json.loads((FIXTURES_DIR / "search_response.json").read_text())
