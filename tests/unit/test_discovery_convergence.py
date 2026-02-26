from __future__ import annotations

from collections import defaultdict

import httpx
import pytest
from aiolimiter import AsyncLimiter

from skillsight.clients.http import AdaptiveBlockMonitor, RequestContext, create_http_client
from skillsight.discovery.all_time import run_convergence_discovery
from skillsight.settings import Settings


def _response(payload: dict) -> httpx.Response:
    return httpx.Response(status_code=200, json=payload)


@pytest.mark.asyncio
async def test_convergence_reaches_stable_union() -> None:
    call_counts = defaultdict(int)

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.startswith("/api/skills/all-time/"):
            page = int(path.rsplit("/", maxsplit=1)[1])
            if page == 1:
                call_counts["pass"] += 1
            current_pass = call_counts["pass"]

            if page == 1:
                if current_pass == 1:
                    return _response(
                        {
                            "skills": [
                                {"source": "o/r", "skillId": "a", "name": "a", "installs": 1},
                                {"source": "o/r", "skillId": "b", "name": "b", "installs": 1},
                            ],
                            "hasMore": True,
                            "page": 1,
                            "total": 3,
                        }
                    )
                return _response(
                    {
                        "skills": [
                            {"source": "o/r", "skillId": "a", "name": "a", "installs": 1},
                            {"source": "o/r", "skillId": "b", "name": "b", "installs": 1},
                            {"source": "o/r", "skillId": "d", "name": "d", "installs": 1},
                        ],
                        "hasMore": True,
                        "page": 1,
                        "total": 4,
                    }
                )
            if page == 2:
                return _response(
                    {
                        "skills": [{"source": "o/r", "skillId": "c", "name": "c", "installs": 1}],
                        "hasMore": False,
                        "page": 2,
                        "total": 4,
                    }
                )
        return httpx.Response(status_code=404, text="not found")

    settings = Settings(passes_max=5, converge_repos=2, converge_growth=0.1)
    ctx = RequestContext(
        limiter=AsyncLimiter(100, 1),
        monitor=AdaptiveBlockMonitor(window=10, threshold_percent=50.0),
    )

    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        discovered, repos, report = await run_convergence_discovery(client, ctx, settings, "run-1")

    assert len(discovered) == 4
    assert repos == {"o/r"}
    assert report.converged is True
    assert report.passes_executed >= 3
