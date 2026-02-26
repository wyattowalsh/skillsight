"""HTTP client and resilience helpers."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import httpx
from aiolimiter import AsyncLimiter  # noqa: TC002
from loguru import logger
from lxml import etree
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

from skillsight import __version__

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from skillsight.settings import Settings


class SoftErrorDetected(Exception):
    """Raised when response looks successful but content indicates a soft error (e.g. CAPTCHA, empty body)."""


class RetryableStatusError(Exception):
    """Raised when a response has a retryable HTTP status code, so tenacity can retry."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"Retryable HTTP {status_code}")


class AdaptiveBlockMonitor:
    """Tracks rolling response window and decides whether browser fallback should trigger."""

    def __init__(self, window: int, threshold_percent: float) -> None:
        self.window = window
        self.threshold_percent = threshold_percent
        self._codes: deque[int] = deque(maxlen=window)

    def push_status(self, code: int) -> None:
        self._codes.append(code)

    @property
    def blocked_percent(self) -> float:
        if not self._codes:
            return 0.0
        blocked = sum(1 for code in self._codes if code in {403, 429})
        return blocked * 100.0 / len(self._codes)

    @property
    def should_escalate(self) -> bool:
        return len(self._codes) >= self.window and self.blocked_percent >= self.threshold_percent


@dataclass
class RequestContext:
    """Fetch context passed through pipeline."""

    limiter: AsyncLimiter
    monitor: AdaptiveBlockMonitor
    _limiter_loop_map: dict[int, AsyncLimiter] = field(default_factory=dict, repr=False, compare=False)

    def get_limiter(self) -> AsyncLimiter:
        """Return an AsyncLimiter bound to the current event loop.

        Each event loop gets its own limiter instance to avoid RuntimeWarning
        when a limiter created in one loop is used in another (e.g. Prefect tasks).
        """
        import asyncio

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return self.limiter

        loop_id = id(loop)
        if loop_id not in self._limiter_loop_map:
            # Create a new limiter with the same parameters as the original
            self._limiter_loop_map[loop_id] = AsyncLimiter(self.limiter.max_rate, self.limiter.time_period)
        return self._limiter_loop_map[loop_id]


def _log_before_sleep(retry_state: RetryCallState) -> None:
    """Loguru-compatible before_sleep callback for tenacity."""
    if retry_state.next_action:
        sleep = retry_state.next_action.sleep
        logger.warning(
            "Retrying {} (attempt {}), sleeping {:.1f}s",
            retry_state.fn.__name__ if retry_state.fn else "unknown",
            retry_state.attempt_number,
            sleep,
        )


async def create_http_client(
    settings: Settings, transport: httpx.AsyncBaseTransport | None = None
) -> httpx.AsyncClient:
    """Create shared async client with predictable defaults."""

    return httpx.AsyncClient(
        transport=transport,
        http2=True,
        timeout=httpx.Timeout(
            connect=5.0,
            read=settings.request_timeout,
            write=10.0,
            pool=10.0,
        ),
        limits=httpx.Limits(
            max_connections=max(20, settings.concurrency),
            max_keepalive_connections=max(10, settings.concurrency // 2),
            keepalive_expiry=30.0,
        ),
        headers={
            "User-Agent": f"skillsight/{__version__} (+https://github.com/wyattowalsh/skillsight)",
            "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        },
        follow_redirects=True,
    )


def _is_retryable_status(response: httpx.Response) -> bool:
    return response.status_code in {408, 429, 500, 502, 503, 504}


@retry(
    retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException, RetryableStatusError)),
    wait=wait_exponential(multiplier=1, min=1, max=30) + wait_random(0, 2),
    stop=stop_after_attempt(5),
    before_sleep=_log_before_sleep,
    reraise=True,
)
async def fetch_with_retry(
    client: httpx.AsyncClient,
    ctx: RequestContext,
    url: str,
    *,
    request_fn: Callable[[], Awaitable[httpx.Response]] | None = None,
) -> httpx.Response:
    """Rate-limited resilient request."""

    async with ctx.get_limiter():
        response = await (request_fn() if request_fn is not None else client.get(url))
        ctx.monitor.push_status(response.status_code)
        if _is_retryable_status(response):
            status = response.status_code
            await response.aclose()
            raise RetryableStatusError(status)
        return response


async def fetch_json(
    client: httpx.AsyncClient,
    ctx: RequestContext,
    url: str,
    *,
    request_fn: Callable[[], Awaitable[httpx.Response]] | None = None,
) -> dict[str, Any]:
    """Fetch JSON payload with retries and fail-fast on non-200."""

    response = await fetch_with_retry(client, ctx, url, request_fn=request_fn)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise TypeError(f"Expected object JSON payload from {url}")
    return payload


async def fetch_text(
    client: httpx.AsyncClient,
    ctx: RequestContext,
    url: str,
    *,
    request_fn: Callable[[], Awaitable[httpx.Response]] | None = None,
) -> str:
    """Fetch text payload with retries and fail-fast on non-200."""

    response = await fetch_with_retry(client, ctx, url, request_fn=request_fn)
    response.raise_for_status()
    return response.text


def validate_json_response(payload: dict[str, Any], *, required_keys: set[str] | None = None) -> None:
    """Validate a JSON response dict has expected structure."""

    if required_keys:
        missing = required_keys - payload.keys()
        if missing:
            raise SoftErrorDetected(f"Missing required keys in JSON response: {missing}")


def validate_html_response(html: str, *, min_length: int = 100) -> etree._Element:
    """Validate and parse HTML, raising SoftErrorDetected on suspicious content."""

    if len(html) < min_length:
        raise SoftErrorDetected(f"HTML response too short ({len(html)} chars)")
    doc = etree.HTML(html)
    if doc is None:
        raise SoftErrorDetected("Failed to parse HTML document")
    return doc
