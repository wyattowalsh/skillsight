"""Tests for HTTP client module."""

import httpx
import pytest
from aiolimiter import AsyncLimiter

from skillsight.clients.http import (
    AdaptiveBlockMonitor,
    RequestContext,
    RetryableStatusError,
    SoftErrorDetected,
    create_http_client,
    fetch_json,
    fetch_text,
    fetch_with_retry,
    validate_html_response,
    validate_json_response,
)
from skillsight.settings import Settings


def test_adaptive_block_monitor_no_data() -> None:
    monitor = AdaptiveBlockMonitor(window=10, threshold_percent=50.0)
    assert monitor.blocked_percent == 0.0
    assert monitor.should_escalate is False


def test_adaptive_block_monitor_tracking() -> None:
    monitor = AdaptiveBlockMonitor(window=5, threshold_percent=40.0)
    for _ in range(3):
        monitor.push_status(200)
    for _ in range(2):
        monitor.push_status(403)
    assert monitor.blocked_percent == 40.0
    assert monitor.should_escalate is True


def test_adaptive_block_monitor_below_window() -> None:
    monitor = AdaptiveBlockMonitor(window=100, threshold_percent=50.0)
    monitor.push_status(403)
    assert monitor.should_escalate is False


def test_request_context() -> None:
    ctx = RequestContext(
        limiter=AsyncLimiter(10, 1),
        monitor=AdaptiveBlockMonitor(window=10, threshold_percent=50.0),
    )
    assert ctx.limiter is not None
    assert ctx.monitor is not None


@pytest.mark.asyncio
async def test_create_http_client() -> None:
    settings = Settings()
    client = await create_http_client(settings)
    assert isinstance(client, httpx.AsyncClient)
    await client.aclose()


def test_validate_json_response_ok() -> None:
    validate_json_response({"id": "a", "name": "b"}, required_keys={"id", "name"})


def test_validate_json_response_missing_keys() -> None:
    with pytest.raises(SoftErrorDetected):
        validate_json_response({"id": "a"}, required_keys={"id", "name"})


def test_validate_json_response_no_required() -> None:
    validate_json_response({"anything": "works"})


def test_validate_html_response_ok() -> None:
    html = "<html><head><title>Test</title></head><body>" + "x" * 100 + "</body></html>"
    doc = validate_html_response(html)
    assert doc is not None


def test_validate_html_response_too_short() -> None:
    with pytest.raises(SoftErrorDetected):
        validate_html_response("<p>tiny</p>")


@pytest.mark.asyncio
async def test_fetch_with_retry_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    settings = Settings()
    ctx = RequestContext(
        limiter=AsyncLimiter(100, 1),
        monitor=AdaptiveBlockMonitor(window=10, threshold_percent=50.0),
    )
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        response = await fetch_with_retry(client, ctx, "https://example.com/test")
        assert response.status_code == 200


@pytest.mark.asyncio
async def test_fetch_json_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"skills": []})

    settings = Settings()
    ctx = RequestContext(
        limiter=AsyncLimiter(100, 1),
        monitor=AdaptiveBlockMonitor(window=10, threshold_percent=50.0),
    )
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        payload = await fetch_json(client, ctx, "https://example.com/api")
        assert "skills" in payload


@pytest.mark.asyncio
async def test_fetch_text_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>Hello</html>")

    settings = Settings()
    ctx = RequestContext(
        limiter=AsyncLimiter(100, 1),
        monitor=AdaptiveBlockMonitor(window=10, threshold_percent=50.0),
    )
    async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
        text = await fetch_text(client, ctx, "https://example.com/page")
        assert "Hello" in text


def test_soft_error_detected() -> None:
    exc = SoftErrorDetected("test error")
    assert str(exc) == "test error"


def test_retryable_status_error() -> None:
    exc = RetryableStatusError(429)
    assert exc.status_code == 429
    assert "429" in str(exc)


def test_get_limiter_no_event_loop() -> None:
    """When called outside an async context (no running loop), should return the base limiter."""
    ctx = RequestContext(
        limiter=AsyncLimiter(10, 1),
        monitor=AdaptiveBlockMonitor(window=10, threshold_percent=50.0),
    )
    result = ctx.get_limiter()
    assert result is ctx.limiter


@pytest.mark.asyncio
async def test_fetch_with_retry_retryable_status() -> None:
    """fetch_with_retry raises RetryableStatusError after exhausting retries on 429."""
    from tenacity import wait_none

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429)

    settings = Settings()
    ctx = RequestContext(
        limiter=AsyncLimiter(100, 1),
        monitor=AdaptiveBlockMonitor(window=10, threshold_percent=50.0),
    )
    original_wait = fetch_with_retry.retry.wait
    fetch_with_retry.retry.wait = wait_none()
    try:
        async with await create_http_client(settings, transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(RetryableStatusError) as exc_info:
                await fetch_with_retry(client, ctx, "https://example.com/test")
            assert exc_info.value.status_code == 429
    finally:
        fetch_with_retry.retry.wait = original_wait
