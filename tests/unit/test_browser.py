"""Tests for browser client module."""

from __future__ import annotations

from skillsight.clients.browser import BrowserClient, BrowserProbeResult


def test_browser_probe_result_defaults() -> None:
    result = BrowserProbeResult()
    assert result.urls == []


def test_browser_probe_result_with_urls() -> None:
    result = BrowserProbeResult(urls=["https://skills.sh/api/test"])
    assert len(result.urls) == 1


def test_browser_client_init() -> None:
    client = BrowserClient(headless=True)
    assert client.headless is True


def test_browser_client_init_visible() -> None:
    client = BrowserClient(headless=False)
    assert client.headless is False
