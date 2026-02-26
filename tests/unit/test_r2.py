"""Tests for R2 upload helpers."""

from __future__ import annotations

import pytest

from skillsight.clients.r2 import can_upload, upload_bytes, upload_file
from skillsight.settings import Settings


def test_can_upload_no_credentials() -> None:
    settings = Settings()
    assert can_upload(settings) is False


def test_can_upload_with_credentials() -> None:
    settings = Settings(
        r2_endpoint_url="https://r2.example.com",
        r2_access_key_id="test-key",
        r2_secret_access_key="test-secret",
        r2_bucket_name="test-bucket",
    )
    assert can_upload(settings) is True


def test_can_upload_partial_credentials() -> None:
    settings = Settings(
        r2_endpoint_url="https://r2.example.com",
        r2_access_key_id=None,
        r2_secret_access_key="test-secret",
        r2_bucket_name="test-bucket",
    )
    assert can_upload(settings) is False


def test_upload_file_no_credentials(tmp_path) -> None:
    settings = Settings()
    path = tmp_path / "test.txt"
    path.write_text("hello")
    with pytest.raises(RuntimeError, match="R2 credentials are incomplete"):
        upload_file(settings, path, "test/key")


def test_upload_bytes_no_credentials() -> None:
    settings = Settings()
    with pytest.raises(RuntimeError, match="R2 credentials are incomplete"):
        upload_bytes(settings, b"hello", "test/key")
