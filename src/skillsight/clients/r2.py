"""R2 upload helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from skillsight.settings import Settings


def can_upload(settings: Settings) -> bool:
    """Check if all required R2 credentials are configured."""

    return (
        settings.r2_endpoint_url is not None
        and settings.r2_access_key_id is not None
        and settings.r2_secret_access_key is not None
        and bool(settings.r2_bucket_name)
    )


def _get_store(settings: Settings):
    """Create S3Store for R2 uploads."""
    if not can_upload(settings):
        raise RuntimeError("R2 credentials are incomplete.")
    try:
        from obstore.store import S3Store
    except ImportError as exc:
        raise RuntimeError("obstore is not installed.") from exc
    return S3Store(
        endpoint=settings.r2_endpoint_url,
        region="auto",
        bucket=settings.r2_bucket_name,
        access_key_id=settings.r2_access_key_id.get_secret_value(),
        secret_access_key=settings.r2_secret_access_key.get_secret_value(),
    )


def upload_file(settings: Settings, local_path: Path, key: str) -> str:
    """Upload one file to R2 bucket using obstore if available."""
    store = _get_store(settings)
    with local_path.open("rb") as f:
        store.put(key, f.read())
    return f"s3://{settings.r2_bucket_name}/{key}"


def upload_bytes(settings: Settings, data: bytes, key: str) -> str:
    """Upload raw bytes to R2."""
    store = _get_store(settings)
    store.put(key, data)
    return f"s3://{settings.r2_bucket_name}/{key}"
