"""Runtime settings."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment."""

    model_config = SettingsConfigDict(
        env_prefix="SKILLSIGHT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    output_dir: Path = Path("./data")
    concurrency: int = Field(default=20, ge=1, le=200)
    rate_limit_per_second: float = Field(default=20.0, gt=0)
    request_timeout: float = Field(default=30.0, ge=1, le=120)

    passes_max: int = Field(default=10, ge=1, le=50)
    converge_repos: int = Field(default=2, ge=1, le=10)
    converge_growth: float = Field(default=0.1, ge=0.0, le=100.0)

    search_query_limit: int = Field(default=1000, ge=1, le=10000)
    search_batch_size: int = Field(default=50, ge=1, le=500)

    structured_only: bool = True
    resume: bool = True

    use_browser: bool = False
    browser_headless: bool = True
    browser_block_threshold_percent: float = Field(default=2.0, ge=0.0, le=100.0)
    browser_block_window: int = Field(default=500, ge=50, le=5000)

    r2_endpoint_url: str | None = None
    r2_access_key_id: SecretStr | None = None
    r2_secret_access_key: SecretStr | None = None
    r2_bucket_name: str = "skillsight-data"
    r2_prefix: str = "snapshots"
    web_export_prefix: str = "data/v1"
    web_export_page_size: int = Field(default=12, ge=1, le=500)
    r2_retain_canonical_days: int = Field(default=30, ge=1, le=3650)
    r2_retain_web_days: int = Field(default=90, ge=1, le=3650)

    github_release_enabled: bool = False
    github_release_repo: str | None = None
    kaggle_publish_enabled: bool = False
    kaggle_dataset_slug: str | None = None

    prefect_api_url: str | None = None

    parser_version: str = "0.1.0"
