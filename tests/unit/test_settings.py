"""Tests for settings module."""

from pathlib import Path

from skillsight.settings import Settings


def test_default_settings() -> None:
    s = Settings()
    assert s.output_dir == Path("./data")
    assert s.concurrency == 20
    assert s.rate_limit_per_second == 20.0
    assert s.request_timeout == 30.0
    assert s.search_query_limit == 1000
    assert s.search_batch_size == 50
    assert s.browser_headless is True
    assert s.prefect_api_url is None
    assert s.parser_version == "0.1.0"


def test_settings_convergence_defaults() -> None:
    s = Settings()
    assert s.passes_max == 10
    assert s.converge_repos == 2
    assert s.converge_growth == 0.1


def test_settings_r2_defaults() -> None:
    s = Settings()
    assert s.r2_endpoint_url is None
    assert s.r2_access_key_id is None
    assert s.r2_secret_access_key is None
    assert s.r2_bucket_name == "skillsight-data"
    assert s.r2_prefix == "snapshots"


def test_settings_override() -> None:
    s = Settings(concurrency=50, search_query_limit=500)
    assert s.concurrency == 50
    assert s.search_query_limit == 500


def test_settings_web_export_and_mirror_defaults() -> None:
    s = Settings()
    assert s.web_export_prefix == "data/v1"
    assert s.web_export_page_size == 12
    assert s.r2_retain_canonical_days == 30
    assert s.r2_retain_web_days == 90
    assert s.github_release_enabled is False
    assert s.github_release_repo is None
    assert s.kaggle_publish_enabled is False
    assert s.kaggle_dataset_slug is None
