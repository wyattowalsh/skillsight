"""Tests for contracts module."""

from __future__ import annotations

import json

import pytest

from skillsight.contracts import (
    contracts_root,
    load_fixture,
    load_legacy_worker_openapi,
    load_openapi,
    load_search_openapi,
)


def test_contracts_root_exists() -> None:
    root = contracts_root()
    assert root.exists()
    assert root.is_dir()


def test_load_openapi() -> None:
    spec = load_openapi()
    assert isinstance(spec, dict)
    assert "paths" in spec
    assert "/v1/search" in spec["paths"]


def test_load_openapi_has_paths() -> None:
    spec = load_legacy_worker_openapi()
    paths = spec["paths"]
    assert "/v1/skills" in paths
    assert "/v1/skills/{id}" in paths


def test_load_search_openapi_has_paths() -> None:
    spec = load_search_openapi()
    paths = spec["paths"]
    assert "/v1/search" in paths
    assert "/healthz" in paths


def test_load_openapi_surface_selector() -> None:
    assert "/v1/search" in load_openapi("search")["paths"]
    assert "/v1/skills" in load_openapi("legacy")["paths"]
    with pytest.raises(ValueError):
        load_openapi("nope")


def test_load_fixture() -> None:
    fixture = load_fixture("skill_detail")
    assert isinstance(fixture, dict)


def test_static_contract_files_exist_and_parse() -> None:
    root = contracts_root()
    for name in (
        "static_latest.schema.json",
        "static_leaderboard_page.schema.json",
        "static_skill_list_item.schema.json",
        "static_search_index.schema.json",
        "worker_search_openapi.json",
    ):
        payload = json.loads((root / name).read_text())
        assert isinstance(payload, dict)


def test_search_openapi_documents_error_schemas_and_headers() -> None:
    spec = load_search_openapi()
    paths = spec["paths"]
    search_get = paths["/v1/search"]["get"]
    responses = search_get["responses"]

    for code in ("400", "404", "405", "429", "500", "503"):
        assert code in responses

    schemas = spec["components"]["schemas"]
    assert "ErrorResponse" in schemas
    assert "RateLimitErrorResponse" in schemas

    ok_headers = responses["200"].get("headers", {})
    assert "Cache-Control" in ok_headers
    assert "X-RateLimit-Limit" in ok_headers
    assert "X-RateLimit-Remaining" in ok_headers
    assert "X-RateLimit-Reset" in ok_headers

    ratelimit_headers = responses["429"].get("headers", {})
    assert "Retry-After" in ratelimit_headers
