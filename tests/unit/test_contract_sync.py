"""Verify Python models stay in sync with OpenAPI contract."""

from __future__ import annotations

from typing import get_args

from skillsight.contracts import load_fixture, load_legacy_worker_openapi
from skillsight.models.skill import DiscoverySource, SkillRecord


def test_skill_record_fields_match_openapi():
    """Every field in SkillRecord must exist in the OpenAPI SkillRecord schema."""
    schema = load_legacy_worker_openapi()
    openapi_props = set(schema["components"]["schemas"]["SkillRecord"]["properties"].keys())
    model_fields = set(SkillRecord.model_fields.keys())

    missing_from_openapi = model_fields - openapi_props
    missing_from_model = openapi_props - model_fields

    assert not missing_from_openapi, f"Fields in Python model but not OpenAPI: {missing_from_openapi}"
    assert not missing_from_model, f"Fields in OpenAPI but not Python model: {missing_from_model}"


def test_discovery_source_enum_matches_openapi():
    """DiscoverySource literal values must match OpenAPI enum."""
    schema = load_legacy_worker_openapi()
    # Find the enum in the OpenAPI spec
    openapi_enum = set(schema["components"]["schemas"]["SkillRecord"]["properties"]["discovery_source"]["enum"])
    # Get Python Literal values
    python_values = set(get_args(DiscoverySource))

    assert openapi_enum == python_values, f"Enum mismatch - OpenAPI: {openapi_enum}, Python: {python_values}"


def test_fixtures_validate_against_models():
    """Contract fixtures must parse as valid Pydantic models."""
    detail = load_fixture("skill_detail")
    SkillRecord.model_validate(detail)
