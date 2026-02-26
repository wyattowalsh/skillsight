"""API contract helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal


def contracts_root() -> Path:
    """Return repository contract directory."""

    return Path(__file__).resolve().parents[2] / "contracts"


ContractSurface = Literal["search", "legacy"]


def _load_json_document(path: Path, *, label: str) -> dict[str, Any]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise TypeError(f"{label} contract must be a JSON object")
    return payload


def load_search_openapi() -> dict[str, Any]:
    """Load the tiny search Worker OpenAPI document."""

    path = contracts_root() / "worker_search_openapi.json"
    return _load_json_document(path, label="Search OpenAPI")


def load_legacy_worker_openapi() -> dict[str, Any]:
    """Load the frozen legacy Worker OpenAPI document."""

    path = contracts_root() / "worker_openapi.json"
    return _load_json_document(path, label="Legacy OpenAPI")


def load_openapi(surface: ContractSurface = "search") -> dict[str, Any]:
    """Load an OpenAPI document by surface name."""

    if surface == "search":
        return load_search_openapi()
    if surface == "legacy":
        return load_legacy_worker_openapi()
    raise ValueError(f"Unknown OpenAPI surface: {surface}")


def load_fixture(name: str) -> dict[str, Any]:
    """Load contract fixture by name."""

    path = contracts_root() / "fixtures" / "v1" / f"{name}.json"
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise TypeError(f"Fixture {name} must be a JSON object")
    return payload
