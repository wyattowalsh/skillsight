"""Tests for pipeline package init."""

from __future__ import annotations

from skillsight.pipeline import skillsight_pipeline


def test_pipeline_import() -> None:
    assert skillsight_pipeline is not None
    assert hasattr(skillsight_pipeline, "fn")
