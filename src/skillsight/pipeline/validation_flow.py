"""Validation and completeness checks."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from prefect import flow

from skillsight.storage.quality import build_quality_report

if TYPE_CHECKING:
    from skillsight.models.skill import SkillRecord


@flow(name="skillsight-validation")
def validation_flow(
    records: list[SkillRecord],
    failures: dict[str, str],
    discovery_summary: dict[str, Any],
) -> dict[str, object]:
    """Validate extraction quality and completeness posture."""

    quality = build_quality_report(records, failures)
    quality["total_discovered"] = discovery_summary.get("total_skills", 0)
    quality["total_repos"] = discovery_summary.get("total_repos", 0)
    quality["discovery_by_source"] = discovery_summary.get("by_source", {})
    return quality


def verify_completeness(current_total: int, baseline_total: int) -> dict[str, object]:
    """Compute baseline delta and pass/fail gate."""

    delta = current_total - baseline_total
    delta_pct = 0.0 if baseline_total <= 0 else delta * 100.0 / baseline_total
    status = "ok" if current_total >= baseline_total else "regression"
    return {
        "status": status,
        "current_total": current_total,
        "baseline_total": baseline_total,
        "delta": delta,
        "delta_pct": delta_pct,
    }
