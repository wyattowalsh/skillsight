"""Quality report utilities."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from skillsight.models.skill import SkillRecord

CORE_FIELDS = ["name", "description", "total_installs"]


def build_quality_report(records: list[SkillRecord], failures: dict[str, str]) -> dict[str, Any]:
    """Create quality summary for extraction output."""

    total = len(records)
    missing_counts = Counter[str]()

    for record in records:
        if not record.name:
            missing_counts["name"] += 1
        if not record.description:
            missing_counts["description"] += 1
        if record.total_installs is None:
            missing_counts["total_installs"] += 1

    coverage = {field: 0.0 if total == 0 else (total - missing_counts[field]) * 100.0 / total for field in CORE_FIELDS}

    return {
        "total_records": total,
        "failures": len(failures),
        "failure_ids": sorted(failures.keys())[:100],
        "missing_counts": dict(missing_counts),
        "coverage": coverage,
    }
