"""Completeness comparison utilities."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from skillsight.storage.jsonl import count_jsonl_rows_with_errors

if TYPE_CHECKING:
    from pathlib import Path


def _snapshot_dates(snapshots_dir: Path) -> list[date]:
    parsed: list[date] = []
    if not snapshots_dir.exists():
        return parsed
    for child in snapshots_dir.iterdir():
        if not child.is_dir():
            continue
        try:
            parsed.append(date.fromisoformat(child.name))
        except ValueError:
            continue
    parsed.sort()
    return parsed


def compare_with_previous_snapshot(
    output_dir: Path, current_snapshot_date: date, current_count: int
) -> dict[str, object]:
    """Compare current snapshot count against the most recent prior snapshot."""

    snapshots_dir = output_dir / "snapshots"
    dates = _snapshot_dates(snapshots_dir)
    previous_dates = [value for value in dates if value < current_snapshot_date]

    if not previous_dates:
        return {
            "previous_snapshot_date": None,
            "previous_count": None,
            "previous_parse_errors": None,
            "current_count": current_count,
            "delta": None,
            "delta_pct": None,
            "status": "no_baseline",
        }

    previous_date = previous_dates[-1]
    previous_path = snapshots_dir / previous_date.isoformat() / "skills_full.jsonl"
    previous_count, previous_parse_errors = count_jsonl_rows_with_errors(previous_path)
    delta = current_count - previous_count
    delta_pct = 0.0 if previous_count == 0 else delta * 100.0 / previous_count

    return {
        "previous_snapshot_date": previous_date.isoformat(),
        "previous_count": previous_count,
        "previous_parse_errors": previous_parse_errors,
        "current_count": current_count,
        "delta": delta,
        "delta_pct": delta_pct,
        "status": "degraded" if previous_parse_errors > 0 else ("ok" if delta >= 0 else "regression"),
    }
