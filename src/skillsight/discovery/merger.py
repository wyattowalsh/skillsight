"""Discovery merge helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from skillsight.models.skill import DiscoveredSkill


def merge_discovered(*groups: dict[str, DiscoveredSkill]) -> dict[str, DiscoveredSkill]:
    """Merge discovered skill dicts without overwriting earliest seen values."""

    merged: dict[str, DiscoveredSkill] = {}
    for group in groups:
        for canonical_id, skill in group.items():
            merged.setdefault(canonical_id, skill)
    return merged
