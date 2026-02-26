"""Parsing and normalization helpers."""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal

_COMPACT_NUMBER_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMB])?\s*$", re.IGNORECASE)

_DATE_FORMATS = ("%b %d, %Y", "%B %d, %Y")


def split_source(source: str) -> tuple[str, str] | None:
    """Split a source string like 'owner/repo' into (owner, repo), normalized to lowercase.

    Returns None if the source cannot be split or has empty parts.
    """
    parts = source.split("/")
    if len(parts) < 2:
        return None
    owner = parts[0].strip().lower()
    repo = parts[1].strip().lower()
    if not owner or not repo:
        return None
    return owner, repo


def canonical_skill_id(owner: str, repo: str, skill_id: str) -> str:
    """Build canonical lowercase skill identifier."""

    owner_clean = owner.strip().lower()
    repo_clean = repo.strip().lower()
    skill_clean = skill_id.strip().lower()
    return f"{owner_clean}/{repo_clean}/{skill_clean}"


def parse_compact_number(value: str | int | None) -> int | None:
    """Parse number strings such as 1.2K into integers."""

    if value is None:
        return None
    if isinstance(value, int):
        return value

    text = value.strip().replace(",", "")
    if text.isdigit():
        return int(text)

    match = _COMPACT_NUMBER_RE.match(text)
    if not match:
        return None

    number = Decimal(match.group(1))
    suffix = (match.group(2) or "").upper()

    if suffix == "K":
        number *= 1_000
    elif suffix == "M":
        number *= 1_000_000
    elif suffix == "B":
        number *= 1_000_000_000

    return int(number)


def parse_first_seen_date(raw: str | None) -> date | None:
    """Parse first seen label into date."""

    if raw is None:
        return None
    cleaned = raw.strip()
    if not cleaned:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None
