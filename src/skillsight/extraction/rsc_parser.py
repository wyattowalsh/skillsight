"""React Server Component (RSC) payload parser.

Extracts structured data from Next.js RSC payloads embedded in HTML script tags.
"""

from __future__ import annotations

import json
import re
from typing import Any

from loguru import logger

# Pattern matching Next.js RSC push scripts
_RSC_PUSH_RE = re.compile(r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)', re.DOTALL)


def extract_rsc_chunks(html_content: str) -> list[str]:
    """Extract raw RSC payload chunks from HTML script tags."""
    chunks: list[str] = []
    for match in _RSC_PUSH_RE.finditer(html_content):
        raw = match.group(1)
        try:
            decoded = json.loads(f'"{raw}"')
        except (json.JSONDecodeError, ValueError):
            decoded = raw
        chunks.append(decoded)
    return chunks


def extract_json_objects(text: str) -> list[dict[str, Any]]:
    """Extract JSON objects/arrays from a text string."""
    results: list[dict[str, Any]] = []
    # Find JSON arrays
    for match in re.finditer(r'\[(?:[^\[\]]*"(?:skillId|id)"[^\[\]]*)\]', text):
        try:
            arr = json.loads(match.group())
            if isinstance(arr, list):
                for item in arr:
                    if isinstance(item, dict):
                        results.append(item)
        except (json.JSONDecodeError, ValueError):
            continue

    # Find JSON objects
    for match in re.finditer(r'\{[^{}]*"(?:skillId|id)"[^{}]*\}', text):
        try:
            obj = json.loads(match.group())
            if isinstance(obj, dict):
                results.append(obj)
        except (json.JSONDecodeError, ValueError):
            continue

    return results


def parse_rsc_skills(html_content: str) -> list[dict[str, Any]]:
    """Parse RSC payloads from HTML and extract skill-like data objects.

    Returns list of dicts with keys like: id, skillId, name, installs, source.
    """
    chunks = extract_rsc_chunks(html_content)
    skills: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for chunk in chunks:
        objects = extract_json_objects(chunk)
        for obj in objects:
            skill_id = obj.get("skillId") or obj.get("id")
            if skill_id and str(skill_id) not in seen_ids:
                seen_ids.add(str(skill_id))
                skills.append(obj)

    logger.debug("RSC parser extracted {} skill objects", len(skills))
    return skills


def parse_rsc_detail_data(html_content: str) -> dict[str, Any] | None:
    """Extract skill detail data from RSC payload on a detail page.

    Returns the first skill-like object found, or None.
    """
    chunks = extract_rsc_chunks(html_content)
    for chunk in chunks:
        objects = extract_json_objects(chunk)
        for obj in objects:
            if "skillId" in obj or ("name" in obj and "installs" in obj):
                return obj
    return None
