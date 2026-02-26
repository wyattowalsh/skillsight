"""HTML parsing helpers for skill detail and repo pages."""

from __future__ import annotations

import re

from lxml import html

from skillsight.models.skill import PlatformInstalls
from skillsight.utils.parsing import parse_compact_number

_FIRST_SEEN_RE = re.compile(r"First seen:\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", re.IGNORECASE)
_WEEKLY_RE = re.compile(r"([0-9][0-9,\.]*\s*[KMB]?)\s*/\s*week", re.IGNORECASE)
_SKILL_PATH_RE = re.compile(r"^/([a-zA-Z0-9_.-]+)/([a-zA-Z0-9_.-]+)/([a-zA-Z0-9_.-]+)$")

PLATFORM_LABELS = {
    "opencode": "opencode",
    "codex": "codex",
    "gemini-cli": "gemini_cli",
    "github-copilot": "github_copilot",
    "amp": "amp",
    "kimi-cli": "kimi_cli",
}


def _extract_text(tree: html.HtmlElement, xpath: str) -> str | None:
    """Extract text from an XPath expression, returning None if empty."""
    nodes = tree.xpath(xpath)
    if not nodes:
        return None
    value = " ".join(str(node).strip() for node in nodes if str(node).strip())
    return value or None


def validate_skill_page(tree: html.HtmlElement) -> bool:
    """Check for sentinel elements indicating a valid skill page."""
    has_canonical = bool(tree.xpath("//link[@rel='canonical']/@href"))
    has_h1 = bool(tree.xpath("//h1"))
    return has_canonical or has_h1


def parse_skill_name(tree: html.HtmlElement) -> str | None:
    """Extract skill name from h1 heading."""
    return _extract_text(tree, "//h1/text()")


def parse_skill_description(tree: html.HtmlElement) -> str | None:
    """Extract skill description from meta tag."""
    return _extract_text(tree, "//meta[@name='description']/@content")


def parse_canonical_url(tree: html.HtmlElement) -> str | None:
    """Extract canonical URL from link tag."""
    return _extract_text(tree, "//link[@rel='canonical']/@href")


def parse_og_image(tree: html.HtmlElement) -> str | None:
    """Extract Open Graph image URL."""
    return _extract_text(tree, "//meta[@property='og:image']/@content")


def parse_github_url(tree: html.HtmlElement) -> str | None:
    """Extract GitHub repository URL."""
    return _extract_text(tree, "//a[contains(@href, 'github.com')]/@href")


def parse_weekly_installs(tree: html.HtmlElement, *, full_text: str | None = None) -> tuple[str | None, int | None]:
    """Extract weekly install count. Returns (raw_text, parsed_int)."""
    all_text = full_text if full_text is not None else " ".join(tree.xpath("//text()"))
    match = _WEEKLY_RE.search(all_text)
    if not match:
        return None, None
    raw = match.group(1)
    return raw, parse_compact_number(raw)


def parse_first_seen(tree: html.HtmlElement, *, full_text: str | None = None) -> str | None:
    """Extract first seen date string."""
    all_text = full_text if full_text is not None else " ".join(tree.xpath("//text()"))
    match = _FIRST_SEEN_RE.search(all_text)
    if not match:
        return None
    return match.group(1)


def parse_platform_installs(tree: html.HtmlElement, *, full_text: str | None = None) -> PlatformInstalls | None:
    """Extract per-platform install breakdown."""
    text = full_text if full_text is not None else " ".join(tree.xpath("//text()"))
    platforms: dict[str, int] = {}
    for label, field_name in PLATFORM_LABELS.items():
        match = re.search(rf"{re.escape(label)}\s+([0-9][0-9,\.]*[KMB]?)\b", text, re.IGNORECASE)
        if match:
            parsed = parse_compact_number(match.group(1))
            if parsed is not None:
                platforms[field_name] = parsed
    if not platforms:
        return None
    return PlatformInstalls(**platforms)


def parse_install_command(tree: html.HtmlElement) -> str | None:
    """Extract install command from code/pre blocks."""
    # Look for install commands in code blocks
    for code in tree.xpath("//code/text() | //pre/text()"):
        text = str(code).strip()
        if text.startswith("npx skills add") or text.startswith("skills add"):
            return text
    return None


def parse_categories(tree: html.HtmlElement) -> list[str]:
    """Extract category/tag labels."""
    categories: list[str] = []
    for tag in tree.xpath("//a[contains(@class, 'tag') or contains(@class, 'category')]/text()"):
        text = str(tag).strip()
        if text:
            categories.append(text)
    return categories


def parse_skill_md_content(tree: html.HtmlElement) -> str | None:
    """Extract skill markdown content from the page."""
    # Look for markdown rendered content in article or main content areas
    content_nodes = tree.xpath("//article//text() | //div[contains(@class, 'markdown')]//text()")
    if not content_nodes:
        return None
    text = " ".join(str(n).strip() for n in content_nodes if str(n).strip())
    return text or None


def parse_repo_listing(page_html: str, owner: str, repo: str) -> list[tuple[str, str]]:
    """Parse a repo page and return list of (skill_id, name) tuples."""
    tree = html.fromstring(page_html)
    results: list[tuple[str, str]] = []
    seen: set[str] = set()

    for anchor in tree.xpath("//a[@href]"):
        href = anchor.get("href", "")
        match = _SKILL_PATH_RE.match(href)
        if not match:
            continue
        page_owner, page_repo, skill_id = match.groups()
        if page_owner.lower() != owner.lower() or page_repo.lower() != repo.lower():
            continue

        skill_id_lower = skill_id.lower()
        if skill_id_lower in seen:
            continue
        seen.add(skill_id_lower)

        name = " ".join(anchor.xpath(".//text()")).strip() or skill_id
        results.append((skill_id_lower, name))

    return results
