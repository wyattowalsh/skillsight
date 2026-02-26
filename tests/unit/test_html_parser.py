"""Tests for HTML parser module."""

from lxml import html

from skillsight.extraction.html_parser import (
    parse_canonical_url,
    parse_categories,
    parse_first_seen,
    parse_github_url,
    parse_install_command,
    parse_og_image,
    parse_platform_installs,
    parse_repo_listing,
    parse_skill_description,
    parse_skill_md_content,
    parse_skill_name,
    parse_weekly_installs,
    validate_skill_page,
)


def test_parse_skill_name(skill_detail_html: str) -> None:
    tree = html.fromstring(skill_detail_html)
    assert parse_skill_name(tree) == "test-skill"


def test_parse_skill_description(skill_detail_html: str) -> None:
    tree = html.fromstring(skill_detail_html)
    assert parse_skill_description(tree) == "A test skill for unit testing"


def test_parse_canonical_url(skill_detail_html: str) -> None:
    tree = html.fromstring(skill_detail_html)
    assert parse_canonical_url(tree) == "https://skills.sh/testowner/testrepo/test-skill"


def test_parse_og_image(skill_detail_html: str) -> None:
    tree = html.fromstring(skill_detail_html)
    assert parse_og_image(tree) == "https://skills.sh/og/testowner/testrepo/test-skill.png"


def test_parse_github_url(skill_detail_html: str) -> None:
    tree = html.fromstring(skill_detail_html)
    assert parse_github_url(tree) == "https://github.com/testowner/testrepo"


def test_parse_weekly_installs(skill_detail_html: str) -> None:
    tree = html.fromstring(skill_detail_html)
    raw, parsed = parse_weekly_installs(tree)
    assert raw == "1.2K"
    assert parsed == 1200


def test_parse_first_seen(skill_detail_html: str) -> None:
    tree = html.fromstring(skill_detail_html)
    result = parse_first_seen(tree)
    assert result == "Jan 15, 2025"


def test_parse_platform_installs(skill_detail_html: str) -> None:
    tree = html.fromstring(skill_detail_html)
    platform = parse_platform_installs(tree)
    assert platform is not None
    assert platform.opencode == 500
    assert platform.codex == 300
    assert platform.gemini_cli == 200
    assert platform.github_copilot == 100
    assert platform.amp == 50
    assert platform.kimi_cli == 50


def test_parse_install_command(skill_detail_html: str) -> None:
    tree = html.fromstring(skill_detail_html)
    cmd = parse_install_command(tree)
    assert cmd is not None
    assert "npx skills add" in cmd


def test_parse_categories(skill_detail_html: str) -> None:
    tree = html.fromstring(skill_detail_html)
    cats = parse_categories(tree)
    assert "testing" in cats
    assert "dev" in cats


def test_parse_skill_md_content(skill_detail_html: str) -> None:
    tree = html.fromstring(skill_detail_html)
    content = parse_skill_md_content(tree)
    assert content is not None
    assert "skill markdown content" in content


def test_validate_skill_page_valid(skill_detail_html: str) -> None:
    tree = html.fromstring(skill_detail_html)
    assert validate_skill_page(tree) is True


def test_validate_skill_page_invalid() -> None:
    tree = html.fromstring("<html><body><p>Empty page</p></body></html>")
    assert validate_skill_page(tree) is False


def test_parse_repo_listing(repo_page_html: str) -> None:
    results = parse_repo_listing(repo_page_html, "testowner", "testrepo")
    skill_ids = [sid for sid, _ in results]
    assert "skill-one" in skill_ids
    assert "skill-two" in skill_ids
    assert "skill-three" in skill_ids
    assert len(results) == 3


def test_parse_repo_listing_filters_other_repos(repo_page_html: str) -> None:
    results = parse_repo_listing(repo_page_html, "testowner", "testrepo")
    skill_ids = [sid for sid, _ in results]
    assert "unrelated" not in skill_ids


def test_parse_weekly_installs_missing() -> None:
    tree = html.fromstring("<html><body>No installs here</body></html>")
    raw, parsed = parse_weekly_installs(tree)
    assert raw is None
    assert parsed is None


def test_parse_first_seen_missing() -> None:
    tree = html.fromstring("<html><body>No date here</body></html>")
    assert parse_first_seen(tree) is None


def test_parse_platform_installs_missing() -> None:
    tree = html.fromstring("<html><body>No platforms</body></html>")
    assert parse_platform_installs(tree) is None
