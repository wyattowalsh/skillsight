"""Tests for discovery modules."""

from skillsight.discovery.leaderboard import parse_leaderboard_html
from skillsight.discovery.merger import merge_discovered
from skillsight.discovery.repo_pages import parse_repo_page
from skillsight.discovery.search_api import generate_two_char_queries
from skillsight.discovery.sitemap import parse_sitemap_xml


def test_generate_two_char_queries() -> None:
    queries = generate_two_char_queries()
    assert len(queries) == 1296
    assert "aa" in queries
    assert "z9" in queries
    assert "00" in queries
    assert all(len(q) == 2 for q in queries)


def test_parse_sitemap_xml(sitemap_xml: str) -> None:
    skills = parse_sitemap_xml(sitemap_xml)
    assert len(skills) == 3
    ids = {s.id for s in skills}
    assert "testowner/testrepo/skill-alpha" in ids
    assert "testowner/testrepo/skill-beta" in ids
    assert "otherowner/otherrepo/skill-gamma" in ids


def test_parse_sitemap_filters_non_skill_urls(sitemap_xml: str) -> None:
    skills = parse_sitemap_xml(sitemap_xml)
    ids = {s.id for s in skills}
    # Homepage and repo-level URLs should be filtered out
    assert all("/" in sid and sid.count("/") == 2 for sid in ids)


def test_parse_sitemap_sets_discovery_source(sitemap_xml: str) -> None:
    skills = parse_sitemap_xml(sitemap_xml)
    assert all(s.discovered_via == "sitemap" for s in skills)


def test_parse_sitemap_invalid_xml() -> None:
    skills = parse_sitemap_xml("not xml at all")
    assert skills == []


def test_parse_leaderboard_html(rsc_payload: str) -> None:
    skills = parse_leaderboard_html(rsc_payload)
    assert len(skills) >= 1
    assert all(s.discovered_via == "leaderboard" for s in skills)


def test_parse_leaderboard_empty() -> None:
    skills = parse_leaderboard_html("<html><body>No RSC data</body></html>")
    assert skills == []


def test_merger_dedup() -> None:
    from datetime import UTC, datetime

    from skillsight.models.skill import DiscoveredSkill

    now = datetime.now(UTC)
    group_a = {
        "o/r/a": DiscoveredSkill(
            id="o/r/a",
            skill_id="a",
            owner="o",
            repo="r",
            name="A",
            discovered_via="search_api",
            source_endpoint="search_api",
            discovered_at=now,
        ),
        "o/r/b": DiscoveredSkill(
            id="o/r/b",
            skill_id="b",
            owner="o",
            repo="r",
            name="B",
            discovered_via="search_api",
            source_endpoint="search_api",
            discovered_at=now,
        ),
    }
    group_b = {
        "o/r/b": DiscoveredSkill(
            id="o/r/b",
            skill_id="b",
            owner="o",
            repo="r",
            name="B-dup",
            discovered_via="sitemap",
            source_endpoint="sitemap",
            discovered_at=now,
        ),
        "o/r/c": DiscoveredSkill(
            id="o/r/c",
            skill_id="c",
            owner="o",
            repo="r",
            name="C",
            discovered_via="sitemap",
            source_endpoint="sitemap",
            discovered_at=now,
        ),
    }

    merged = merge_discovered(group_a, group_b)
    assert len(merged) == 3
    # First seen wins - group_a's version of "o/r/b"
    assert merged["o/r/b"].name == "B"
    assert merged["o/r/b"].discovered_via == "search_api"


def test_parse_repo_page(repo_page_html: str) -> None:
    found = parse_repo_page("testowner", "testrepo", repo_page_html)
    assert len(found) == 3
    assert "testowner/testrepo/skill-one" in found
    assert "testowner/testrepo/skill-two" in found
    assert "testowner/testrepo/skill-three" in found


def test_parse_repo_page_ignores_other_repos(repo_page_html: str) -> None:
    found = parse_repo_page("testowner", "testrepo", repo_page_html)
    assert "otherown/otherrepo/unrelated" not in found
