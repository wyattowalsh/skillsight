"""Tests for utility functions."""

from datetime import date

from skillsight.utils.parsing import canonical_skill_id, parse_compact_number, parse_first_seen_date, split_source


def test_split_source_valid() -> None:
    assert split_source("owner/repo") == ("owner", "repo")


def test_split_source_no_slash() -> None:
    assert split_source("noslash") is None


def test_split_source_empty_owner() -> None:
    assert split_source("/repo") is None


def test_split_source_empty_repo() -> None:
    assert split_source("owner/") is None


def test_split_source_normalizes_case() -> None:
    assert split_source("Owner/Repo") == ("owner", "repo")


def test_split_source_strips_whitespace() -> None:
    assert split_source("  Owner / Repo ") == ("owner", "repo")


def test_split_source_with_extra_parts() -> None:
    assert split_source("owner/repo/extra") == ("owner", "repo")


def test_canonical_skill_id() -> None:
    assert canonical_skill_id("Owner", "Repo", "Skill") == "owner/repo/skill"


def test_canonical_skill_id_strips_whitespace() -> None:
    assert canonical_skill_id("  owner  ", " repo ", "  skill  ") == "owner/repo/skill"


def test_parse_compact_number_integer() -> None:
    assert parse_compact_number(42) == 42


def test_parse_compact_number_string_int() -> None:
    assert parse_compact_number("1234") == 1234


def test_parse_compact_number_comma() -> None:
    assert parse_compact_number("1,234") == 1234


def test_parse_compact_number_k() -> None:
    assert parse_compact_number("242.3K") == 242300


def test_parse_compact_number_m() -> None:
    assert parse_compact_number("1.2M") == 1200000


def test_parse_compact_number_b() -> None:
    assert parse_compact_number("3B") == 3000000000


def test_parse_compact_number_none() -> None:
    assert parse_compact_number(None) is None


def test_parse_compact_number_invalid() -> None:
    assert parse_compact_number("bad") is None


def test_parse_compact_number_empty() -> None:
    assert parse_compact_number("") is None


def test_parse_first_seen_date_short_month() -> None:
    result = parse_first_seen_date("Jan 15, 2025")
    assert result == date(2025, 1, 15)


def test_parse_first_seen_date_full_month() -> None:
    result = parse_first_seen_date("January 15, 2025")
    assert result == date(2025, 1, 15)


def test_parse_first_seen_date_none() -> None:
    assert parse_first_seen_date(None) is None


def test_parse_first_seen_date_empty() -> None:
    assert parse_first_seen_date("") is None


def test_parse_first_seen_date_invalid() -> None:
    assert parse_first_seen_date("not a date") is None
