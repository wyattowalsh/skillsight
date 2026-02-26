"""Tests for RSC parser module."""

from skillsight.extraction.rsc_parser import (
    extract_json_objects,
    extract_rsc_chunks,
    parse_rsc_detail_data,
    parse_rsc_skills,
)


def test_extract_rsc_chunks(rsc_payload: str) -> None:
    chunks = extract_rsc_chunks(rsc_payload)
    assert len(chunks) >= 2


def test_parse_rsc_skills(rsc_payload: str) -> None:
    skills = parse_rsc_skills(rsc_payload)
    assert len(skills) >= 2
    ids = {s.get("skillId") or s.get("id") for s in skills}
    assert "rsc-skill-one" in ids or "rsc1" in ids


def test_parse_rsc_detail_data(rsc_payload: str) -> None:
    result = parse_rsc_detail_data(rsc_payload)
    assert result is not None
    assert "skillId" in result or "name" in result


def test_extract_json_objects_from_text() -> None:
    text = 'some prefix {"skillId": "abc", "name": "test", "installs": 100} suffix'
    results = extract_json_objects(text)
    assert len(results) >= 1
    assert any(r.get("skillId") == "abc" for r in results)


def test_extract_json_objects_array() -> None:
    text = '[{"skillId": "a", "name": "A"}, {"skillId": "b", "name": "B"}]'
    results = extract_json_objects(text)
    assert len(results) >= 2


def test_extract_rsc_chunks_empty() -> None:
    chunks = extract_rsc_chunks("<html><body>No RSC here</body></html>")
    assert chunks == []


def test_parse_rsc_skills_empty() -> None:
    skills = parse_rsc_skills("<html><body>No skills</body></html>")
    assert skills == []


def test_parse_rsc_detail_data_empty() -> None:
    result = parse_rsc_detail_data("<html><body>No RSC data</body></html>")
    assert result is None


def test_extract_rsc_chunks_unicode_error() -> None:
    """Test handling of invalid unicode escapes."""
    html = '<script>self.__next_f.push([1,"\\x80invalid"])</script>'
    # Should not crash
    chunks = extract_rsc_chunks(html)
    assert isinstance(chunks, list)


def test_extract_json_objects_invalid_json() -> None:
    text = '{"skillId": broken json}'
    results = extract_json_objects(text)
    assert results == []


def test_parse_rsc_skills_deduplicates() -> None:
    html = (
        '<script>self.__next_f.push([1,"{\\"skillId\\":\\"s1\\",\\"name\\":\\"S\\"}"])</script>'
        '<script>self.__next_f.push([1,"{\\"skillId\\":\\"s1\\",\\"name\\":\\"S\\"}"])</script>'
    )
    skills = parse_rsc_skills(html)
    # Should only get 1 due to dedup
    assert len(skills) == 1
