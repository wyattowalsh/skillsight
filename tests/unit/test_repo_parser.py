from skillsight.discovery.repo_pages import parse_repo_page

HTML = """
<html>
  <body>
    <a href="/vercel-labs/skills/find-skills">Find Skills</a>
    <a href="/vercel-labs/skills/project-context">Project Context</a>
    <a href="/other/repo/skip">Skip</a>
  </body>
</html>
"""


def test_parse_repo_page_extracts_matching_skill_links() -> None:
    found = parse_repo_page("vercel-labs", "skills", HTML)
    assert sorted(found.keys()) == [
        "vercel-labs/skills/find-skills",
        "vercel-labs/skills/project-context",
    ]
