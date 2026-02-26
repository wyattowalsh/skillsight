from datetime import UTC, datetime

from skillsight.extraction.detail_page import extract_skill_record
from skillsight.models.skill import DiscoveredSkill
from skillsight.settings import Settings

HTML = """
<html>
  <head>
    <link rel="canonical" href="https://skills.sh/vercel-labs/skills/find-skills" />
    <meta name="description" content="Find installable skills quickly" />
    <meta property="og:image" content="https://skills.sh/og.png" />
  </head>
  <body>
    <h1>find-skills</h1>
    <div>First seen: Jan 26, 2026</div>
    <div>678 / week</div>
    <a href="https://github.com/vercel-labs/skills">Repo</a>
  </body>
</html>
"""


def test_extract_structured_fields() -> None:
    discovered = DiscoveredSkill(
        id="vercel-labs/skills/find-skills",
        skill_id="find-skills",
        owner="vercel-labs",
        repo="skills",
        name="find-skills",
        installs=12345,
        discovered_via="all_time_api",
        source_endpoint="all_time_api",
        discovery_pass=1,
        rank_at_fetch=1,
        discovered_at=datetime.now(UTC),
    )

    record = extract_skill_record(discovered, HTML, Settings(), "run-1", fetched_at=datetime.now(UTC))

    assert record.name == "find-skills"
    assert record.weekly_installs == 678
    assert record.total_installs == 12345
    assert str(record.github_url) == "https://github.com/vercel-labs/skills"
