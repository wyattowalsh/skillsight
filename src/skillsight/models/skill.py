"""Core data models for discovery and extraction."""

from __future__ import annotations

from datetime import date, datetime  # noqa: TC003
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

DiscoverySource = Literal["all_time_api", "search_api", "sitemap", "leaderboard", "repo_page", "browser"]


class PlatformInstalls(BaseModel):
    """Per-platform install breakdown."""

    model_config = ConfigDict(extra="allow")

    opencode: int | None = None
    codex: int | None = None
    gemini_cli: int | None = None
    github_copilot: int | None = None
    amp: int | None = None
    kimi_cli: int | None = None


class DiscoveredSkill(BaseModel):
    """Minimal discovery record.

    ``discovered_via`` is the high-level discovery method that first found this
    skill (e.g. ``"sitemap"``, ``"search_api"``).  ``source_endpoint`` records
    the specific API or page type the skill was fetched from, which today is
    always identical to ``discovered_via`` but is kept as a separate field so
    future multi-hop discovery (e.g. sitemap -> repo_page) can distinguish
    origin from fetch location.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    id: str
    skill_id: str
    owner: str
    repo: str
    name: str
    installs: int | None = None
    discovered_via: DiscoverySource
    source_endpoint: DiscoverySource
    discovery_pass: int = Field(default=1, ge=1)
    rank_at_fetch: int | None = Field(default=None, ge=1)
    discovered_at: datetime


class SkillRecord(BaseModel):
    """Structured skill record used for snapshot output."""

    model_config = ConfigDict(str_strip_whitespace=True, populate_by_name=True)

    id: str
    skill_id: str
    owner: str
    repo: str
    canonical_url: HttpUrl

    total_installs: int | None = None
    weekly_installs: int | None = None
    weekly_installs_raw: str | None = None
    platform_installs: PlatformInstalls | None = None

    name: str
    description: str | None = None
    first_seen_date: date | None = None

    github_url: HttpUrl | None = None
    og_image_url: HttpUrl | None = None

    skill_md_content: str | None = None
    skill_md_frontmatter: dict[str, Any] | None = None
    install_command: str | None = None
    categories: list[str] = Field(default_factory=list)

    run_id: str
    fetched_at: datetime
    discovery_source: DiscoverySource
    source_endpoint: DiscoverySource
    discovery_pass: int = Field(default=1, ge=1)
    rank_at_fetch: int | None = Field(default=None, ge=1)
    http_status: int | None = None
    parser_version: str = "0.1.0"
    raw_html_hash: str | None = None


class SkillMetrics(BaseModel):
    """Daily metric snapshot."""

    id: str
    snapshot_date: date
    total_installs: int | None = None
    weekly_installs: int | None = None
    platform_installs: PlatformInstalls | None = None


class ConvergencePassSummary(BaseModel):
    """Metrics for one discovery pass."""

    pass_number: int
    ids_seen: int
    repos_seen: int
    new_ids: int
    new_repos: int
    new_ids_growth_pct: float


class ConvergenceReport(BaseModel):
    """Discovery convergence outcome."""

    run_id: str
    started_at: datetime
    finished_at: datetime
    passes_executed: int
    converged: bool
    converged_reason: str
    total_ids: int
    total_repos: int
    pass_summaries: list[ConvergencePassSummary]
    fallback_used: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)
