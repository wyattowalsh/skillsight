"""Checkpoint models with JSON-safe field types."""

from __future__ import annotations

from datetime import datetime  # noqa: TC003

from pydantic import BaseModel, ConfigDict, Field


class FailureRecord(BaseModel):
    """Single extraction failure state."""

    error: str
    attempts: int = Field(default=1, ge=1)
    last_attempt: datetime
    http_status: int | None = None


class DiscoveryCheckpoint(BaseModel):
    """Discovery progress stored as JSON-serializable fields."""

    model_config = ConfigDict(extra="ignore")

    schema_version: str = "1"
    run_id: str
    search_queries_completed: set[str] = Field(default_factory=set)
    repos_crawled: set[str] = Field(default_factory=set)
    discovered_skill_ids: set[str] = Field(default_factory=set)
    pass_summaries: list[dict[str, float | int]] = Field(default_factory=list)
    started_at: datetime
    last_updated: datetime


class ExtractionCheckpoint(BaseModel):
    """Extraction progress stored as JSON-serializable fields."""

    model_config = ConfigDict(extra="ignore")

    schema_version: str = "1"
    run_id: str
    completed: set[str] = Field(default_factory=set)
    failed: dict[str, FailureRecord] = Field(default_factory=dict)
    total: int = Field(default=0, ge=0)
    started_at: datetime
    last_updated: datetime
