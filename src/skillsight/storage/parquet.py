"""Parquet writers with explicit schemas."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from skillsight.models.skill import SkillMetrics, SkillRecord

SKILLS_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("skill_id", pa.string(), nullable=False),
        pa.field("owner", pa.string(), nullable=False),
        pa.field("repo", pa.string(), nullable=False),
        pa.field("canonical_url", pa.string(), nullable=False),
        pa.field("total_installs", pa.int64()),
        pa.field("weekly_installs", pa.int64()),
        pa.field("weekly_installs_raw", pa.string()),
        pa.field("platform_installs", pa.string()),
        pa.field("name", pa.string(), nullable=False),
        pa.field("description", pa.string()),
        pa.field("first_seen_date", pa.date32()),
        pa.field("github_url", pa.string()),
        pa.field("og_image_url", pa.string()),
        pa.field("skill_md_content", pa.string()),
        pa.field("skill_md_frontmatter", pa.string()),
        pa.field("install_command", pa.string()),
        pa.field("categories", pa.string()),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("discovery_source", pa.string(), nullable=False),
        pa.field("source_endpoint", pa.string(), nullable=False),
        pa.field("discovery_pass", pa.int64(), nullable=False),
        pa.field("rank_at_fetch", pa.int64()),
        pa.field("http_status", pa.int64()),
        pa.field("parser_version", pa.string(), nullable=False),
        pa.field("raw_html_hash", pa.string()),
    ]
)

METRICS_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("snapshot_date", pa.date32(), nullable=False),
        pa.field("total_installs", pa.int64()),
        pa.field("weekly_installs", pa.int64()),
        pa.field("platform_installs", pa.string()),
    ]
)


def write_skills_parquet(path: Path, records: list[SkillRecord]) -> None:
    """Write full skill records to parquet with explicit schema, atomically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [record.model_dump(mode="json") for record in records]
    # Serialize complex fields to JSON strings for flat parquet storage
    for row in rows:
        for key in ("platform_installs", "skill_md_frontmatter", "categories"):
            if row.get(key) is not None:
                row[key] = json.dumps(row[key], default=str)
        # Convert date/datetime strings to native types for proper Parquet typing
        if row.get("first_seen_date") is not None:
            v = row["first_seen_date"]
            if isinstance(v, str):
                row["first_seen_date"] = date.fromisoformat(v)
        if row.get("fetched_at") is not None:
            v = row["fetched_at"]
            if isinstance(v, str):
                row["fetched_at"] = datetime.fromisoformat(v)
    table = pa.Table.from_pylist(rows, schema=SKILLS_PARQUET_SCHEMA)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    os.close(fd)
    try:
        pq.write_table(table, tmp_path, compression="zstd")
        Path(tmp_path).replace(path)
    except BaseException:  # pragma: no cover
        Path(tmp_path).unlink(missing_ok=True)
        raise


def write_metrics_parquet(path: Path, records: list[SkillMetrics]) -> None:
    """Write metrics records to parquet with explicit schema, atomically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [record.model_dump(mode="json") for record in records]
    for row in rows:
        if row.get("platform_installs") is not None:
            row["platform_installs"] = json.dumps(row["platform_installs"], default=str)
        # Convert date strings to native date for proper Parquet typing
        if row.get("snapshot_date") is not None:
            v = row["snapshot_date"]
            if isinstance(v, str):
                row["snapshot_date"] = date.fromisoformat(v)
    table = pa.Table.from_pylist(rows, schema=METRICS_PARQUET_SCHEMA)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    os.close(fd)
    try:
        pq.write_table(table, tmp_path, compression="zstd")
        Path(tmp_path).replace(path)
    except BaseException:  # pragma: no cover
        Path(tmp_path).unlink(missing_ok=True)
        raise
