"""SQLModel-based SQLite output."""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlmodel import Field, Session, SQLModel, create_engine, delete, select

if TYPE_CHECKING:
    from pathlib import Path

    from skillsight.models.skill import SkillRecord


class SkillDB(SQLModel, table=True):
    """SQLite table for skill records."""

    __tablename__ = "skills"

    id: str = Field(primary_key=True)
    skill_id: str = Field(index=True)
    owner: str = Field(index=True)
    repo: str = Field(index=True)
    canonical_url: str
    total_installs: int | None = None
    weekly_installs: int | None = None
    weekly_installs_raw: str | None = None
    name: str
    description: str | None = None
    first_seen_date: date | None = None
    github_url: str | None = None
    og_image_url: str | None = None
    skill_md_content: str | None = None
    install_command: str | None = None
    run_id: str
    fetched_at: datetime
    discovery_source: str
    source_endpoint: str
    discovery_pass: int = 1
    rank_at_fetch: int | None = None
    http_status: int | None = None
    parser_version: str = "0.1.0"
    raw_html_hash: str | None = None


class SkillMetricsDB(SQLModel, table=True):
    """SQLite table for daily metrics."""

    __tablename__ = "skill_metrics"

    id: int | None = Field(default=None, primary_key=True)
    skill_id: str = Field(index=True)
    snapshot_date: date = Field(index=True)
    total_installs: int | None = None
    weekly_installs: int | None = None


class PlatformInstallDB(SQLModel, table=True):
    """SQLite table for platform install breakdown."""

    __tablename__ = "platform_installs"

    id: int | None = Field(default=None, primary_key=True)
    skill_id: str = Field(index=True)
    snapshot_date: date = Field(index=True)
    platform: str
    installs: int


def _create_engine(path: Path):  # noqa: ANN202
    """Create SQLite engine."""
    path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{path}", echo=False)


_BATCH_SIZE = 500


def write_skills_sqlite(path: Path, records: list[SkillRecord]) -> None:
    """Write skill records to SQLite database."""
    engine = _create_engine(path)
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        # Delete existing platform installs for these skills to prevent duplicates
        skill_ids = [r.id for r in records]
        if skill_ids:
            session.exec(delete(PlatformInstallDB).where(PlatformInstallDB.skill_id.in_(skill_ids)))  # type: ignore[arg-type]

        platform_batch: list[PlatformInstallDB] = []

        for i, record in enumerate(records):
            data = record.model_dump()
            db_record = SkillDB(
                id=data["id"],
                skill_id=data["skill_id"],
                owner=data["owner"],
                repo=data["repo"],
                canonical_url=str(data["canonical_url"]),
                total_installs=data.get("total_installs"),
                weekly_installs=data.get("weekly_installs"),
                weekly_installs_raw=data.get("weekly_installs_raw"),
                name=data["name"],
                description=data.get("description"),
                first_seen_date=data.get("first_seen_date"),
                github_url=str(data["github_url"]) if data.get("github_url") else None,
                og_image_url=str(data["og_image_url"]) if data.get("og_image_url") else None,
                skill_md_content=data.get("skill_md_content"),
                install_command=data.get("install_command"),
                run_id=data["run_id"],
                fetched_at=data["fetched_at"],
                discovery_source=data["discovery_source"],
                source_endpoint=data["source_endpoint"],
                discovery_pass=data.get("discovery_pass", 1),
                rank_at_fetch=data.get("rank_at_fetch"),
                http_status=data.get("http_status"),
                parser_version=data.get("parser_version", "0.1.0"),
                raw_html_hash=data.get("raw_html_hash"),
            )
            session.merge(db_record)

            # Collect platform installs for batch insert
            platform_data = record.platform_installs
            if platform_data:
                snapshot_date = record.fetched_at.date() if hasattr(record.fetched_at, "date") else date.today()
                for platform, installs in platform_data.model_dump(exclude_none=True).items():
                    if isinstance(installs, int):
                        platform_batch.append(
                            PlatformInstallDB(
                                skill_id=record.id,
                                snapshot_date=snapshot_date,
                                platform=platform,
                                installs=installs,
                            )
                        )

            # Flush in batches to reduce commit overhead
            if (i + 1) % _BATCH_SIZE == 0:
                session.add_all(platform_batch)
                session.flush()
                platform_batch = []

        # Flush remaining platform installs and commit
        if platform_batch:
            session.add_all(platform_batch)
        session.commit()


def read_skills_sqlite(path: Path) -> list[dict]:
    """Read skill records from SQLite database."""
    engine = _create_engine(path)
    with Session(engine) as session:
        results = session.exec(select(SkillDB)).all()
        return [{col: getattr(row, col) for col in SkillDB.model_fields} for row in results]
