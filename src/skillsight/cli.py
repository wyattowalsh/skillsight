"""Skillsight CLI."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, date, datetime
from pathlib import Path

import typer
from aiolimiter import AsyncLimiter
from rich.console import Console
from rich.table import Table

from skillsight.clients.browser import BrowserClient
from skillsight.clients.http import AdaptiveBlockMonitor, RequestContext, create_http_client
from skillsight.contracts import load_openapi
from skillsight.models.skill import DiscoveredSkill, SkillRecord
from skillsight.pipeline.discovery_flow import discovery_flow
from skillsight.pipeline.export_flow import export_flow
from skillsight.pipeline.extraction_flow import extraction_flow
from skillsight.pipeline.orchestrator import skillsight_pipeline
from skillsight.pipeline.publish_datasets import publish_datasets
from skillsight.pipeline.web_static_pack import web_static_pack_flow
from skillsight.pipeline.validation_flow import validation_flow, verify_completeness
from skillsight.settings import Settings
from skillsight.storage import persist_discovery_results
from skillsight.storage.jsonl import read_jsonl, write_jsonl
from skillsight.storage.quality import build_quality_report

app = typer.Typer(help="Skillsight extraction and analytics pipeline")
console = Console()


def _settings_from_args(
    output_dir: Path | None = None,
    passes_max: int | None = None,
    converge_repos: int | None = None,
    converge_growth: float | None = None,
    structured_only: bool | None = None,
) -> Settings:
    settings = Settings()
    if output_dir is not None:
        settings.output_dir = output_dir
    if passes_max is not None:
        settings.passes_max = passes_max
    if converge_repos is not None:
        settings.converge_repos = converge_repos
    if converge_growth is not None:
        settings.converge_growth = converge_growth
    if structured_only is not None:
        settings.structured_only = structured_only
    return settings


def _request_context(settings: Settings) -> RequestContext:
    limiter = AsyncLimiter(settings.rate_limit_per_second, 1)
    monitor = AdaptiveBlockMonitor(settings.browser_block_window, settings.browser_block_threshold_percent)
    return RequestContext(limiter=limiter, monitor=monitor)


def _load_discovered(path: Path) -> dict[str, DiscoveredSkill]:
    records = read_jsonl(path)
    result: dict[str, DiscoveredSkill] = {}
    for row in records:
        skill = DiscoveredSkill.model_validate(row)
        result[skill.id] = skill
    return result


def _load_skill_records(path: Path) -> list[SkillRecord]:
    rows = read_jsonl(path)
    return [SkillRecord.model_validate(row) for row in rows]


@app.command("discover")
def discover(
    output_dir: Path = typer.Option(Path("./data"), "--output-dir"),
    sample: int = typer.Option(0, "--sample", help="Limit search API to N queries (0=all 1296)"),
    strategy: str = typer.Option(
        "three-phase", "--strategy", help="Discovery strategy: 'three-phase' (default) or 'convergence'"
    ),
) -> None:
    """Run skill discovery using the selected strategy."""
    settings = _settings_from_args(output_dir=output_dir)

    if strategy == "convergence":
        from skillsight.discovery.all_time import run_convergence_discovery

        async def _run_convergence() -> tuple[int, int]:
            run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            ctx = _request_context(settings)
            async with await create_http_client(settings) as client:
                discovered, repos, _report = await run_convergence_discovery(client, ctx, settings, run_id)

            persist_discovery_results(
                settings.output_dir,
                discovered,
                sorted(repos),
                run_id,
            )

            return len(discovered), len(repos)

        discovered_count, repo_count = asyncio.run(_run_convergence())
    else:

        async def _run() -> tuple[int, int]:
            run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            ctx = _request_context(settings)
            async with await create_http_client(settings) as client:
                # discovery_flow is called directly (not .fn()) because it uses
                # Prefect .submit() internally, which requires a FlowRunContext.
                discovered, summary = await discovery_flow(settings, run_id, client, ctx, sample=sample or None)
            return len(discovered), summary.get("total_repos", 0)

        discovered_count, repo_count = asyncio.run(_run())

    console.print(f"discovery complete: skills={discovered_count} repos={repo_count}")


@app.command("extract")
def extract(
    output_dir: Path = typer.Option(Path("./data"), "--output-dir"),
    structured_only: bool = typer.Option(True, "--structured-only"),
    resume: bool = typer.Option(True, "--resume"),
) -> None:
    """Run structured extraction over discovered skills."""

    settings = _settings_from_args(output_dir=output_dir, structured_only=structured_only)
    settings.resume = resume

    discovered_path = settings.output_dir / "discovery" / "discovered_skills.jsonl"
    discovered = _load_discovered(discovered_path)
    if not discovered:
        raise typer.BadParameter("No discovered skills found. Run `skillsight discover` first.")

    async def _run() -> tuple[int, int]:
        run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        ctx = _request_context(settings)
        async with await create_http_client(settings) as client:
            records, _, failures = await extraction_flow.fn(settings, run_id, client, ctx, discovered)
        quality = build_quality_report(records, failures)
        quality_path = settings.output_dir / "reports" / "quality_report.json"
        quality_path.parent.mkdir(parents=True, exist_ok=True)
        write_jsonl(quality_path.with_suffix(".jsonl"), [quality])
        quality_path.write_text(json.dumps(quality, indent=2, default=str))
        return len(records), len(failures)

    record_count, failure_count = asyncio.run(_run())
    console.print(f"extraction complete: records={record_count} failures={failure_count}")


@app.command("run")
def run(
    output_dir: Path = typer.Option(Path("./data"), "--output-dir"),
    structured_only: bool = typer.Option(True, "--structured-only"),
    upload_r2: bool = typer.Option(False, "--upload-r2"),
) -> None:
    """Run full discovery + extraction + validation pipeline."""

    settings = _settings_from_args(output_dir=output_dir, structured_only=structured_only)
    quality = asyncio.run(skillsight_pipeline.fn(settings))

    if upload_r2:
        upload_result = export_flow.fn(settings, upload_r2=True)
        console.print(upload_result)

    console.print(quality)


@app.command("validate")
def validate(
    output_dir: Path = typer.Option(Path("./data"), "--output-dir"),
) -> None:
    """Validate latest snapshot and print quality summary."""

    settings = _settings_from_args(output_dir=output_dir)
    snapshot_dir = settings.output_dir / "snapshots" / date.today().isoformat()
    records_path = snapshot_dir / "skills_full.jsonl"

    if not records_path.exists():
        raise typer.BadParameter("No snapshot found for today. Run `skillsight run` or `skillsight extract` first.")

    records = _load_skill_records(records_path)
    quality = validation_flow(
        records,
        failures={},
        discovery_summary={
            "total_skills": len(records),
            "total_repos": len({f"{r.owner}/{r.repo}" for r in records}),
            "by_source": {},
        },
    )
    table = Table(title="Quality")
    table.add_column("Metric")
    table.add_column("Value")
    for key, value in quality.items():
        table.add_row(str(key), str(value))
    console.print(table)


@app.command("export")
def export(
    output_dir: Path = typer.Option(Path("./data"), "--output-dir"),
    upload_r2: bool = typer.Option(False, "--upload-r2"),
) -> None:
    """Output artifact paths and optionally upload to R2."""

    settings = _settings_from_args(output_dir=output_dir)
    result = export_flow.fn(settings, upload_r2=upload_r2)
    console.print(result)


@app.command("export-web")
def export_web(
    output_dir: Path = typer.Option(Path("./data"), "--output-dir"),
    snapshot_date: str = typer.Option("", "--date", help="Snapshot date (YYYY-MM-DD), defaults to today"),
    page_size: int = typer.Option(12, "--page-size", min=1, max=200),
    upload_r2: bool = typer.Option(False, "--upload-r2"),
    publish_latest: bool = typer.Option(
        False,
        "--publish-latest",
        help="Promote latest pointers during upload (required for backfill promotion).",
    ),
) -> None:
    """Generate static web data pack and optionally upload artifacts to R2."""

    settings = _settings_from_args(output_dir=output_dir)
    today = date.today()
    target_date = date.fromisoformat(snapshot_date) if snapshot_date else today
    result = web_static_pack_flow.fn(
        settings,
        snapshot_date=target_date,
        page_size=page_size,
        export_prefix=settings.web_export_prefix,
    )
    console.print(result)
    if upload_r2:
        if target_date != today and not publish_latest:
            console.print(
                f"Backfill upload detected ({target_date.isoformat()}); "
                "skipping latest-pointer promotion. Use --publish-latest to promote this snapshot."
            )
        export_kwargs: dict[str, object] = {"upload_r2": True, "snapshot_date": target_date}
        if publish_latest:
            export_kwargs["publish_latest"] = True
        uploads = export_flow.fn(settings, **export_kwargs)
        console.print(uploads)


@app.command("publish-datasets")
def publish_datasets_cmd(
    output_dir: Path = typer.Option(Path("./data"), "--output-dir"),
    snapshot_date: str = typer.Option("", "--date", help="Snapshot date (YYYY-MM-DD), defaults to today"),
) -> None:
    """Publish dataset mirrors (GitHub Releases / Kaggle) when enabled."""

    settings = _settings_from_args(output_dir=output_dir)
    target_date = date.fromisoformat(snapshot_date) if snapshot_date else date.today()
    result = publish_datasets(settings, snapshot_date=target_date)
    console.print(result)


@app.command("verify-completeness")
def verify_completeness_cmd(
    baseline_total: int = typer.Option(..., "--baseline-total"),
    output_dir: Path = typer.Option(Path("./data"), "--output-dir"),
) -> None:
    """Compare current discovered total with baseline."""

    settings = _settings_from_args(output_dir=output_dir)
    discovered_path = settings.output_dir / "discovery" / "discovered_skills.jsonl"
    current_total = len(read_jsonl(discovered_path))
    result = verify_completeness(current_total=current_total, baseline_total=baseline_total)
    console.print(result)
    if result["status"] != "ok":
        raise typer.Exit(code=1)


@app.command("probe")
def probe(
    url: str = typer.Option("https://skills.sh/", "--url"),
) -> None:
    """Run browser probe (Camoufox optional extra)."""

    async def _run() -> list[str]:
        client = BrowserClient(headless=True)
        result = await client.probe(url=url)
        return result.urls

    try:
        urls = asyncio.run(_run())
    except RuntimeError as exc:
        raise typer.BadParameter(str(exc)) from exc

    for endpoint in urls:
        console.print(endpoint)


@app.command("stats")
def stats(
    output_dir: Path = typer.Option(Path("./data"), "--output-dir"),
    snapshot_date: str = typer.Option("", "--date", help="Snapshot date (YYYY-MM-DD), defaults to today"),
) -> None:
    """Show dataset summary statistics using DuckDB."""

    from skillsight.storage.duckdb_query import run_dataset_stats

    settings = _settings_from_args(output_dir=output_dir)
    target_date = snapshot_date or date.today().isoformat()
    skills_path = settings.output_dir / "snapshots" / target_date / "skills_full.parquet"

    if not skills_path.exists():
        raise typer.BadParameter(f"No snapshot found for {target_date}")

    result = run_dataset_stats(skills_path)
    table = Table(title=f"Dataset Stats ({target_date})")
    table.add_column("Metric")
    table.add_column("Value")
    for key, value in result.items():
        table.add_row(str(key), str(value))
    console.print(table)


@app.command("diff")
def diff(
    date_a: str = typer.Argument(..., help="Earlier snapshot date (YYYY-MM-DD)"),
    date_b: str = typer.Argument(..., help="Later snapshot date (YYYY-MM-DD)"),
    output_dir: Path = typer.Option(Path("./data"), "--output-dir"),
) -> None:
    """Compare two snapshot dates."""

    from skillsight.storage.duckdb_query import run_diff_query

    settings = _settings_from_args(output_dir=output_dir)
    path_a = settings.output_dir / "snapshots" / date_a / "skills_full.parquet"
    path_b = settings.output_dir / "snapshots" / date_b / "skills_full.parquet"

    if not path_a.exists():
        raise typer.BadParameter(f"No snapshot found for {date_a}")
    if not path_b.exists():
        raise typer.BadParameter(f"No snapshot found for {date_b}")

    result = run_diff_query(path_a, path_b)
    table = Table(title=f"Diff: {date_a} vs {date_b}")
    table.add_column("Metric")
    table.add_column("Value")
    for key, value in result.items():
        table.add_row(str(key), str(value))
    console.print(table)


@app.command("contract")
def contract(
    surface: str = typer.Option(
        "search",
        "--surface",
        help="Contract surface to inspect: search, legacy, or all",
    ),
) -> None:
    """Print summary of Worker API contracts."""

    selected = surface.strip().lower()
    if selected not in {"search", "legacy", "all"}:
        raise typer.BadParameter("surface must be one of: search, legacy, all")

    def _print_contract(label: str, spec_surface: str) -> None:
        openapi = load_openapi(spec_surface)  # type: ignore[arg-type]
        paths = openapi.get("paths", {})
        if not isinstance(paths, dict):
            raise typer.BadParameter("OpenAPI contract has invalid paths")
        console.print(f"{label} worker contract version={openapi.get('info', {}).get('version', 'unknown')}")
        for path in sorted(paths):
            console.print(path)

    if selected == "all":
        _print_contract("search", "search")
        _print_contract("legacy", "legacy")
        return

    _print_contract(selected, selected)


if __name__ == "__main__":
    app()
