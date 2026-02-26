"""Tests for CLI module."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from skillsight.cli import (
    _load_discovered,
    _load_skill_records,
    _request_context,
    _settings_from_args,
    app,
)
from skillsight.settings import Settings

runner = CliRunner()


def test_settings_from_args_defaults() -> None:
    s = _settings_from_args()
    assert s.output_dir == Path("./data")


def test_settings_from_args_overrides() -> None:
    s = _settings_from_args(
        output_dir=Path("/tmp/test"),
        passes_max=5,
        converge_repos=3,
        converge_growth=0.05,
        structured_only=False,
    )
    assert s.output_dir == Path("/tmp/test")
    assert s.passes_max == 5
    assert s.converge_repos == 3
    assert s.converge_growth == 0.05
    assert s.structured_only is False


def test_settings_from_args_partial() -> None:
    s = _settings_from_args(passes_max=7)
    assert s.passes_max == 7
    assert s.output_dir == Path("./data")


def test_request_context() -> None:
    settings = Settings()
    ctx = _request_context(settings)
    assert ctx.limiter is not None
    assert ctx.monitor is not None


def test_load_discovered_empty(tmp_path: Path) -> None:
    path = tmp_path / "discovered.jsonl"
    result = _load_discovered(path)
    assert result == {}


def test_load_discovered_with_data(tmp_path: Path) -> None:
    path = tmp_path / "discovered.jsonl"
    record = {
        "id": "o/r/s",
        "skill_id": "s",
        "owner": "o",
        "repo": "r",
        "name": "Test",
        "discovered_via": "search_api",
        "source_endpoint": "search_api",
        "discovered_at": datetime.now(UTC).isoformat(),
    }
    path.write_text(json.dumps(record) + "\n")
    result = _load_discovered(path)
    assert "o/r/s" in result
    assert result["o/r/s"].name == "Test"


def test_load_skill_records_empty(tmp_path: Path) -> None:
    path = tmp_path / "skills.jsonl"
    result = _load_skill_records(path)
    assert result == []


def test_load_skill_records_with_data(tmp_path: Path) -> None:
    path = tmp_path / "skills.jsonl"
    record = {
        "id": "o/r/s",
        "skill_id": "s",
        "owner": "o",
        "repo": "r",
        "canonical_url": "https://skills.sh/o/r/s",
        "name": "Test",
        "total_installs": 100,
        "description": "Test desc",
        "run_id": "run-1",
        "fetched_at": datetime.now(UTC).isoformat(),
        "discovery_source": "search_api",
        "source_endpoint": "search_api",
    }
    path.write_text(json.dumps(record) + "\n")
    result = _load_skill_records(path)
    assert len(result) == 1
    assert result[0].name == "Test"


def test_cli_contract() -> None:
    result = runner.invoke(app, ["contract"])
    assert result.exit_code == 0
    assert "search worker contract version=" in result.stdout
    assert "/v1/search" in result.stdout


def test_cli_contract_legacy_surface() -> None:
    result = runner.invoke(app, ["contract", "--surface", "legacy"])
    assert result.exit_code == 0
    assert "legacy worker contract version=" in result.stdout
    assert "/v1/skills" in result.stdout


def test_cli_contract_all_surfaces() -> None:
    result = runner.invoke(app, ["contract", "--surface", "all"])
    assert result.exit_code == 0
    assert "search worker contract version=" in result.stdout
    assert "legacy worker contract version=" in result.stdout


def test_cli_help() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Skillsight" in result.stdout


def test_cli_discover_help() -> None:
    result = runner.invoke(app, ["discover", "--help"])
    assert result.exit_code == 0
    assert "--sample" in result.stdout


def test_cli_extract_help() -> None:
    result = runner.invoke(app, ["extract", "--help"])
    assert result.exit_code == 0
    assert "--resume" in result.stdout


def test_cli_run_help() -> None:
    result = runner.invoke(app, ["run", "--help"])
    assert result.exit_code == 0
    assert "--upload-r2" in result.stdout


def test_cli_validate_no_snapshot(tmp_path: Path) -> None:
    result = runner.invoke(app, ["validate", "--output-dir", str(tmp_path)])
    assert result.exit_code != 0


def test_cli_export_no_snapshot(tmp_path: Path) -> None:
    result = runner.invoke(app, ["export", "--output-dir", str(tmp_path)])
    assert result.exit_code == 0


def test_cli_export_web_no_snapshot(tmp_path: Path) -> None:
    result = runner.invoke(app, ["export-web", "--output-dir", str(tmp_path)])
    assert result.exit_code != 0


def test_cli_verify_completeness_no_data(tmp_path: Path) -> None:
    result = runner.invoke(app, ["verify-completeness", "--baseline-total", "100", "--output-dir", str(tmp_path)])
    assert result.exit_code == 1


def test_cli_stats_no_snapshot(tmp_path: Path) -> None:
    result = runner.invoke(app, ["stats", "--output-dir", str(tmp_path), "--date", "2025-01-01"])
    assert result.exit_code != 0


def test_cli_diff_no_snapshots(tmp_path: Path) -> None:
    result = runner.invoke(app, ["diff", "2025-01-01", "2025-01-02", "--output-dir", str(tmp_path)])
    assert result.exit_code != 0


def test_cli_discover_mocked(tmp_path: Path) -> None:
    """Test discover command with mocked discovery_flow."""
    mock_discovered = {"o/r/s": "mock_skill"}
    mock_summary = {"total_repos": 1}

    mock_flow_fn = AsyncMock(return_value=(mock_discovered, mock_summary))
    mock_create_client = AsyncMock()
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_create_client.return_value = mock_client

    with (
        patch("skillsight.cli.discovery_flow", mock_flow_fn),
        patch("skillsight.cli.create_http_client", mock_create_client),
    ):
        result = runner.invoke(app, ["discover", "--output-dir", str(tmp_path), "--sample", "3"])

    assert result.exit_code == 0
    assert "discovery complete" in result.stdout


def test_cli_extract_no_discovered(tmp_path: Path) -> None:
    """Test extract command when no discovered skills exist."""
    result = runner.invoke(app, ["extract", "--output-dir", str(tmp_path)])
    assert result.exit_code != 0


def test_cli_extract_mocked(tmp_path: Path) -> None:
    """Test extract command with mocked extraction flow."""
    from skillsight.models.skill import SkillRecord
    from skillsight.storage.jsonl import write_jsonl

    # Write discovered skills
    disc_dir = tmp_path / "discovery"
    disc_dir.mkdir(parents=True)
    disc_record = {
        "id": "o/r/s",
        "skill_id": "s",
        "owner": "o",
        "repo": "r",
        "name": "Test",
        "discovered_via": "search_api",
        "source_endpoint": "search_api",
        "discovered_at": datetime.now(UTC).isoformat(),
    }
    write_jsonl(disc_dir / "discovered_skills.jsonl", [disc_record])

    mock_record = SkillRecord(
        id="o/r/s",
        skill_id="s",
        owner="o",
        repo="r",
        canonical_url="https://skills.sh/o/r/s",
        name="Test",
        total_installs=100,
        run_id="run-1",
        fetched_at=datetime.now(UTC),
        discovery_source="search_api",
        source_endpoint="search_api",
    )
    mock_flow_fn = AsyncMock(return_value=([mock_record], [], {}))
    mock_create_client = AsyncMock()
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_create_client.return_value = mock_client

    with (
        patch("skillsight.cli.extraction_flow.fn", mock_flow_fn),
        patch("skillsight.cli.create_http_client", mock_create_client),
    ):
        result = runner.invoke(app, ["extract", "--output-dir", str(tmp_path)])

    assert result.exit_code == 0
    assert "extraction complete" in result.stdout


def test_cli_run_mocked(tmp_path: Path) -> None:
    """Test run command with mocked pipeline."""
    mock_quality = {"total_records": 10, "failures": 0}
    mock_pipeline = AsyncMock(return_value=mock_quality)

    with patch("skillsight.cli.skillsight_pipeline.fn", mock_pipeline):
        result = runner.invoke(app, ["run", "--output-dir", str(tmp_path)])

    assert result.exit_code == 0


def test_cli_export_web_mocked(tmp_path: Path) -> None:
    mock_result = {"snapshot_date": "2025-01-15", "manifest_path": str(tmp_path / "web_data" / "data" / "v1" / "latest.json")}
    with patch("skillsight.cli.web_static_pack_flow.fn", return_value=mock_result):
        result = runner.invoke(app, ["export-web", "--output-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "manifest_path" in result.stdout


def test_cli_export_web_upload_passes_target_snapshot_date(tmp_path: Path) -> None:
    mock_result = {"snapshot_date": "2025-01-15", "manifest_path": str(tmp_path / "web_data" / "data" / "v1" / "latest.json")}
    with (
        patch("skillsight.cli.date") as mock_date,
        patch("skillsight.cli.web_static_pack_flow.fn", return_value=mock_result),
        patch("skillsight.cli.export_flow.fn", return_value={"ok": "1"}) as mock_export_flow,
    ):
        mock_date.today.return_value = date(2025, 1, 16)
        mock_date.fromisoformat.side_effect = date.fromisoformat
        result = runner.invoke(
            app,
            [
                "export-web",
                "--output-dir",
                str(tmp_path),
                "--date",
                "2025-01-15",
                "--upload-r2",
            ],
        )

    assert result.exit_code == 0
    mock_export_flow.assert_called_once()
    _, kwargs = mock_export_flow.call_args
    assert kwargs["upload_r2"] is True
    assert kwargs["snapshot_date"] == date(2025, 1, 15)
    assert "publish_latest" not in kwargs
    assert "Backfill upload detected (2025-01-15)" in result.stdout


def test_cli_export_web_upload_backfill_can_publish_latest(tmp_path: Path) -> None:
    mock_result = {"snapshot_date": "2025-01-14", "manifest_path": str(tmp_path / "web_data" / "data" / "v1" / "latest.json")}
    with (
        patch("skillsight.cli.date") as mock_date,
        patch("skillsight.cli.web_static_pack_flow.fn", return_value=mock_result),
        patch("skillsight.cli.export_flow.fn", return_value={"ok": "1"}) as mock_export_flow,
    ):
        mock_date.today.return_value = date(2025, 1, 15)
        mock_date.fromisoformat.side_effect = date.fromisoformat
        result = runner.invoke(
            app,
            [
                "export-web",
                "--output-dir",
                str(tmp_path),
                "--date",
                "2025-01-14",
                "--upload-r2",
                "--publish-latest",
            ],
        )

    assert result.exit_code == 0
    _, kwargs = mock_export_flow.call_args
    assert kwargs["upload_r2"] is True
    assert kwargs["snapshot_date"] == date(2025, 1, 14)
    assert kwargs["publish_latest"] is True
    assert "Backfill upload detected" not in result.stdout


def test_cli_validate_mocked(tmp_path: Path) -> None:
    """Test validate command with actual snapshot data."""
    snapshot_dir = tmp_path / "snapshots" / date.today().isoformat()
    snapshot_dir.mkdir(parents=True)
    skill_record = {
        "id": "o/r/s",
        "skill_id": "s",
        "owner": "o",
        "repo": "r",
        "canonical_url": "https://skills.sh/o/r/s",
        "name": "Test",
        "total_installs": 100,
        "run_id": "run-1",
        "fetched_at": datetime.now(UTC).isoformat(),
        "discovery_source": "search_api",
        "source_endpoint": "search_api",
    }
    jsonl_path = snapshot_dir / "skills_full.jsonl"
    jsonl_path.write_text(json.dumps(skill_record) + "\n")

    mock_quality = {"total_records": 1, "failures": 0, "coverage": {"name": 100.0}}
    with patch("skillsight.cli.validation_flow", return_value=mock_quality):
        result = runner.invoke(app, ["validate", "--output-dir", str(tmp_path)])

    assert result.exit_code == 0
    assert "Quality" in result.stdout


def test_cli_stats_mocked(tmp_path: Path) -> None:
    """Test stats command with actual parquet data."""
    from skillsight.models.skill import SkillRecord
    from skillsight.storage.parquet import write_skills_parquet

    snapshot_dir = tmp_path / "snapshots" / "2025-01-15"
    snapshot_dir.mkdir(parents=True)
    record = SkillRecord(
        id="o/r/s",
        skill_id="s",
        owner="o",
        repo="r",
        canonical_url="https://skills.sh/o/r/s",
        name="Test",
        total_installs=100,
        run_id="run-1",
        fetched_at=datetime.now(UTC),
        discovery_source="search_api",
        source_endpoint="search_api",
    )
    write_skills_parquet(snapshot_dir / "skills_full.parquet", [record])

    result = runner.invoke(app, ["stats", "--output-dir", str(tmp_path), "--date", "2025-01-15"])
    assert result.exit_code == 0
    assert "Dataset Stats" in result.stdout


def test_cli_diff_mocked(tmp_path: Path) -> None:
    """Test diff command with actual parquet data."""
    from skillsight.models.skill import SkillRecord
    from skillsight.storage.parquet import write_skills_parquet

    for d in ["2025-01-14", "2025-01-15"]:
        snapshot_dir = tmp_path / "snapshots" / d
        snapshot_dir.mkdir(parents=True)
        record = SkillRecord(
            id="o/r/s",
            skill_id="s",
            owner="o",
            repo="r",
            canonical_url="https://skills.sh/o/r/s",
            name="Test",
            total_installs=100,
            run_id="run-1",
            fetched_at=datetime.now(UTC),
            discovery_source="search_api",
            source_endpoint="search_api",
        )
        write_skills_parquet(snapshot_dir / "skills_full.parquet", [record])

    result = runner.invoke(app, ["diff", "2025-01-14", "2025-01-15", "--output-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "Diff" in result.stdout


def test_cli_probe_no_camoufox() -> None:
    """Test probe command when camoufox is not installed."""
    result = runner.invoke(app, ["probe"])
    # Camoufox likely not installed; expect error
    assert result.exit_code != 0 or "RuntimeError" in (result.stdout or "")


def test_cli_verify_completeness_ok(tmp_path: Path) -> None:
    """Test verify-completeness with data meeting baseline."""
    disc_dir = tmp_path / "discovery"
    disc_dir.mkdir(parents=True)
    records = [json.dumps({"id": f"o/r/s{i}"}) for i in range(5)]
    (disc_dir / "discovered_skills.jsonl").write_text("\n".join(records) + "\n")

    result = runner.invoke(app, ["verify-completeness", "--baseline-total", "3", "--output-dir", str(tmp_path)])
    assert result.exit_code == 0


def test_cli_discover_convergence_persists(tmp_path: Path) -> None:
    """Convergence strategy must write discovered_skills.jsonl and repos.json."""
    from skillsight.discovery.all_time import ConvergenceReport
    from skillsight.models import DiscoveredSkill

    mock_skill = DiscoveredSkill(
        id="o/r/s",
        skill_id="s",
        owner="o",
        repo="r",
        name="Test",
        discovered_via="all_time_api",
        source_endpoint="all_time_api",
        discovered_at=datetime.now(UTC),
    )
    mock_report = ConvergenceReport(
        run_id="r",
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
        passes_executed=1,
        converged=True,
        converged_reason="test",
        total_ids=1,
        total_repos=1,
        pass_summaries=[],
    )
    mock_convergence = AsyncMock(return_value=({"o/r/s": mock_skill}, {"o/r"}, mock_report))
    mock_create_client = AsyncMock()
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_create_client.return_value = mock_client

    with (
        patch("skillsight.cli.create_http_client", mock_create_client),
        # NOTE: Patches at definition site because cli.py uses a deferred import
        # (line 85: `from skillsight.discovery.all_time import ...`).
        # If the import is moved to module top-level, change target to
        # "skillsight.cli.run_convergence_discovery".
        patch("skillsight.discovery.all_time.run_convergence_discovery", mock_convergence),
    ):
        result = runner.invoke(app, ["discover", "--strategy", "convergence", "--output-dir", str(tmp_path)])

    assert result.exit_code == 0
    assert (tmp_path / "discovery" / "discovered_skills.jsonl").exists()
    assert (tmp_path / "discovery" / "repos.json").exists()
