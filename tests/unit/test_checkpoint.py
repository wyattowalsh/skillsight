from datetime import UTC, datetime

from skillsight.models.checkpoint import DiscoveryCheckpoint, ExtractionCheckpoint, FailureRecord
from skillsight.storage.checkpoint import load_checkpoint, save_checkpoint


def test_checkpoint_roundtrip(tmp_path) -> None:
    path = tmp_path / "discovery_state.json"
    checkpoint = DiscoveryCheckpoint(
        run_id="run-1",
        search_queries_completed=set(),
        repos_crawled={"vercel-labs/skills"},
        discovered_skill_ids={"vercel-labs/skills/find-skills"},
        pass_summaries=[
            {"pass_number": 1, "ids_seen": 1, "repos_seen": 1, "new_ids": 1, "new_repos": 1, "new_ids_growth_pct": 0.0}
        ],
        started_at=datetime.now(UTC),
        last_updated=datetime.now(UTC),
    )

    save_checkpoint(path, checkpoint)
    loaded = load_checkpoint(path, DiscoveryCheckpoint)

    assert loaded is not None
    assert loaded.discovered_skill_ids == checkpoint.discovered_skill_ids
    assert loaded.repos_crawled == {"vercel-labs/skills"}


def test_checkpoint_atomic_write(tmp_path) -> None:
    """Verify atomic write produces a valid file."""
    path = tmp_path / "test_state.json"
    checkpoint = DiscoveryCheckpoint(
        run_id="run-2",
        started_at=datetime.now(UTC),
        last_updated=datetime.now(UTC),
    )

    save_checkpoint(path, checkpoint)
    assert path.exists()
    loaded = load_checkpoint(path, DiscoveryCheckpoint)
    assert loaded is not None
    assert loaded.run_id == "run-2"


def test_checkpoint_load_nonexistent(tmp_path) -> None:
    loaded = load_checkpoint(tmp_path / "nonexistent.json", DiscoveryCheckpoint)
    assert loaded is None


def test_checkpoint_load_corrupt_file(tmp_path) -> None:
    path = tmp_path / "corrupt.json"
    path.write_text("not valid json {{{")
    loaded = load_checkpoint(path, DiscoveryCheckpoint)
    assert loaded is None


def test_checkpoint_load_corrupt_with_bak_fallback(tmp_path) -> None:
    path = tmp_path / "state.json"
    bak = tmp_path / "state.json.bak"
    path.write_text("corrupt!!!")

    checkpoint = DiscoveryCheckpoint(
        run_id="bak-run",
        started_at=datetime.now(UTC),
        last_updated=datetime.now(UTC),
    )
    bak.write_text(checkpoint.model_dump_json())

    loaded = load_checkpoint(path, DiscoveryCheckpoint)
    assert loaded is not None
    assert loaded.run_id == "bak-run"


def test_checkpoint_load_corrupt_bak_also_corrupt(tmp_path) -> None:
    """Both main and .bak are corrupt."""
    path = tmp_path / "state.json"
    bak = tmp_path / "state.json.bak"
    path.write_text("corrupt main")
    bak.write_text("corrupt bak too")
    loaded = load_checkpoint(path, DiscoveryCheckpoint)
    assert loaded is None


def test_checkpoint_creates_bak_on_overwrite(tmp_path) -> None:
    """Verify .bak is created when overwriting an existing checkpoint."""
    path = tmp_path / "state.json"
    first = DiscoveryCheckpoint(
        run_id="run-first",
        started_at=datetime.now(UTC),
        last_updated=datetime.now(UTC),
    )
    save_checkpoint(path, first)
    assert path.exists()

    second = DiscoveryCheckpoint(
        run_id="run-second",
        started_at=datetime.now(UTC),
        last_updated=datetime.now(UTC),
    )
    save_checkpoint(path, second)

    bak = path.with_suffix(path.suffix + ".bak")
    assert bak.exists()

    loaded_bak = load_checkpoint(bak, DiscoveryCheckpoint)
    assert loaded_bak is not None
    assert loaded_bak.run_id == "run-first"

    loaded_main = load_checkpoint(path, DiscoveryCheckpoint)
    assert loaded_main is not None
    assert loaded_main.run_id == "run-second"


def test_checkpoint_load_missing_primary_with_bak_fallback(tmp_path) -> None:
    """Simulate crash between .bak creation and new file replace."""
    path = tmp_path / "state.json"
    bak = tmp_path / "state.json.bak"
    # Primary doesn't exist, but .bak does (crash scenario)
    checkpoint = DiscoveryCheckpoint(
        run_id="recovered",
        started_at=datetime.now(UTC),
        last_updated=datetime.now(UTC),
    )
    bak.write_text(checkpoint.model_dump_json())
    loaded = load_checkpoint(path, DiscoveryCheckpoint)
    assert loaded is not None
    assert loaded.run_id == "recovered"


def test_checkpoint_load_missing_primary_with_corrupt_bak(tmp_path) -> None:
    """Primary missing, .bak exists but is corrupt."""
    path = tmp_path / "state.json"
    bak = tmp_path / "state.json.bak"
    bak.write_text("not valid json {{{")
    loaded = load_checkpoint(path, DiscoveryCheckpoint)
    assert loaded is None


def test_extraction_checkpoint_roundtrip(tmp_path) -> None:
    path = tmp_path / "extraction_state.json"
    checkpoint = ExtractionCheckpoint(
        run_id="ext-1",
        completed={"skill-a", "skill-b"},
        failed={"skill-c": FailureRecord(error="timeout", attempts=2, last_attempt=datetime.now(UTC), http_status=408)},
        total=3,
        started_at=datetime.now(UTC),
        last_updated=datetime.now(UTC),
    )

    save_checkpoint(path, checkpoint)
    loaded = load_checkpoint(path, ExtractionCheckpoint)

    assert loaded is not None
    assert loaded.completed == {"skill-a", "skill-b"}
    assert "skill-c" in loaded.failed
    assert loaded.failed["skill-c"].error == "timeout"
    assert loaded.failed["skill-c"].attempts == 2
