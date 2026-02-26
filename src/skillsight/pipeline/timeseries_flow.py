"""Time-series analysis flow: daily diffs, anomaly detection, discovery log."""

from __future__ import annotations

import json
import os
import statistics
from datetime import date
from typing import TYPE_CHECKING, Any

from loguru import logger
from prefect import flow, task

from skillsight.storage.duckdb_query import run_timeseries_delta

if TYPE_CHECKING:
    from pathlib import Path


@task(name="compute-deltas")
def compute_deltas(metrics_prev: Path, metrics_curr: Path) -> list[dict[str, Any]]:
    """Compute per-skill install deltas between two snapshots."""
    return run_timeseries_delta(metrics_prev, metrics_curr)


@task(name="detect-anomalies")
def detect_anomalies(deltas: list[dict[str, Any]], *, std_threshold: float = 3.0) -> list[dict[str, Any]]:
    """Flag skills with install changes exceeding threshold standard deviations."""
    valid_deltas = [d["delta"] for d in deltas if d.get("delta") is not None and d["delta"] != 0]
    if len(valid_deltas) < 3:
        return []

    mean = statistics.mean(valid_deltas)
    stdev = statistics.stdev(valid_deltas)
    if stdev == 0:
        return []

    anomalies: list[dict[str, Any]] = []
    for d in deltas:
        delta = d.get("delta")
        if delta is None or delta == 0:
            continue
        z_score = (delta - mean) / stdev
        if abs(z_score) >= std_threshold:
            anomalies.append({**d, "z_score": round(z_score, 2)})

    logger.info("Detected {} anomalies (threshold={}sigma)", len(anomalies), std_threshold)
    return anomalies


@task(name="append-discovery-log")
def append_discovery_log(
    output_dir: Path,
    snapshot_date: date,
    deltas: list[dict[str, Any]],
) -> int:
    """Log newly discovered skills (those with no previous installs) to discovery log."""
    new_skills = [d for d in deltas if d.get("prev_installs") is None and d.get("curr_installs") is not None]

    if not new_skills:
        return 0

    log_path = output_dir / "timeseries" / "discovery_log.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Build all new lines in memory, then write in a single call + fsync
    # to minimise the corruption window on crash.
    buffer = ""
    for skill in new_skills:
        entry = {
            "id": skill["id"],
            "snapshot_date": snapshot_date.isoformat(),
            "initial_installs": skill["curr_installs"],
        }
        buffer += json.dumps(entry) + "\n"

    with log_path.open("a") as f:
        f.write(buffer)
        f.flush()
        os.fsync(f.fileno())

    logger.info("Logged {} newly discovered skills on {}", len(new_skills), snapshot_date)
    return len(new_skills)


@flow(name="skillsight-timeseries")
def timeseries_flow(
    output_dir: Path,
    current_date: date,
    previous_date: date | None = None,
) -> dict[str, Any]:
    """Run time-series analysis: deltas, anomalies, discovery log."""
    snapshots_dir = output_dir / "snapshots"
    curr_metrics = snapshots_dir / current_date.isoformat() / "metrics.parquet"

    if not curr_metrics.exists():
        logger.warning("No metrics parquet for {}", current_date)
        return {"status": "no_current_metrics"}

    # Find previous snapshot
    if previous_date is None:
        # Auto-detect most recent previous snapshot
        dates: list[date] = []
        if snapshots_dir.exists():
            for child in snapshots_dir.iterdir():
                if child.is_dir():
                    try:
                        d = date.fromisoformat(child.name)
                        if d < current_date:
                            dates.append(d)
                    except ValueError:
                        continue
        dates.sort()
        if not dates:
            logger.info("No previous snapshot found for timeseries analysis")
            return {"status": "no_previous_snapshot"}
        previous_date = dates[-1]

    prev_metrics = snapshots_dir / previous_date.isoformat() / "metrics.parquet"
    if not prev_metrics.exists():
        logger.warning("Previous metrics parquet missing for {}", previous_date)
        return {"status": "previous_metrics_missing"}

    deltas = compute_deltas(prev_metrics, curr_metrics)
    anomalies = detect_anomalies(deltas)
    new_count = append_discovery_log(output_dir, current_date, deltas)

    # Save daily summary
    summary = {
        "current_date": current_date.isoformat(),
        "previous_date": previous_date.isoformat(),
        "total_skills_compared": len(deltas),
        "anomalies_detected": len(anomalies),
        "newly_discovered": new_count,
    }

    summary_path = output_dir / "timeseries" / f"summary_{current_date.isoformat()}.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2))

    if anomalies:
        anomaly_path = output_dir / "timeseries" / f"anomalies_{current_date.isoformat()}.json"
        anomaly_path.write_text(json.dumps(anomalies, indent=2, default=str))

    return summary
