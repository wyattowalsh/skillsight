"""DuckDB helpers for snapshot queries."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import duckdb

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path


@contextmanager
def duckdb_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Context manager that yields a DuckDB in-memory connection and closes it on exit."""
    conn = duckdb.connect()
    try:
        yield conn
    finally:
        conn.close()


def run_stats_query(
    metrics_glob: Path,
    *,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> list[tuple[str, int]]:
    """Compute simple metric row counts by snapshot date."""

    sql = """
    SELECT snapshot_date::VARCHAR, COUNT(*)
    FROM read_parquet(? , hive_partitioning=false)
    GROUP BY snapshot_date
    ORDER BY snapshot_date DESC
    """
    if conn is not None:
        result = conn.execute(sql, [str(metrics_glob)]).fetchall()
    else:
        with duckdb_connection() as _conn:
            result = _conn.execute(sql, [str(metrics_glob)]).fetchall()
    return [(str(row[0]), int(row[1])) for row in result]


def run_dataset_stats(
    skills_path: Path,
    *,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, Any]:
    """Compute dataset summary stats from a skills parquet file."""

    def _execute(c: duckdb.DuckDBPyConnection) -> dict[str, Any]:
        total = c.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(skills_path)]).fetchone()
        total_count = total[0] if total else 0

        coverage = c.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(name) AS has_name,
                COUNT(description) AS has_description,
                COUNT(total_installs) AS has_installs,
                COUNT(github_url) AS has_github,
                COUNT(DISTINCT owner || '/' || repo) AS unique_repos,
                COUNT(DISTINCT owner) AS unique_owners
            FROM read_parquet(?)
            """,
            [str(skills_path)],
        ).fetchone()

        if not coverage:
            return {"total": total_count}

        return {
            "total": coverage[0],
            "has_name": coverage[1],
            "has_description": coverage[2],
            "has_installs": coverage[3],
            "has_github": coverage[4],
            "unique_repos": coverage[5],
            "unique_owners": coverage[6],
            "name_pct": round(coverage[1] * 100.0 / max(1, coverage[0]), 1),
            "description_pct": round(coverage[2] * 100.0 / max(1, coverage[0]), 1),
            "installs_pct": round(coverage[3] * 100.0 / max(1, coverage[0]), 1),
        }

    if conn is not None:
        return _execute(conn)
    with duckdb_connection() as _conn:
        return _execute(_conn)


def run_diff_query(
    skills_path_a: Path,
    skills_path_b: Path,
    *,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, Any]:
    """Compare two skill snapshot parquet files and return diff summary."""

    def _execute(c: duckdb.DuckDBPyConnection) -> dict[str, Any]:
        count_a = c.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(skills_path_a)]).fetchone()
        count_b = c.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(skills_path_b)]).fetchone()
        a_count = count_a[0] if count_a else 0
        b_count = count_b[0] if count_b else 0

        # Skills in B not in A (newly added)
        new_in_b = c.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT id FROM read_parquet(?)
                EXCEPT
                SELECT id FROM read_parquet(?)
            )
            """,
            [str(skills_path_b), str(skills_path_a)],
        ).fetchone()

        # Skills in A not in B (removed)
        removed = c.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT id FROM read_parquet(?)
                EXCEPT
                SELECT id FROM read_parquet(?)
            )
            """,
            [str(skills_path_a), str(skills_path_b)],
        ).fetchone()

        return {
            "count_a": a_count,
            "count_b": b_count,
            "delta": b_count - a_count,
            "new_in_b": new_in_b[0] if new_in_b else 0,
            "removed_from_a": removed[0] if removed else 0,
        }

    if conn is not None:
        return _execute(conn)
    with duckdb_connection() as _conn:
        return _execute(_conn)


def run_timeseries_delta(
    metrics_path_prev: Path,
    metrics_path_curr: Path,
    *,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> list[dict[str, Any]]:
    """Compute per-skill install deltas between two metrics snapshots."""

    def _execute(c: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
        results = c.execute(
            """
            SELECT
                COALESCE(c.id, p.id) AS id,
                p.total_installs AS prev_installs,
                c.total_installs AS curr_installs,
                c.total_installs - COALESCE(p.total_installs, 0) AS delta,
                p.weekly_installs AS prev_weekly,
                c.weekly_installs AS curr_weekly
            FROM read_parquet(?) c
            FULL OUTER JOIN read_parquet(?) p ON c.id = p.id
            WHERE c.total_installs IS NOT NULL OR p.total_installs IS NOT NULL
            ORDER BY delta DESC NULLS LAST
            """,
            [str(metrics_path_curr), str(metrics_path_prev)],
        ).fetchall()

        return [
            {
                "id": row[0],
                "prev_installs": row[1],
                "curr_installs": row[2],
                "delta": row[3],
                "prev_weekly": row[4],
                "curr_weekly": row[5],
            }
            for row in results
        ]

    if conn is not None:
        return _execute(conn)
    with duckdb_connection() as _conn:
        return _execute(_conn)
