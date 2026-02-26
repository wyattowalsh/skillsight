"""JSONL persistence."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write dictionaries to JSONL file atomically via tempfile + os.replace."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            for row in rows:
                f.write(json.dumps(row, default=str))
                f.write("\n")
        Path(tmp_path).replace(path)
    except BaseException:  # pragma: no cover
        Path(tmp_path).unlink(missing_ok=True)
        raise


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read JSONL file into dictionary rows."""

    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                # Skip corrupt/truncated trailing lines (e.g. from a mid-write crash)
                continue
            if isinstance(parsed, dict):
                rows.append(parsed)
    return rows


def count_jsonl_rows_with_errors(path: Path) -> tuple[int, int]:
    """Count dictionary rows and parse errors in a JSONL file."""

    if not path.exists():
        return 0, 0
    count = 0
    parse_errors = 0
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                # Skip corrupt/truncated trailing lines (e.g. from a mid-write crash)
                parse_errors += 1
                continue
            if isinstance(parsed, dict):
                count += 1
    return count, parse_errors


def count_jsonl_rows(path: Path) -> int:
    """Count dictionary rows in a JSONL file without loading all rows into memory."""

    count, _ = count_jsonl_rows_with_errors(path)
    return count
