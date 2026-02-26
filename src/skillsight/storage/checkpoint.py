"""Checkpoint load/save helpers with atomic writes."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from pathlib import Path

from loguru import logger
from pydantic import BaseModel


def save_checkpoint(path: Path, model: BaseModel) -> None:
    """Persist checkpoint model as JSON using atomic write (tempfile + os.replace)."""

    path.parent.mkdir(parents=True, exist_ok=True)
    data = model.model_dump_json(indent=2)

    # Write to a temp file in the same directory, then atomically replace
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(data)
        bak = path.with_suffix(path.suffix + ".bak")
        with contextlib.suppress(FileNotFoundError):
            path.replace(bak)
        Path(tmp_path).replace(path)
    except BaseException:
        Path(tmp_path).unlink(missing_ok=True)
        raise


def _try_load_bak[ModelT: BaseModel](
    path: Path,
    model_type: type[ModelT],
    *,
    reason: str,
) -> ModelT | None:
    """Attempt to load checkpoint from .bak file."""
    bak = path.with_suffix(path.suffix + ".bak")
    if not bak.exists():
        return None
    logger.warning("Checkpoint {} at {}, trying .bak fallback", reason, path)
    try:
        return model_type.model_validate_json(bak.read_text())
    except (json.JSONDecodeError, ValueError):
        logger.error("Backup checkpoint also corrupt at {}", bak)
        return None


def load_checkpoint[ModelT: BaseModel](path: Path, model_type: type[ModelT]) -> ModelT | None:
    """Load checkpoint model from JSON if present, with .bak fallback on corruption."""
    if not path.exists():
        return _try_load_bak(path, model_type, reason="missing")

    try:
        return model_type.model_validate_json(path.read_text())
    except (json.JSONDecodeError, ValueError):
        result = _try_load_bak(path, model_type, reason="corrupt")
        if result is not None:
            return result
        if not path.with_suffix(path.suffix + ".bak").exists():
            logger.error("Checkpoint corrupt at {} with no backup", path)
        return None
