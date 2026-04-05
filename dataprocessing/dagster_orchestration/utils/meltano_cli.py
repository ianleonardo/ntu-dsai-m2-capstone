"""Resolve the meltano executable for subprocess calls (local dev + containers)."""

from __future__ import annotations

import shutil
from pathlib import Path


def resolve_meltano_executable(project_root: Path) -> str:
    """Prefer `meltano` on PATH (Docker / cloud), then project `.venv/bin/meltano` (local uv)."""
    which = shutil.which("meltano")
    if which:
        return which
    venv_bin = project_root / ".venv" / "bin" / "meltano"
    if venv_bin.is_file():
        return str(venv_bin)
    raise FileNotFoundError(
        "meltano CLI not found. Run `uv sync` from the repo root (meltano is a project dependency) "
        "or ensure meltano is on PATH."
    )
