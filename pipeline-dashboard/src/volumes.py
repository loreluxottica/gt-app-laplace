"""UC Volume listings via the SDK Files API (batch folders, gate counts)."""
from __future__ import annotations

import posixpath

from databricks.sdk.errors import NotFound

from .config import config
from .db import get_sql


def _files(volume_path: str, suffix: str) -> list[str]:
    """Stems of files with the given suffix directly under volume_path."""
    w = get_sql().workspace
    names = []
    try:
        for entry in w.files.list_directory_contents(volume_path):
            if entry.is_directory:
                continue
            base = posixpath.basename(entry.path)
            if base.lower().endswith(suffix):
                names.append(base[: -len(suffix)])
    except NotFound:
        return []
    return sorted(names)


def list_subdirs(volume: str) -> list[str]:
    """day_id folders directly under a volume, newest first."""
    w = get_sql().workspace
    dirs = []
    try:
        for entry in w.files.list_directory_contents(config.volume_path(volume)):
            if entry.is_directory:
                dirs.append(posixpath.basename(entry.path.rstrip("/")))
    except NotFound:
        return []
    return sorted(dirs, reverse=True)


def inbox_days() -> list[str]:
    return list_subdirs("inbox")


def inbox_count(day_id: str) -> int:
    return len(_files(config.volume_path("inbox", day_id), ".pdf"))


def check_pdfs(day_id: str) -> list[str]:
    return _files(config.volume_path("check", day_id), ".pdf")


def gt_jsons(day_id: str) -> set[str]:
    return set(_files(config.volume_path("ground_truth", day_id), ".json"))


def volume_counts(day_id: str) -> dict:
    return {
        "inbox": len(_files(config.volume_path("inbox", day_id), ".pdf")),
        "check": len(_files(config.volume_path("check", day_id), ".pdf")),
        "ground_truth": len(_files(config.volume_path("ground_truth", day_id), ".json")),
        "archive": len(_files(config.volume_path("archive", day_id), ".pdf")),
        "manual": len(_files(config.volume_path("manual", day_id), ".pdf")),
        "quarantine": len(_files(config.volume_path("quarantine", day_id), ".pdf")),
    }
