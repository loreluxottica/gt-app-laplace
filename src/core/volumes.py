"""UC Volume access via the Databricks SDK Files API.

Two surfaces over one client:
  * VolumeClient — path-based: list/download/read/write, used by the annotator
    (PDF bytes for the viewer, ground-truth JSON).
  * module functions — volume-name-based listings for the pipeline (batch
    folders, gate counts). They take a volume NAME and resolve it via config.

Both go through get_volumes(). Nothing here needs a SQL warehouse — listing a
directory has nothing to do with one.
"""
from __future__ import annotations

import io
import json
import posixpath

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from .config import config


class VolumeClient:
    def __init__(self):
        self._w = WorkspaceClient()

    # ── listing ────────────────────────────────────────────────────────────
    def list_stems(self, volume_path: str, suffix: str) -> list[str]:
        """Filenames with `suffix` stripped, directly under volume_path.

        Stems (not full names) so PDF and JSON listings compare directly.
        """
        names = []
        try:
            for entry in self._w.files.list_directory_contents(volume_path):
                if entry.is_directory:
                    continue
                base = posixpath.basename(entry.path)
                if base.lower().endswith(suffix):
                    names.append(base[: -len(suffix)])
        except NotFound:
            # KNOWN: fail-open — a missing/unreadable directory looks empty.
            # For the gate that means an unlistable sample opens it. See CLAUDE.md.
            return []
        return sorted(names)

    def list_pdfs(self, volume_path: str) -> list[str]:
        return self.list_stems(volume_path, ".pdf")

    def list_json_stems(self, volume_path: str) -> set[str]:
        return set(self.list_stems(volume_path, ".json"))

    def list_subdirs(self, volume_path: str) -> list[str]:
        """Names of directories directly under volume_path (e.g. day_id batches)."""
        dirs = []
        try:
            for entry in self._w.files.list_directory_contents(volume_path):
                if entry.is_directory:
                    dirs.append(posixpath.basename(entry.path.rstrip("/")))
        except NotFound:
            return []
        return sorted(dirs, reverse=True)  # newest batch first

    # ── read ───────────────────────────────────────────────────────────────
    def download_bytes(self, file_path: str) -> bytes:
        resp = self._w.files.download(file_path)
        return resp.contents.read()

    def read_json(self, file_path: str) -> dict | None:
        try:
            return json.loads(self.download_bytes(file_path).decode("utf-8"))
        except NotFound:
            return None

    # ── write ──────────────────────────────────────────────────────────────
    def upload_json(self, file_path: str, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self._w.files.upload(file_path, io.BytesIO(data), overwrite=True)

    # ── path helpers ───────────────────────────────────────────────────────
    @staticmethod
    def pdf_path(volume_path: str, filename: str) -> str:
        return f"{volume_path}/{filename}.pdf"

    @staticmethod
    def json_path(volume_path: str, filename: str) -> str:
        return f"{volume_path}/{filename}.json"


_client: VolumeClient | None = None


def get_volumes() -> VolumeClient:
    global _client
    if _client is None:
        _client = VolumeClient()
    return _client


# ─────────────────────────────────────────────────────────────────────────────
#  Pipeline-facing listings — these take a volume NAME, not a path.
# ─────────────────────────────────────────────────────────────────────────────
def day_folders(volume: str) -> list[str]:
    """day_id folders directly under a volume, newest first.

    Named apart from VolumeClient.list_subdirs on purpose: that one takes a
    full path, this one a volume name.
    """
    return get_volumes().list_subdirs(config.volume_path(volume))


def inbox_days() -> list[str]:
    return day_folders(config.INBOX_VOLUME)


def inbox_count(day_id: str) -> int:
    return len(get_volumes().list_pdfs(config.volume_path(config.INBOX_VOLUME, day_id)))


def validation_pdfs(day_id: str) -> list[str]:
    return get_volumes().list_pdfs(config.validation_path(day_id))


def gt_jsons(day_id: str) -> set[str]:
    return get_volumes().list_json_stems(config.ground_truth_path(day_id))


def volume_counts(day_id: str) -> dict:
    v = get_volumes()
    pdfs = lambda vol: len(v.list_pdfs(config.volume_path(vol, day_id)))  # noqa: E731
    return {
        "inbox": pdfs(config.INBOX_VOLUME),
        "validation": pdfs(config.VALIDATION_VOLUME),
        "ground_truth": len(v.list_json_stems(config.ground_truth_path(day_id))),
        "archive": pdfs(config.ARCHIVE_VOLUME),
        "oversized": pdfs(config.OVERSIZED_VOLUME),
        "quarantine": pdfs(config.QUARANTINE_VOLUME),
    }
