"""UC Volume access via Databricks SDK Files API.

Used to: list PDFs in check/, download PDF bytes for the viewer, and read/write
ground-truth JSON labels in ground_truth/.
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
    def list_pdfs(self, volume_path: str) -> list[str]:
        """Return filenames (without .pdf extension) directly under volume_path."""
        names = []
        try:
            for entry in self._w.files.list_directory_contents(volume_path):
                if entry.is_directory:
                    continue
                base = posixpath.basename(entry.path)
                if base.lower().endswith(".pdf"):
                    names.append(base[:-4])
        except NotFound:
            return []
        return sorted(names)

    def list_json_stems(self, volume_path: str) -> set[str]:
        """Return set of filenames (without .json) present under volume_path."""
        stems: set[str] = set()
        try:
            for entry in self._w.files.list_directory_contents(volume_path):
                if entry.is_directory:
                    continue
                base = posixpath.basename(entry.path)
                if base.lower().endswith(".json"):
                    stems.add(base[:-5])
        except NotFound:
            return set()
        return stems

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
