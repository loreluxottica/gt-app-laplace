"""Central configuration for the merged app, read from environment.

Set via app.yaml in Databricks Apps. Locally the Databricks SDK authenticates
from a profile (~/.databrickscfg) or DATABRICKS_HOST + DATABRICKS_TOKEN.

Class attributes are read at import time — env changes need a restart.
"""
import getpass
import os


def _csv(name: str) -> list[str]:
    """'a@x.com, b@x.com' -> ['a@x.com', 'b@x.com'] (lowercased, blanks dropped)."""
    return [s.strip().lower() for s in os.environ.get(name, "").split(",") if s.strip()]


class Config:
    # ── Unity Catalog ──────────────────────────────────────────────────────
    CATALOG = os.environ.get("UC_CATALOG", "sbx-logistics")
    SCHEMA = os.environ.get("UC_SCHEMA", "multidocument-prod")

    # ── SQL Warehouse for table access (StatementExecution) ────────────────
    WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

    # ── Databricks Jobs (set after creating job_ingest / job_deliver) ──────
    JOB_INGEST_ID = os.environ.get("JOB_INGEST_ID", "")
    JOB_DELIVER_ID = os.environ.get("JOB_DELIVER_ID", "")

    # ── Identity ───────────────────────────────────────────────────────────
    # In Databricks Apps the calling user's email arrives in this header.
    USER_HEADER = "X-Forwarded-Email"
    # Fallback when the header is absent (local runs, health probes).
    ACTOR = os.environ.get("DASHBOARD_ACTOR", "") or f"{getpass.getuser()}@local"
    DEFAULT_ANNOTATOR = os.environ.get("DEFAULT_ANNOTATOR", "") or ACTOR
    # Empty = everyone may operate the pipeline. See core/auth.can_operate().
    OPERATOR_EMAILS = _csv("OPERATOR_EMAILS")

    # ── Volumes ────────────────────────────────────────────────────────────
    # Every volume name lives here and nowhere else. Hardcoding one of these
    # elsewhere lets a rename silently desync the writer from the reader —
    # e.g. the GT app writing to a renamed validation/ while the gate keeps
    # reading the old path, finds it empty, and opens.
    INBOX_VOLUME = os.environ.get("INBOX_VOLUME", "inbox")
    VALIDATION_VOLUME = os.environ.get("VALIDATION_VOLUME", "validation")
    GROUND_TRUTH_VOLUME = os.environ.get("GROUND_TRUTH_VOLUME", "ground_truth")
    ARCHIVE_VOLUME = os.environ.get("ARCHIVE_VOLUME", "archive")
    OVERSIZED_VOLUME = os.environ.get("OVERSIZED_VOLUME", "oversized")
    QUARANTINE_VOLUME = os.environ.get("QUARANTINE_VOLUME", "quarantine")

    # ── Table names ────────────────────────────────────────────────────────
    TABLE_SPLIT_RESULTS = "split_results"
    TABLE_EVALUATION = "evaluation_results"
    TABLE_PROCESSING_LOG = "processing_log"

    # ── Derived helpers ────────────────────────────────────────────────────
    @classmethod
    def fq(cls, name: str) -> str:
        """Backtick-quoted fully qualified table/view name (the schema has a hyphen)."""
        return f"`{cls.CATALOG}`.`{cls.SCHEMA}`.`{name}`"

    @classmethod
    def volume_path(cls, volume: str, day_id: str | None = None) -> str:
        """/Volumes path; dated subfolder {volume}/{day_id}/ when a batch is given."""
        base = f"/Volumes/{cls.CATALOG}/{cls.SCHEMA}/{volume}"
        return f"{base}/{day_id}" if day_id else base

    @classmethod
    def validation_path(cls, day_id: str | None = None) -> str:
        return cls.volume_path(cls.VALIDATION_VOLUME, day_id)

    @classmethod
    def ground_truth_path(cls, day_id: str | None = None) -> str:
        return cls.volume_path(cls.GROUND_TRUTH_VOLUME, day_id)


config = Config()
