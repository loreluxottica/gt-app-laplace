"""Control-tower configuration, read from environment.

Runs locally against the Databricks SQL warehouse: the SDK authenticates via
the DEFAULT profile (~/.databrickscfg) or DATABRICKS_HOST + DATABRICKS_TOKEN.
"""
import getpass
import os


class Config:
    # ── Unity Catalog ──────────────────────────────────────────────────────
    CATALOG = os.environ.get("UC_CATALOG", "sbx-logistics")
    SCHEMA = os.environ.get("UC_SCHEMA", "multidocument-prod")

    # ── SQL Warehouse for table access (StatementExecution) ────────────────
    WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

    # ── Databricks Jobs (set after creating job_ingest / job_deliver) ──────
    JOB_INGEST_ID = os.environ.get("JOB_INGEST_ID", "")
    JOB_DELIVER_ID = os.environ.get("JOB_DELIVER_ID", "")

    # ── Actor recorded on every dashboard action in pipeline_events ────────
    ACTOR = os.environ.get("DASHBOARD_ACTOR", "") or f"{getpass.getuser()}@local"

    # ── Ground Truth app link (annotation gate "Annotate" button) ──────────
    GT_APP_URL = os.environ.get("GT_APP_URL", "http://localhost:8000")

    # ── Derived helpers ────────────────────────────────────────────────────
    @classmethod
    def fq(cls, name: str) -> str:
        """Backtick-quoted fully qualified table/view name."""
        return f"`{cls.CATALOG}`.`{cls.SCHEMA}`.`{name}`"

    @classmethod
    def volume_path(cls, volume: str, day_id: str | None = None) -> str:
        base = f"/Volumes/{cls.CATALOG}/{cls.SCHEMA}/{volume}"
        return f"{base}/{day_id}" if day_id else base


config = Config()
