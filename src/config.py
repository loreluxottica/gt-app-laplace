"""Central configuration, read from environment (set via app.yaml in Databricks Apps)."""
import os


class Config:
    # ── Unity Catalog ──────────────────────────────────────────────────────
    CATALOG = os.environ.get("UC_CATALOG", "sbx-logistics")
    SCHEMA = os.environ.get("UC_SCHEMA", "multidocument-us")

    # ── SQL Warehouse for table access (StatementExecution) ────────────────
    WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

    # ── Volumes ────────────────────────────────────────────────────────────
    CHECK_VOLUME = os.environ.get("CHECK_VOLUME", "check")
    GROUND_TRUTH_VOLUME = os.environ.get("GROUND_TRUTH_VOLUME", "ground_truth")

    # ── Annotator identity ─────────────────────────────────────────────────
    # In Databricks Apps the calling user's email arrives in this header.
    USER_HEADER = "X-Forwarded-Email"
    DEFAULT_ANNOTATOR = os.environ.get("DEFAULT_ANNOTATOR", "unknown")

    # ── Derived helpers ────────────────────────────────────────────────────
    @classmethod
    def fq_table(cls, name: str) -> str:
        """Backtick-quoted fully qualified table name."""
        return f"`{cls.CATALOG}`.`{cls.SCHEMA}`.`{name}`"

    @classmethod
    def volume_path(cls, volume: str) -> str:
        """/Volumes path for a volume under the configured catalog/schema."""
        return f"/Volumes/{cls.CATALOG}/{cls.SCHEMA}/{volume}"

    @classmethod
    def check_path(cls) -> str:
        return cls.volume_path(cls.CHECK_VOLUME)

    @classmethod
    def ground_truth_path(cls) -> str:
        return cls.volume_path(cls.GROUND_TRUTH_VOLUME)

    # Table names
    TABLE_SPLIT_RESULTS = "split_results"
    TABLE_EVALUATION = "evaluation_results"
    TABLE_PROCESSING_LOG = "processing_log"


config = Config()
