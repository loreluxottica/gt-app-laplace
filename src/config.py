"""Central configuration, read from environment (set via app.yaml in Databricks Apps)."""
import os


class Config:
    # ── Unity Catalog ──────────────────────────────────────────────────────
    CATALOG = os.environ.get("UC_CATALOG", "sbx-logistics")
    SCHEMA = os.environ.get("UC_SCHEMA", "multidocument-prod")

    # ── SQL Warehouse for table access (StatementExecution) ────────────────
    WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

    # ── Volumes ────────────────────────────────────────────────────────────
    VALIDATION_VOLUME = os.environ.get("VALIDATION_VOLUME", "validation")
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
    def validation_path(cls, day_id: str | None = None) -> str:
        """validation/ volume; dated subfolder validation/{day_id}/ when a batch is given."""
        base = cls.volume_path(cls.VALIDATION_VOLUME)
        return f"{base}/{day_id}" if day_id else base

    @classmethod
    def ground_truth_path(cls, day_id: str | None = None) -> str:
        """ground_truth/ volume; dated subfolder ground_truth/{day_id}/ when given."""
        base = cls.volume_path(cls.GROUND_TRUTH_VOLUME)
        return f"{base}/{day_id}" if day_id else base

    # Table names
    TABLE_SPLIT_RESULTS = "split_results"
    TABLE_EVALUATION = "evaluation_results"
    TABLE_PROCESSING_LOG = "processing_log"


config = Config()
