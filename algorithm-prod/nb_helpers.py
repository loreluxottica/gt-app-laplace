# Databricks notebook source
# ══════════════════════════════════════════════════════════════════════
# nb_helpers — shared utilities for the multi-document splitting pipeline
# ══════════════════════════════════════════════════════════════════════
# Consumed by every task notebook via:  %run ./nb_helpers
# (place the %run cell AFTER any dbutils.library.restartPython() cell)
#
# Provides:
#   get_day_id() / get_run_id()      widget access, validated — no bare except
#   volume_paths(day_id)             all dated volume paths in one place
#   EventLogger                      buffered append-only writer to pipeline_events
#   merge_processing_log(...)        the ONLY status-write path (temp-view MERGE,
#                                    no user data ever interpolated into SQL text)
#   quarantine_file(...)             copy-verify-delete a poison PDF to quarantine/
# ══════════════════════════════════════════════════════════════════════

import re
import uuid
from datetime import datetime

CATALOG = "sbx-logistics"
SCHEMA = "multidocument-us"

TABLE_LOG = f"`{CATALOG}`.`{SCHEMA}`.`processing_log`"
TABLE_EVENTS = f"`{CATALOG}`.`{SCHEMA}`.`pipeline_events`"

# day_id / run_id are interpolated into SQL after validation against this.
_SAFE_ID = re.compile(r"^[A-Za-z0-9._-]+$")


def _require_safe(value: str, what: str) -> str:
    if not value or not _SAFE_ID.match(value):
        raise ValueError(f"{what} '{value}' is empty or contains unsafe characters")
    return value


def get_day_id() -> str:
    """Batch identity = inbox/{day_id}/ folder. REQUIRED — fail fast if missing."""
    dbutils.widgets.text("day_id", "")
    day_id = dbutils.widgets.get("day_id").strip()
    if not day_id:
        raise ValueError(
            "Widget 'day_id' is required: it selects the inbox/{day_id}/ batch to process. "
            "Set it as a job parameter or fill the widget for a manual run."
        )
    return _require_safe(day_id, "day_id")


def get_run_id() -> str:
    """Execution identity, shared by all tasks via job parameter run_id={{job.run_id}}.
    Manual runs without the widget get a generated uuid (logged, never silent)."""
    dbutils.widgets.text("run_id", "")
    run_id = dbutils.widgets.get("run_id").strip()
    if not run_id:
        run_id = str(uuid.uuid4())
        print(f"⚠️  run_id widget empty — generated {run_id} (manual run)")
    return _require_safe(run_id, "run_id")


def volume_paths(day_id: str) -> dict:
    """Every dated volume path. No notebook builds paths by hand."""
    base = f"/Volumes/{CATALOG}/{SCHEMA}"
    return {
        "inbox": f"{base}/inbox/{day_id}",
        "manual": f"{base}/manual/{day_id}",
        "quarantine": f"{base}/quarantine/{day_id}",
        "check": f"{base}/check/{day_id}",
        "ground_truth": f"{base}/ground_truth/{day_id}",
        "archive": f"{base}/archive/{day_id}",
        "output": f"{base}/output/{day_id}",
    }

# COMMAND ----------

# DBTITLE 1,EventLogger — buffered writer to pipeline_events
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("run_id", StringType(), True),
    StructField("day_id", StringType(), True),
    StructField("stage", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("filename", StringType(), True),
    StructField("folder_id", StringType(), True),
    StructField("old_status", StringType(), True),
    StructField("new_status", StringType(), True),
    StructField("detail", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("actor", StringType(), False),
    StructField("event_ts", TimestampType(), False),
])


class EventLogger:
    """Buffers event rows and appends them in ONE DataFrame write per flush().
    Flush at the end of each cell and inside every except block, so a crash
    still leaves the trail written so far. Never writes row-at-a-time."""

    def __init__(self, run_id: str, day_id: str, stage: str, actor: str = "pipeline"):
        self.run_id = run_id
        self.day_id = day_id
        self.stage = stage
        self.actor = actor
        self._buffer = []

    def log(self, event_type, filename=None, folder_id=None, old_status=None,
            new_status=None, detail=None, error_message=None):
        self._buffer.append((
            str(uuid.uuid4()), self.run_id, self.day_id, self.stage, event_type,
            filename, folder_id, old_status, new_status,
            detail, (error_message or None) and str(error_message)[:2000],
            self.actor, datetime.now(),
        ))

    def flush(self):
        if not self._buffer:
            return
        rows, self._buffer = self._buffer, []
        try:
            spark.createDataFrame(rows, _EVENT_SCHEMA) \
                .write.format("delta").mode("append").saveAsTable(TABLE_EVENTS)
        except Exception as e:  # events must never take the pipeline down
            print(f"⚠️  EventLogger flush failed ({len(rows)} events lost): {e}")

# COMMAND ----------

# DBTITLE 1,merge_processing_log — the only status-write path
from pyspark.sql.types import IntegerType

# Whitelisted processing_log columns a MERGE may set, with their Spark types.
_LOG_COL_TYPES = {
    "status": StringType(),
    "error_message": StringType(),
    "error_stage": StringType(),
    "model_used": StringType(),
    "n_pages": IntegerType(),
    "n_documents_found": IntegerType(),
    "retry_count": IntegerType(),
    "started_at": TimestampType(),
    "completed_at": TimestampType(),
    "archived_path": StringType(),
    "sftp_delivered_at": TimestampType(),
    "sftp_target_folder": StringType(),
    "sftp_delivery_status": StringType(),
    "sftp_delivery_error": StringType(),
}


def merge_processing_log(day_id: str, run_id: str, rows: list, set_cols: list,
                         match_status: list | None = None):
    """Batched UPDATE of processing_log keyed on (day_id, filename).

    rows       : list of dicts, each {'filename': ..., <col>: value for col in set_cols}
    set_cols   : whitelisted column names to SET (values come from the temp view,
                 so filenames/errors never enter the SQL text)
    match_status: optional guard — only update rows currently in one of these statuses
    """
    if not rows:
        return
    bad = [c for c in set_cols if c not in _LOG_COL_TYPES]
    if bad:
        raise ValueError(f"merge_processing_log: columns not whitelisted: {bad}")
    _require_safe(day_id, "day_id")
    _require_safe(run_id, "run_id")

    schema = StructType(
        [StructField("filename", StringType(), False)]
        + [StructField(c, _LOG_COL_TYPES[c], True) for c in set_cols]
    )
    data = [tuple([r["filename"]] + [r.get(c) for c in set_cols]) for r in rows]
    view = f"_upd_log_{uuid.uuid4().hex[:8]}"
    spark.createDataFrame(data, schema).createOrReplaceTempView(view)

    set_clause = ", ".join(
        [f"log.{c} = u.{c}" for c in set_cols] + [f"log.run_id = '{run_id}'"]
    )
    # status guard values are code constants, never user input
    guard = ""
    if match_status:
        statuses = ", ".join(f"'{s}'" for s in match_status)
        guard = f"AND log.status IN ({statuses})"

    spark.sql(f"""
        MERGE INTO {TABLE_LOG} AS log
        USING {view} AS u
        ON log.day_id = '{day_id}' AND log.filename = u.filename
        WHEN MATCHED {guard} THEN UPDATE SET {set_clause}
    """)
    spark.catalog.dropTempView(view)

# COMMAND ----------

# DBTITLE 1,quarantine_file — copy, verify, then remove from inbox
def quarantine_file(day_id: str, filename: str, reason: str, events: "EventLogger") -> bool:
    """Move a poison PDF out of inbox/{day_id}/ so it stops failing future runs.
    Order is crash-safe: copy → verify the copy exists → delete the original."""
    paths = volume_paths(day_id)
    src = f"{paths['inbox']}/{filename}.pdf"
    dst = f"{paths['quarantine']}/{filename}.pdf"
    try:
        dbutils.fs.mkdirs(paths["quarantine"])
        dbutils.fs.cp(src, dst)
        dbutils.fs.ls(dst)  # raises if the copy is missing
        dbutils.fs.rm(src)
        events.log("quarantined", filename=filename, detail=dst, error_message=reason)
        return True
    except Exception as e:
        events.log("error", filename=filename,
                   error_message=f"quarantine failed: {e}", detail=reason)
        return False


print("✓ nb_helpers loaded (get_day_id, get_run_id, volume_paths, EventLogger, "
      "merge_processing_log, quarantine_file)")
