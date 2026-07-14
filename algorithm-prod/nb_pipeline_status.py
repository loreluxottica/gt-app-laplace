# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Config
# ══════════════════════════════════════════════════════════════════════
# nb_pipeline_status — Pipeline monitor (read-only)
# ══════════════════════════════════════════════════════════════════════
# Thin wrapper over the shared SQL views (sql/views.sql) — the same
# definitions the control-tower app reads. Optional day_id widget
# focuses on one batch; empty = all batches.
# The control tower (pipeline-dashboard/) supersedes this notebook for
# day-to-day operations; this stays as an in-workspace quick check.
# ══════════════════════════════════════════════════════════════════════

CATALOG = "sbx-logistics"
SCHEMA = "multidocument-us"

dbutils.widgets.text("day_id", "")
DAY_ID = dbutils.widgets.get("day_id").strip()
DAY_FILTER = f"WHERE day_id = '{DAY_ID}'" if DAY_ID else ""

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

print(f"Batch filter: {DAY_ID or '(all batches)'}")

# COMMAND ----------

# DBTITLE 1,Batch overview (v_batch_status)
df_batches = spark.sql(f"""
    SELECT day_id, lifecycle, has_errors,
           n_files, n_pending, n_parsing, n_parsed, n_predicted,
           n_sftp_pending, n_delivered, n_sftp_failed, n_deferred,
           n_error, n_skipped, n_manual, n_needs_review,
           last_event_ts
    FROM v_batch_status
    {DAY_FILTER}
    ORDER BY day_id DESC
""")
display(df_batches)

# COMMAND ----------

# DBTITLE 1,ASCII progress per batch
rows = df_batches.collect()

for b in rows:
    total = b["n_files"] or 0
    done = (b["n_delivered"] or 0)
    predicted = ((b["n_predicted"] or 0) + (b["n_sftp_pending"] or 0) + done
                 + (b["n_sftp_failed"] or 0) + (b["n_deferred"] or 0))
    bar_len = 40

    def bar(n):
        filled = int(bar_len * n / total) if total else 0
        return "█" * filled + "░" * (bar_len - filled)

    print(f"\n{'═' * 60}")
    print(f"  BATCH {b['day_id']}   [{b['lifecycle'].upper()}]"
          + ("   ⚠️ HAS ERRORS" if b["has_errors"] else ""))
    print(f"{'═' * 60}")
    print(f"  Files:      {total}")
    print(f"  Predicted   {bar(predicted)} {predicted}/{total}")
    print(f"  Delivered   {bar(done)} {done}/{total}")
    print(f"  Errors: {b['n_error']}  SFTP failed: {b['n_sftp_failed']}  "
          f"Deferred: {b['n_deferred']}  Needs review: {b['n_needs_review']}  "
          f"Skipped: {b['n_skipped']}  Manual: {b['n_manual']}")

# COMMAND ----------

# DBTITLE 1,Files needing attention (v_stuck_files)
display(spark.sql(f"""
    SELECT day_id, filename, folder_id, status, sftp_delivery_status,
           stuck_reason, error_stage, retry_count, completed_at
    FROM v_stuck_files
    {DAY_FILTER}
    ORDER BY day_id DESC, stuck_reason, filename
"""))

# COMMAND ----------

# DBTITLE 1,SFTP delivery board (v_sftp_board)
display(spark.sql(f"""
    SELECT * FROM v_sftp_board
    {DAY_FILTER}
    ORDER BY day_id DESC, folder_id
"""))

# COMMAND ----------

# DBTITLE 1,Needs-review queue (v_needs_review)
display(spark.sql(f"""
    SELECT * FROM v_needs_review
    {DAY_FILTER}
    ORDER BY day_id DESC, filename
"""))

# COMMAND ----------

# DBTITLE 1,Recent runs (v_run_summary)
display(spark.sql(f"""
    SELECT * FROM v_run_summary
    {DAY_FILTER}
    ORDER BY started_at DESC
    LIMIT 30
"""))

# COMMAND ----------

# DBTITLE 1,Recent events (v_events_recent)
display(spark.sql(f"""
    SELECT * FROM v_events_recent
    {DAY_FILTER}
    LIMIT 100
"""))

# COMMAND ----------

# DBTITLE 1,Volume counts (physical files per batch)
import os

BASE = f"/Volumes/{CATALOG}/{SCHEMA}"

def count_pdfs(path):
    try:
        return sum(1 for f in os.listdir(path) if f.lower().endswith(".pdf"))
    except FileNotFoundError:
        return 0

def day_dirs(volume):
    try:
        return sorted(
            (d for d in os.listdir(f"{BASE}/{volume}")
             if os.path.isdir(f"{BASE}/{volume}/{d}")), reverse=True)
    except FileNotFoundError:
        return []

days = [DAY_ID] if DAY_ID else day_dirs("inbox")
print(f"{'day_id':<12} {'inbox':>7} {'check':>7} {'gt':>5} {'archive':>8} {'manual':>7} {'quarant.':>9} {'output':>7}")
for d in days:
    gt = 0
    try:
        gt = sum(1 for f in os.listdir(f"{BASE}/ground_truth/{d}") if f.endswith(".json"))
    except FileNotFoundError:
        pass
    out = 0
    try:
        for sub in os.listdir(f"{BASE}/output/{d}"):
            out += count_pdfs(f"{BASE}/output/{d}/{sub}")
    except FileNotFoundError:
        pass
    print(f"{d:<12} {count_pdfs(f'{BASE}/inbox/{d}'):>7} {count_pdfs(f'{BASE}/check/{d}'):>7} "
          f"{gt:>5} {count_pdfs(f'{BASE}/archive/{d}'):>8} {count_pdfs(f'{BASE}/manual/{d}'):>7} "
          f"{count_pdfs(f'{BASE}/quarantine/{d}'):>9} {out:>7}")
