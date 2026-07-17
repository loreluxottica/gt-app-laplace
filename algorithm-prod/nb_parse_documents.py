# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Load shared helpers
# MAGIC %run ./nb_helpers

# COMMAND ----------

# DBTITLE 1,Config
# ══════════════════════════════════════════════════════════════════════
# nb_parse_documents — Production PDF Parsing Pipeline
# ══════════════════════════════════════════════════════════════════════
# Task 1 of job_ingest (parse → split → check_export).
# Scans inbox/{day_id}/, parses PDFs with ai_parse_document,
# writes results to UC managed table, logs every transition to
# processing_log + pipeline_events. No file can end the run stuck
# in 'parsing': a final reconciliation pass forces leftovers to 'error'.
# ══════════════════════════════════════════════════════════════════════

from datetime import datetime

TABLE_PARSED = f"`{CATALOG}`.`{SCHEMA}`.`parsed_documents`"
TABLE_RUN_HISTORY = f"`{CATALOG}`.`{SCHEMA}`.`run_history`"

# ── Processing parameters ──
MAX_FILE_SIZE_MB = 100        # Files above this are skipped (manual processing)
LARGE_FILE_THRESHOLD_MB = 74  # Files above this are parsed sequentially
CHUNK_SIZE = 100              # Files per parallel chunk (single source of truth)
MAX_RETRIES = 2
RETRY_DELAY_SEC = 60

# ── Batch + run identity (job parameters) ──
DAY_ID = get_day_id()
RUN_ID = get_run_id()
PATHS = volume_paths(DAY_ID)
INBOX_PATH = PATHS["inbox"]
OVERSIZED_PATH = PATHS["oversized"]

RUN_START = datetime.now()
events = EventLogger(RUN_ID, DAY_ID, stage="parse")
events.log("run_started", detail=f"inbox={INBOX_PATH}")
events.flush()

print(f"{'═' * 60}")
print(f"  nb_parse_documents — Production Run")
print(f"{'═' * 60}")
print(f"  Run ID:    {RUN_ID}")
print(f"  Day ID:    {DAY_ID}")
print(f"  Started:   {RUN_START:%Y-%m-%d %H:%M:%S}")
print(f"  Inbox:     {INBOX_PATH}")
print(f"  Chunk:     {CHUNK_SIZE} files/batch")
print(f"  Max size:  {MAX_FILE_SIZE_MB} MB (skip above)")
print(f"{'═' * 60}")

# COMMAND ----------

# DBTITLE 1,Scan inbox and register files
# ── Scan inbox/{day_id}/ volume for PDFs ──
from pyspark.sql.functions import (
    col, regexp_extract, current_timestamp, lit, when, round as spark_round
)

df_inbox = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.pdf")
    .option("recursiveFileLookup", "true")
    .load(f"{INBOX_PATH}/")
    .select("path", "length")
    .withColumn("filename", regexp_extract(col("path"), r'([^/]+)\.pdf$', 1))
    .withColumn("folder_id", regexp_extract(col("filename"), r'^([A-Za-z0-9]+)_', 1))
    .withColumn("file_size_mb", spark_round(col("length") / (1024 * 1024), 2))
)

n_total = df_inbox.count()
if n_total == 0:
    events.log("run_completed", new_status="NO_FILES",
               detail=f"inbox/{DAY_ID} empty")
    events.flush()
    print(f"⚠️  inbox/{DAY_ID} empty — no PDFs to process. Early termination.")
    dbutils.notebook.exit("NO_FILES")

# Classify files by size
df_inbox = df_inbox.withColumn(
    "size_category",
    when(col("file_size_mb") > MAX_FILE_SIZE_MB, "oversized")
    .when(col("file_size_mb") > LARGE_FILE_THRESHOLD_MB, "large")
    .otherwise("standard")
)

n_standard = df_inbox.filter(col("size_category") == "standard").count()
n_large = df_inbox.filter(col("size_category") == "large").count()
n_oversized = df_inbox.filter(col("size_category") == "oversized").count()

print(f"PDF found in inbox/{DAY_ID}: {n_total}")
print(f"  Standard (≤{LARGE_FILE_THRESHOLD_MB}MB): {n_standard}")
print(f"  Large ({LARGE_FILE_THRESHOLD_MB}-{MAX_FILE_SIZE_MB}MB): {n_large}")
print(f"  Oversized (>{MAX_FILE_SIZE_MB}MB): {n_oversized} → volume oversized/{DAY_ID}/")

# COMMAND ----------

# DBTITLE 1,Filter already-parsed files
# ── Escludi file già in parsed_documents per QUESTO batch (idempotenza) ──
# Key = (day_id, filename): same filename in another day batch is another record.

df_already_parsed = spark.sql(
    f"SELECT filename FROM {TABLE_PARSED} WHERE day_id = '{DAY_ID}'"
)

df_to_process = df_inbox.join(df_already_parsed, on="filename", how="left_anti")

n_already_parsed = n_total - df_to_process.count()
n_to_process = df_to_process.filter(col("size_category") != "oversized").count()
n_oversized_new = df_to_process.filter(col("size_category") == "oversized").count()

if n_already_parsed > 0:
    print(f"ℹ️  {n_already_parsed} file already parsed (skip)")

if n_to_process == 0 and n_oversized_new == 0:
    events.log("run_completed", new_status="ALL_ALREADY_PARSED")
    events.flush()
    print("ℹ️  All files have already been processed. Early termination.")
    dbutils.notebook.exit("ALL_ALREADY_PARSED")

print(f"\nFile to process: {n_to_process}")
print(f"Oversized files to move to oversized/: {n_oversized_new}")

# COMMAND ----------

# DBTITLE 1,Register in processing_log and handle oversized
# ── Register new files (not yet in the log for this day_id) ──
df_in_log = spark.sql(
    f"SELECT filename FROM {TABLE_LOG} WHERE day_id = '{DAY_ID}'"
)
df_new_entries = df_to_process.join(df_in_log, on="filename", how="left_anti")

df_log_entries = (
    df_new_entries
    .select(
        col("filename"),
        col("folder_id"),
        col("file_size_mb"),
        when(col("size_category") == "oversized", lit("skipped"))
            .otherwise(lit("pending")).alias("status"),
        when(col("size_category") == "oversized", lit("File exceeds 100MB limit"))
            .otherwise(lit(None)).alias("error_message"),
        lit(None).cast("string").alias("error_stage"),
        lit(None).cast("string").alias("model_used"),
        lit(None).cast("int").alias("n_pages"),
        lit(None).cast("int").alias("n_documents_found"),
        lit(0).alias("retry_count"),
        current_timestamp().alias("created_at"),
        lit(None).cast("timestamp").alias("started_at"),
        lit(None).cast("timestamp").alias("completed_at"),
        lit(RUN_ID).alias("run_id"),
        lit(None).cast("string").alias("archived_path"),
        lit(None).cast("timestamp").alias("sftp_delivered_at"),
        lit(None).cast("string").alias("sftp_target_folder"),
        lit(None).cast("string").alias("sftp_delivery_status"),
        lit(None).cast("string").alias("sftp_delivery_error"),
        lit(DAY_ID).alias("day_id"),
    )
)

n_registered = df_new_entries.count()
if n_registered > 0:
    df_log_entries.write.format("delta").mode("append").saveAsTable(
        f"`{CATALOG}`.`{SCHEMA}`.`processing_log`"
    )
events.log("registered", detail=f"{n_registered} files registered")
print(f"✓ {n_registered} entries registered in processing_log")

# ── Move oversized files to oversized/{day_id}/ (copy → verify → delete) ──
oversized_files = (
    df_to_process
    .filter(col("size_category") == "oversized")
    .select("filename", "path")
    .collect()
)

for row in oversized_files:
    dst = f"{OVERSIZED_PATH}/{row['filename']}.pdf"
    try:
        dbutils.fs.mkdirs(OVERSIZED_PATH)
        dbutils.fs.cp(row["path"], dst)
        dbutils.fs.ls(dst)  # verify copy before removing the original
        dbutils.fs.rm(row["path"])
        events.log("status_change", filename=row["filename"],
                   new_status="skipped", detail=f"oversized → {dst}")
        print(f"  → {row['filename']}.pdf ({MAX_FILE_SIZE_MB}MB+) → oversized/{DAY_ID}/")
    except Exception as e:
        events.log("error", filename=row["filename"],
                   error_message=f"oversized move failed: {e}")
        print(f"  ✗ {row['filename']}.pdf move failed: {e}")

events.flush()
if oversized_files:
    print(f"✓ {len(oversized_files)} file moved to oversized/{DAY_ID}/")

# COMMAND ----------

# DBTITLE 1,Parse standard files (batch parallel, per-chunk fault isolation)
# ── Parsing batch parallelizzato per file standard (≤ 74MB) ──
# A chunk failure no longer kills the run: the chunk degrades to per-file
# parsing, and individual failures are marked 'error' + logged as events.
from pyspark.sql.functions import expr
import time

# n_pages: NULL (not 1) when the parsed document has no positioned elements —
# a NULL page count is a signal for review, never a silent single-page guess.
N_PAGES_EXPR = """
    CASE
        WHEN try_cast(parsed_content:document:elements AS ARRAY<VARIANT>) IS NULL
             OR size(try_cast(parsed_content:document:elements AS ARRAY<VARIANT>)) = 0
        THEN NULL
        ELSE aggregate(
            try_cast(parsed_content:document:elements AS ARRAY<VARIANT>),
            0,
            (acc, e) -> GREATEST(acc, COALESCE(CAST(e:bbox[0]:page_id AS INT), 0))
        ) + 1
    END
"""


def parse_paths_to_table(paths_batch):
    """Parse a list of PDF paths with ai_parse_document and append to parsed_documents."""
    df_chunk = (
        spark.read.format("binaryFile")
        .load(paths_batch)
        .withColumn("filename", regexp_extract(col("path"), r'([^/]+)\.pdf$', 1))
        .withColumn("folder_id", regexp_extract(col("filename"), r'^([A-Za-z0-9]+)_', 1))
        .withColumn("file_size_mb", col("length") / (1024 * 1024))
        .withColumn("parsed_content",
            expr("ai_parse_document(content, map('version', '2.0', 'descriptionElementTypes', ''))"))
        .withColumn("parsed_at", current_timestamp())
        .withColumn("run_id", lit(RUN_ID))
        .withColumn("day_id", lit(DAY_ID))
        .withColumn("source_path", col("path"))
        .withColumn("n_pages", expr(N_PAGES_EXPR))
    )
    df_result = df_chunk.select(
        "filename", "folder_id", "source_path", "file_size_mb", "n_pages",
        expr("CAST(parsed_content AS STRING)").alias("parsed_content"),
        "parsed_at", "run_id", "day_id",
    )
    df_result.write.format("delta").mode("append").saveAsTable(
        f"`{CATALOG}`.`{SCHEMA}`.`parsed_documents`"
    )


df_standard = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.pdf")
    .option("recursiveFileLookup", "true")
    .load(f"{INBOX_PATH}/")
    .filter(col("length") <= LARGE_FILE_THRESHOLD_MB * 1024 * 1024)
    .withColumn("filename", regexp_extract(col("path"), r'([^/]+)\.pdf$', 1))
    .join(df_already_parsed, on="filename", how="left_anti")
)

n_standard_to_parse = df_standard.count()
print(f"Parsing standard files: {n_standard_to_parse} (≤{LARGE_FILE_THRESHOLD_MB}MB)")

standard_rows = df_standard.select("path", "filename").collect()

# Mark this batch's pending files as 'parsing' (keyed day_id+filename, not run_id)
if standard_rows:
    now = datetime.now()
    merge_processing_log(
        DAY_ID, RUN_ID,
        rows=[{"filename": r["filename"], "status": "parsing", "started_at": now}
              for r in standard_rows],
        set_cols=["status", "started_at"],
        match_status=["pending", "error"],
    )

file_paths = [r["path"] for r in standard_rows]

# ⚠️ TEST LIMIT — set to an int for a small test run, None for production
TEST_LIMIT = None
if TEST_LIMIT:
    file_paths = file_paths[:TEST_LIMIT]
    print(f"⚠️  TEST MODE: limited to {TEST_LIMIT} files")
chunks = [file_paths[i:i + CHUNK_SIZE] for i in range(0, len(file_paths), CHUNK_SIZE)]

print(f"Chunks: {len(chunks)} of {CHUNK_SIZE} file each")

parsed_count = 0
error_count = 0
start_time = time.time()

for chunk_idx, chunk_paths in enumerate(chunks):
    chunk_start = time.time()
    try:
        parse_paths_to_table(chunk_paths)
        parsed_count += len(chunk_paths)
        chunk_elapsed = time.time() - chunk_start
        print(f"  ✓ Chunk {chunk_idx+1}/{len(chunks)}: {len(chunk_paths)} file in {chunk_elapsed:.0f}s "
              f"({chunk_elapsed/len(chunk_paths):.1f}s/file) | Totale: {parsed_count}")
    except Exception as chunk_err:
        # Chunk failed as a whole → degrade to per-file parsing so one poison
        # PDF cannot take down the other ~99 files of the chunk.
        print(f"  ⚠️  Chunk {chunk_idx+1} failed ({str(chunk_err)[:200]}) — retrying per-file")
        error_rows = []
        for p in chunk_paths:
            fname = p.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            try:
                parse_paths_to_table([p])
                parsed_count += 1
            except Exception as file_err:
                error_count += 1
                error_rows.append({
                    "filename": fname,
                    "status": "error",
                    "error_message": str(file_err)[:500],
                    "error_stage": "parsing",
                    "completed_at": datetime.now(),
                })
                events.log("error", filename=fname, new_status="error",
                           error_message=str(file_err)[:500])
                print(f"    ✗ {fname} FAILED")
        merge_processing_log(DAY_ID, RUN_ID, error_rows,
                             set_cols=["status", "error_message", "error_stage", "completed_at"])
        events.flush()

elapsed_standard = time.time() - start_time
events.flush()
print(f"\n✓ Standard file parsing complete: {parsed_count} file, "
      f"{error_count} errors in {elapsed_standard/60:.1f} min")

# COMMAND ----------

# DBTITLE 1,Parse large files (74-100MB, sequential with retry)
import time

df_large = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.pdf")
    .option("recursiveFileLookup", "true")
    .load(f"{INBOX_PATH}/")
    .filter(
        (col("length") > LARGE_FILE_THRESHOLD_MB * 1024 * 1024) &
        (col("length") <= MAX_FILE_SIZE_MB * 1024 * 1024)
    )
    .withColumn("filename", regexp_extract(col("path"), r'([^/]+)\.pdf$', 1))
    .join(df_already_parsed, on="filename", how="left_anti")
)
n_large = df_large.count()

if n_large == 0:
    print("Nessun file large (74-100MB) da processare.")
else:
    print(f"Parsing sequenziale: {n_large} file large ({LARGE_FILE_THRESHOLD_MB}-{MAX_FILE_SIZE_MB}MB)")
    large_files = df_large.select("path", "filename").collect()

    now = datetime.now()
    merge_processing_log(
        DAY_ID, RUN_ID,
        rows=[{"filename": r["filename"], "status": "parsing", "started_at": now}
              for r in large_files],
        set_cols=["status", "started_at"],
        match_status=["pending", "error"],
    )

    large_parsed = 0
    large_errors = 0

    for i, row in enumerate(large_files):
        file_start = time.time()
        retries = 0
        success = False

        while retries <= MAX_RETRIES and not success:
            try:
                parse_paths_to_table([row["path"]])
                success = True
                large_parsed += 1
                elapsed = time.time() - file_start
                print(f"  ✓ [{i+1}/{n_large}] {row['filename']}.pdf → {elapsed:.0f}s")
            except Exception as e:
                retries += 1
                if retries <= MAX_RETRIES:
                    print(f"  ⚠️  [{i+1}/{n_large}] {row['filename']} retry {retries}/{MAX_RETRIES}...")
                    time.sleep(RETRY_DELAY_SEC)
                else:
                    large_errors += 1
                    merge_processing_log(
                        DAY_ID, RUN_ID,
                        rows=[{
                            "filename": row["filename"],
                            "status": "error",
                            "error_message": str(e)[:500],
                            "error_stage": "parsing",
                            "retry_count": MAX_RETRIES,
                            "completed_at": datetime.now(),
                        }],
                        set_cols=["status", "error_message", "error_stage",
                                  "retry_count", "completed_at"],
                    )
                    events.log("error", filename=row["filename"], new_status="error",
                               error_message=str(e)[:500])
                    print(f"  ✗ [{i+1}/{n_large}] {row['filename']} FAILED after {MAX_RETRIES} retries")

    events.flush()
    print(f"\n✓ Large files: {large_parsed} parsed, {large_errors} errori")

# COMMAND ----------

# DBTITLE 1,Mark parsed + quarantine repeat offenders + reconcile stuck files
# ── Aggiorna processing_log con risultati parsing (key: day_id + filename) ──
df_just_parsed = spark.sql(f"""
    SELECT filename, n_pages
    FROM {TABLE_PARSED}
    WHERE day_id = '{DAY_ID}'
""")

n_parsed_total = df_just_parsed.count()

if n_parsed_total > 0:
    spark.sql(f"""
        MERGE INTO {TABLE_LOG} AS log
        USING (
            SELECT filename, n_pages
            FROM {TABLE_PARSED}
            WHERE day_id = '{DAY_ID}'
        ) AS parsed
        ON log.day_id = '{DAY_ID}' AND log.filename = parsed.filename
        WHEN MATCHED AND log.status IN ('pending', 'parsing') THEN UPDATE SET
            log.status = 'parsed',
            log.n_pages = parsed.n_pages,
            log.completed_at = current_timestamp(),
            log.run_id = '{RUN_ID}'
    """)
    print(f"✓ processing_log aggiornato: {n_parsed_total} file → status='parsed'")

# Files parsed but with NULL n_pages: parse produced no positioned elements —
# flag for review (they would mis-drive the splitter).
null_pages = spark.sql(f"""
    SELECT filename FROM {TABLE_PARSED}
    WHERE day_id = '{DAY_ID}' AND n_pages IS NULL
""").collect()
for r in null_pages:
    events.log("needs_review", filename=r["filename"],
               detail="n_pages NULL: ai_parse_document returned no positioned elements")
if null_pages:
    print(f"⚠️  {len(null_pages)} file with NULL n_pages flagged needs_review")

# Quarantine repeat offenders: files that hit MAX_RETRIES on a previous run
# and failed again on this one keep poisoning the inbox — move them out.
repeat_offenders = spark.sql(f"""
    SELECT filename FROM {TABLE_LOG}
    WHERE day_id = '{DAY_ID}' AND status = 'error'
      AND error_stage = 'parsing' AND retry_count >= {MAX_RETRIES}
""").collect()
quarantined = 0
for r in repeat_offenders:
    if quarantine_file(DAY_ID, r["filename"], f"parse failed after {MAX_RETRIES} retries", events):
        quarantined += 1
if quarantined:
    print(f"✓ {quarantined} poison file quarantined → quarantine/{DAY_ID}/")

# Reconciliation: NOTHING may stay 'parsing' at the end of the run.
stuck = spark.sql(f"""
    SELECT filename FROM {TABLE_LOG}
    WHERE day_id = '{DAY_ID}' AND status = 'parsing'
""").collect()
if stuck:
    merge_processing_log(
        DAY_ID, RUN_ID,
        rows=[{
            "filename": r["filename"],
            "status": "error",
            "error_message": "stranded in parsing at end of run — see pipeline_events",
            "error_stage": "parsing",
            "completed_at": datetime.now(),
        } for r in stuck],
        set_cols=["status", "error_message", "error_stage", "completed_at"],
        match_status=["parsing"],
    )
    for r in stuck:
        events.log("error", filename=r["filename"], old_status="parsing",
                   new_status="error", error_message="stranded in parsing at end of run")
    print(f"⚠️  {len(stuck)} stuck 'parsing' file reconciled to 'error'")

events.flush()

# COMMAND ----------

# DBTITLE 1,Run summary
from datetime import datetime

RUN_END = datetime.now()
run_duration_min = (RUN_END - RUN_START).total_seconds() / 60

stats = spark.sql(f"""
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN status = 'parsed' THEN 1 ELSE 0 END) AS parsed,
        SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skipped,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errored
    FROM {TABLE_LOG}
    WHERE day_id = '{DAY_ID}'
""").first()

if stats['errored'] > 0 and stats['parsed'] > 0:
    run_status = "partial_failure"
elif stats['errored'] > 0 and stats['parsed'] == 0:
    run_status = "failed"
else:
    run_status = "success"

try:
    JOB_RUN_ID = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
except Exception:
    JOB_RUN_ID = None

job_run_id_sql = f"'{JOB_RUN_ID}'" if JOB_RUN_ID else "NULL"

spark.sql(f"""
    INSERT INTO {TABLE_RUN_HISTORY} VALUES (
        '{RUN_ID}',
        {job_run_id_sql},
        TIMESTAMP '{RUN_START:%Y-%m-%d %H:%M:%S}',
        current_timestamp(),
        '{run_status}',
        {stats['total']},
        {stats['parsed']},
        {stats['skipped']},
        {stats['errored']},
        'ai_parse_document',
        NULL,
        {run_duration_min:.2f},
        'day_id={DAY_ID}'
    )
""")

events.log("run_completed", new_status=run_status,
           detail=f"parsed={stats['parsed']} skipped={stats['skipped']} errored={stats['errored']}")
events.flush()

print(f"\n{'═' * 60}")
print(f"  RUN SUMMARY")
print(f"{'═' * 60}")
print(f"  Run ID:     {RUN_ID}")
print(f"  Day ID:     {DAY_ID}")
print(f"  Status:     {run_status}")
print(f"  Duration:   {run_duration_min:.1f} min")
print(f"  Parsed:     {stats['parsed']}")
print(f"  Skipped:    {stats['skipped']} (>{MAX_FILE_SIZE_MB}MB)")
print(f"  Errors:     {stats['errored']}")
print(f"{'═' * 60}")

dbutils.notebook.exit(f"{{\"run_id\": \"{RUN_ID}\", \"day_id\": \"{DAY_ID}\", \"status\": \"{run_status}\", \"parsed\": {stats['parsed']}}}")
