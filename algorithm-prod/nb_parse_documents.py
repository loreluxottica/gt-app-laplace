# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Config
# ══════════════════════════════════════════════════════════════════════
# nb_parse_documents — Production PDF Parsing Pipeline
# ══════════════════════════════════════════════════════════════════════
# Task 1 of the multi-document splitting job.
# Scans inbox volume, parses PDFs with ai_parse_document,
# writes results to UC managed table, logs everything.
# ══════════════════════════════════════════════════════════════════════

import uuid
from datetime import datetime

# ── Schema & Paths ──
CATALOG = "sbx-logistics"
SCHEMA = "multidocument-us"

INBOX_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/inbox"
MANUAL_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/manual"
CHECK_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/check"
ARCHIVE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/archive"

# ── Tables ──
TABLE_PARSED = f"`{CATALOG}`.`{SCHEMA}`.`parsed_documents`"
TABLE_LOG = f"`{CATALOG}`.`{SCHEMA}`.`processing_log`"
TABLE_RUN_HISTORY = f"`{CATALOG}`.`{SCHEMA}`.`run_history`"

# ── Processing parameters ──
MAX_FILE_SIZE_MB = 100  # Files above this are skipped (manual processing)
LARGE_FILE_THRESHOLD_MB = 74  # Files above this are parsed sequentially
CHUNK_SIZE = 30  # Files per parallel chunk
MAX_RETRIES = 2
RETRY_DELAY_SEC = 60

# ── Run ID (passed from job or generated) ──
try:
    RUN_ID = dbutils.widgets.get("run_id")
except:
    RUN_ID = str(uuid.uuid4())

RUN_START = datetime.now()

print(f"{'═' * 60}")
print(f"  nb_parse_documents — Production Run")
print(f"{'═' * 60}")
print(f"  Run ID:    {RUN_ID}")
print(f"  Started:   {RUN_START:%Y-%m-%d %H:%M:%S}")
print(f"  Inbox:     {INBOX_PATH}")
print(f"  Chunk:     {CHUNK_SIZE} files/batch")
print(f"  Max size:  {MAX_FILE_SIZE_MB} MB (skip above)")
print(f"{'═' * 60}")

# COMMAND ----------

# DBTITLE 1,Scan inbox and register files
# ── Scan inbox volume for PDFs ──
from pyspark.sql.functions import (
    col, regexp_extract, current_timestamp, lit, when, round as spark_round
)

# Read binary metadata (no content yet, just listing)
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
    print("⚠️  Inbox empty — no PDFs to process. Early termination.")
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

print(f"PDF found in inbox: {n_total}")
print(f"  Standard (≤{LARGE_FILE_THRESHOLD_MB}MB): {n_standard}")
print(f"  Large ({LARGE_FILE_THRESHOLD_MB}-{MAX_FILE_SIZE_MB}MB): {n_large}")
print(f"  Oversized (>{MAX_FILE_SIZE_MB}MB): {n_oversized} → volume manual/")

# COMMAND ----------

# DBTITLE 1,Filter already-parsed files
# ── Escludi file già presenti in parsed_documents (idempotenza) ──

df_already_parsed = spark.sql(f"SELECT filename FROM {TABLE_PARSED}")

df_to_process = (
    df_inbox
    .join(df_already_parsed, on="filename", how="left_anti")
)

n_already_parsed = n_total - df_to_process.count()
n_to_process = df_to_process.filter(col("size_category") != "oversized").count()
n_oversized_new = df_to_process.filter(col("size_category") == "oversized").count()

if n_already_parsed > 0:
    print(f"ℹ️  {n_already_parsed} file already parsed (skip)")

if n_to_process == 0 and n_oversized_new == 0:
    print("ℹ️  All files have already been processed. Early termination.")
    dbutils.notebook.exit("ALL_ALREADY_PARSED")

print(f"\nFile to process: {n_to_process}")
print(f"Oversized files to move to manual: {n_oversized_new}")

# COMMAND ----------

# DBTITLE 1,Register in processing_log and handle oversized
# ── Register ALL new files in processing_log ──
from pyspark.sql.functions import lit, current_timestamp

df_log_entries = (
    df_to_process
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
        lit(None).cast("string").alias("sftp_delivery_error")
    )
)

df_log_entries.write.format("delta").mode("append").saveAsTable(
    f"`{CATALOG}`.`{SCHEMA}`.`processing_log`"
)
print(f"✓ {df_to_process.count()} entries registered in processing_log")

# ── Move oversized files to manual/ volume ──
import shutil, os

oversized_files = (
    df_to_process
    .filter(col("size_category") == "oversized")
    .select("filename", "path")
    .collect()
)

for row in oversized_files:
    src = row["path"].replace("dbfs:", "/dbfs") if row["path"].startswith("dbfs:") else row["path"]
    # Copy to manual volume
    dst = f"{MANUAL_PATH}/{row['filename']}.pdf"
    dbutils.fs.cp(row["path"], dst)
    print(f"  → {row['filename']}.pdf ({MAX_FILE_SIZE_MB}MB+) → manual/")

if oversized_files:
    print(f"✓ {len(oversized_files)} file moved to manual/")

# COMMAND ----------

# DBTITLE 1,Parse standard files (batch parallel)
# ── Parsing batch parallelizzato per file standard (≤ 74MB) ──
from pyspark.sql.functions import col, regexp_extract, current_timestamp, expr
import time


CHUNK_SIZE = 100
df_standard = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.pdf")
    .option("recursiveFileLookup", "true")
    .load(f"{INBOX_PATH}/")
    .filter(col("length") <= LARGE_FILE_THRESHOLD_MB * 1024 * 1024)
    .withColumn("filename", regexp_extract(col("path"), r'([^/]+)\.pdf$', 1))
)

# Filter to only unprocessed files
df_standard = df_standard.join(
    df_already_parsed, on="filename", how="left_anti"
)

n_standard_to_parse = df_standard.count()
print(f"Parsing standard files: {n_standard_to_parse} (≤{LARGE_FILE_THRESHOLD_MB}MB)")

# Update processing_log to 'parsing'
if n_standard_to_parse > 0:
    spark.sql(f"""
        UPDATE {TABLE_LOG}
        SET status = 'parsing', started_at = current_timestamp()
        WHERE run_id = '{RUN_ID}' AND status = 'pending'
    """)

# Collect file paths for chunked processing
file_paths = [row["path"] for row in df_standard.select("path").collect()]

# ⚠️ TEST LIMIT — rimuovere per run completo
TEST_LIMIT = None  # Set to None for full production run
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
    
    # Load chunk of files
    df_chunk = (
        spark.read.format("binaryFile")
        .load(chunk_paths)
        .withColumn("filename", regexp_extract(col("path"), r'([^/]+)\.pdf$', 1))
        .withColumn("folder_id", regexp_extract(col("filename"), r'^([A-Za-z0-9]+)_', 1))
        .withColumn("file_size_mb", col("length") / (1024 * 1024))
    )
    
    # Parse with ai_parse_document
    df_parsed_chunk = (
        df_chunk
        .withColumn("parsed_content",
            expr("ai_parse_document(content, map('version', '2.0', 'descriptionElementTypes', ''))"))
        .withColumn("parsed_at", current_timestamp())
        .withColumn("run_id", lit(RUN_ID))
        .withColumn("source_path", col("path"))
        # Extract page count: max(page_id) + 1 using aggregate (avoids correlated subquery on VARIANT)
        .withColumn("n_pages",
            expr("""
                COALESCE(
                    aggregate(
                        try_cast(parsed_content:document:elements AS ARRAY<VARIANT>),
                        0,
                        (acc, e) -> GREATEST(acc, COALESCE(CAST(e:bbox[0]:page_id AS INT), 0))
                    ) + 1,
                    1
                )
            """))
    )
    
    df_result = df_parsed_chunk.select(
        "filename", "folder_id", "source_path", "file_size_mb", "n_pages",
        expr("CAST(parsed_content AS STRING)").alias("parsed_content"),
        "parsed_at", "run_id"
    )
    
    # Append to UC table
    df_result.write.format("delta").mode("append").saveAsTable(
        f"`{CATALOG}`.`{SCHEMA}`.`parsed_documents`"
    )
    
    chunk_count = len(chunk_paths)
    parsed_count += chunk_count
    chunk_elapsed = time.time() - chunk_start
    
    print(f"  ✓ Chunk {chunk_idx+1}/{len(chunks)}: {chunk_count} file in {chunk_elapsed:.0f}s "
          f"({chunk_elapsed/chunk_count:.1f}s/file) | Totale: {parsed_count}")

elapsed_standard = time.time() - start_time
print(f"\n✓ Standard file parsing complete: {parsed_count} file in {elapsed_standard/60:.1f} min")

# COMMAND ----------

# DBTITLE 1,Parse large files (74-100MB, sequential)
# ── Parsing sequenziale per file grandi (74-100MB) ──
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
)

# Filter to only unprocessed
df_large = df_large.join(df_already_parsed, on="filename", how="left_anti")
n_large = df_large.count()

if n_large == 0:
    print("Nessun file large (74-100MB) da processare.")
else:
    print(f"Parsing sequenziale: {n_large} file large ({LARGE_FILE_THRESHOLD_MB}-{MAX_FILE_SIZE_MB}MB)")
    large_files = df_large.select("path", "filename").collect()
    
    large_parsed = 0
    large_errors = 0
    
    for i, row in enumerate(large_files):
        file_start = time.time()
        retries = 0
        success = False
        
        while retries <= MAX_RETRIES and not success:
            try:
                df_single = (
                    spark.read.format("binaryFile")
                    .load([row["path"]])
                    .withColumn("filename", regexp_extract(col("path"), r'([^/]+)\.pdf$', 1))
                    .withColumn("folder_id", regexp_extract(col("filename"), r'^([A-Za-z0-9]+)_', 1))
                    .withColumn("file_size_mb", col("length") / (1024 * 1024))
                    .withColumn("parsed_content",
                        expr("ai_parse_document(content, map('version', '2.0', 'descriptionElementTypes', ''))"))
                    .withColumn("parsed_at", current_timestamp())
                    .withColumn("run_id", lit(RUN_ID))
                    .withColumn("source_path", col("path"))
                )
                
                df_single_result = (
                    df_single
                    .withColumn("n_pages", expr("""
                        COALESCE(
                            aggregate(
                                try_cast(parsed_content:document:elements AS ARRAY<VARIANT>),
                                0,
                                (acc, e) -> GREATEST(acc, COALESCE(CAST(e:bbox[0]:page_id AS INT), 0))
                            ) + 1,
                            1
                        )
                    """))
                    .select(
                        "filename", "folder_id", "source_path", "file_size_mb", "n_pages",
                        expr("CAST(parsed_content AS STRING)").alias("parsed_content"),
                        "parsed_at", "run_id"
                    )
                )
                
                df_single_result.write.format("delta").mode("append").saveAsTable(
                    f"`{CATALOG}`.`{SCHEMA}`.`parsed_documents`"
                )
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
                    error_msg = str(e)[:500]
                    # Log error in processing_log
                    spark.sql(f"""
                        UPDATE {TABLE_LOG}
                        SET status = 'error',
                            error_message = '{error_msg.replace(chr(39), chr(39)+chr(39))}',
                            error_stage = 'parsing',
                            retry_count = {MAX_RETRIES},
                            completed_at = current_timestamp()
                        WHERE run_id = '{RUN_ID}' AND filename = '{row["filename"]}'
                    """)
                    print(f"  ✗ [{i+1}/{n_large}] {row['filename']} FAILED after {MAX_RETRIES} retries")
    
    print(f"\n✓ Large files: {large_parsed} parsed, {large_errors} errori")

# COMMAND ----------

# DBTITLE 1,Update processing_log and select 10% for check
# ── Aggiorna processing_log con risultati parsing ──
from pyspark.sql.functions import col

# Get parsed file info from parsed_documents
df_just_parsed = spark.sql(f"""
    SELECT filename, n_pages
    FROM {TABLE_PARSED}
    WHERE run_id = '{RUN_ID}'
""")

n_parsed_total = df_just_parsed.count()

# Update processing_log: status = parsed for successful files
if n_parsed_total > 0:
    spark.sql(f"""
        MERGE INTO {TABLE_LOG} AS log
        USING (
            SELECT filename, n_pages
            FROM {TABLE_PARSED}
            WHERE run_id = '{RUN_ID}'
        ) AS parsed
        ON log.filename = parsed.filename AND log.run_id = '{RUN_ID}'
        WHEN MATCHED AND log.status IN ('pending', 'parsing') THEN UPDATE SET
            log.status = 'parsed',
            log.n_pages = parsed.n_pages,
            log.completed_at = current_timestamp()
    """)
    print(f"✓ processing_log aggiornato: {n_parsed_total} file → status='parsed'")

# ── Seleziona 10% random per ground truth check ──
import random

parsed_filenames = [row["filename"] for row in df_just_parsed.collect()]
sample_size = max(1, int(len(parsed_filenames) * 0.10))
check_sample = random.sample(parsed_filenames, min(sample_size, len(parsed_filenames)))

print(f"\nGround truth sample: {len(check_sample)}/{n_parsed_total} file (10%) → check/")

for fname in check_sample:
    src = f"{INBOX_PATH}/{fname}.pdf"
    dst = f"{CHECK_PATH}/{fname}.pdf"
    try:
        dbutils.fs.cp(src, dst)
    except Exception as e:
        print(f"  ⚠️  Copy failed for {fname}: {e}")

print(f"✓ {len(check_sample)} file copiati in check/")

# COMMAND ----------

# DBTITLE 1,Run summary
# ── Run Summary ──
from datetime import datetime
import time

RUN_END = datetime.now()
run_duration_min = (RUN_END - RUN_START).total_seconds() / 60

# Count final stats
stats = spark.sql(f"""
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN status = 'parsed' THEN 1 ELSE 0 END) AS parsed,
        SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skipped,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errored
    FROM {TABLE_LOG}
    WHERE run_id = '{RUN_ID}'
""").first()

# Determine run status
if stats['errored'] > 0 and stats['parsed'] > 0:
    run_status = "partial_failure"
elif stats['errored'] > 0 and stats['parsed'] == 0:
    run_status = "failed"
else:
    run_status = "success"

# Write to run_history
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
        NULL
    )
""")

print(f"\n{'═' * 60}")
print(f"  RUN SUMMARY")
print(f"{'═' * 60}")
print(f"  Run ID:     {RUN_ID}")
print(f"  Status:     {run_status}")
print(f"  Duration:   {run_duration_min:.1f} min")
print(f"  Parsed:     {stats['parsed']}")
print(f"  Skipped:    {stats['skipped']} (>{MAX_FILE_SIZE_MB}MB)")
print(f"  Errors:     {stats['errored']}")
print(f"  Check (GT): {len(check_sample)} file")
print(f"{'═' * 60}")

# Exit with status for downstream tasks
dbutils.notebook.exit(f"{{\"run_id\": \"{RUN_ID}\", \"status\": \"{run_status}\", \"parsed\": {stats['parsed']}}}")