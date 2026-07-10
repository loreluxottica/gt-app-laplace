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
# nb_split_documents — Production GCS Document Boundary Detection
# ══════════════════════════════════════════════════════════════════════
# Task 2 of job_ingest (parse → split → check_export).
# Reads parsed PDFs of the day_id batch, extracts features, runs GCS
# segmentation with LLM (Sonnet primary, Maverick fallback),
# verification pass, writes boundary results to split_results.
# Every result carries boundary_source; total LLM failures are tagged
# needs_review=true so they can never be delivered unsplit silently.
# ══════════════════════════════════════════════════════════════════════

import json, re, time
from datetime import datetime

TABLE_PARSED = f"`{CATALOG}`.`{SCHEMA}`.`parsed_documents`"
TABLE_PAGE_SIGNALS = f"`{CATALOG}`.`{SCHEMA}`.`page_signals`"
TABLE_PKG_SUMMARIES = f"`{CATALOG}`.`{SCHEMA}`.`package_summaries`"
TABLE_LLM_RESPONSES = f"`{CATALOG}`.`{SCHEMA}`.`gcs_llm_responses`"
TABLE_SPLIT_RESULTS = f"`{CATALOG}`.`{SCHEMA}`.`split_results`"
TABLE_RUN_HISTORY = f"`{CATALOG}`.`{SCHEMA}`.`run_history`"

# ── LLM Models ──
PRIMARY_MODEL = "databricks-claude-sonnet-4"
FALLBACK_MODEL = "databricks-llama-4-maverick"

# ── Processing parameters ──
MAX_PAGES_SINGLE_CALL = 50  # PDFs above this use chunked strategy
CHUNK_OVERLAP = 3
MAX_TOKENS_S = 500   # Token budget: ≤30 pages
MAX_TOKENS_M = 800   # Token budget: 31-60 pages
MAX_TOKENS_L = 2000  # Token budget: >60 pages

# ── Batch + run identity (job parameters) ──
DAY_ID = get_day_id()
RUN_ID = get_run_id()

# Temp Delta storage, per-batch so concurrent day_ids never collide
# (cleaned up at end of run ONLY on success — kept for the recovery cell otherwise)
TMP_BASE = f"abfss://logistics@elddaapsbxsdlsd07.dfs.core.windows.net/{CATALOG}/{SCHEMA}/tmp/{DAY_ID}"

RUN_START = datetime.now()
events = EventLogger(RUN_ID, DAY_ID, stage="split")
events.log("run_started")
events.flush()

print(f"{'═' * 60}")
print(f"  nb_split_documents — Production Run")
print(f"{'═' * 60}")
print(f"  Run ID:       {RUN_ID}")
print(f"  Day ID:       {DAY_ID}")
print(f"  Started:      {RUN_START:%Y-%m-%d %H:%M:%S}")
print(f"  Primary LLM:  {PRIMARY_MODEL}")
print(f"  Fallback LLM: {FALLBACK_MODEL}")
print(f"  Max single:   {MAX_PAGES_SINGLE_CALL} pages")
print(f"{'═' * 60}")

# COMMAND ----------

# DBTITLE 1,RECOVERY — Ripristino sessione da materializzazioni esistenti
# # ══════════════════════════════════════════════════════════════════════
# # RECOVERY CELL — Usare solo dopo restart della sessione.
# # Ripristina variabili e temp views dai Delta già materializzati su ABFSS.
# # NON ri-esegue nessuna chiamata LLM. Saltare questa cella in un run normale.
# # ══════════════════════════════════════════════════════════════════════
# from pyspark.sql.functions import lit, col, concat, when, udf, current_timestamp, array
# from pyspark.sql.types import ArrayType, IntegerType
# import re as _re_parse, json as _json_parse

# # ── Ripristina parse_gcs_response UDF (da Cell 6) ──
# @udf(returnType=ArrayType(IntegerType()))
# def parse_gcs_response(raw: str, total_pages: int) -> list:
#     if raw is None or not isinstance(raw, str):
#         return None
#     cleaned = _re_parse.sub(r'```(?:json)?\s*', '', raw).strip()
#     match = _re_parse.search(r'\{.*?\}', cleaned, _re_parse.DOTALL)
#     if not match:
#         return None
#     try:
#         obj = _json_parse.loads(match.group(0))
#         starts = sorted(set(int(x) for x in obj.get('predicted_starts', [1])))
#         starts = [p for p in starts if 1 <= p <= total_pages]
#         if 1 not in starts:
#             starts = [1] + starts
#         return sorted(set(starts))
#     except Exception:
#         return None

# # ── Ripristina GCS_SYSTEM (da Cell 5) ──
# GCS_SYSTEM = """<SYSTEM_INSTRUCTIONS>
# You are an expert logistics document analyst.
# You receive a summary of an entire multi-page PDF package containing multiple concatenated documents.
# Each page is described with: first_element, header, doc_ref, page_indicator, form_number,
# invoice_number, footer_pagination, text_start, text_end.

# Your task: identify the FIRST PAGE of each distinct document in the package.
# Page 1 is ALWAYS the start of the first document.

# Key signals for a new document boundary:
# - invoice_number changes between pages \u2192 CERTAIN boundary
# - invoice_number is present but text_end is nearly identical to previous page \u2192 DUPLICATE COPY,
#   each copy is a SEPARATE document. Mark every duplicate as a new boundary.
#   Look for 'Page 1/1' or 'Page 1/ 1' in text_end \u2014 if multiple consecutive pages all end 
#   with 'Page 1/1', each is a separate single-page document.
# - page_indicator shows '1 of N' or '- 1 / N -' \u2192 very strong signal
# - form_number changes or appears \u2192 certain boundary
# - header changes significantly \u2192 strong signal
# - doc_ref changes \u2192 strong signal
# - SAME_TYPE_NEW_INSTANCE: same type, different instance \u2192 boundary

# Key signals that a page CONTINUES the previous document:
# - page_indicator shows 'Page N of M' where N > 1 \u2192 strong continuation signal
# - text_end shows page number > 1 (e.g. 'Page 2/5') \u2192 continuation
# - Same invoice_number AND text flows naturally \u2192 continuation

# Return ONLY a valid JSON object, no explanation, no markdown:
# {"predicted_starts": [1, <page>, <page>, ...]}

# The list must be sorted ascending and always include 1.
# </SYSTEM_INSTRUCTIONS>"""

# # ── Ripristina df_all_parsed da gcs_parsed materializzato ──
# try:
#     df_all_parsed = spark.read.format("delta").load(f"{TMP_BASE}/gcs_parsed")
#     df_all_parsed.createOrReplaceTempView("gcs_parsed")
#     n_gcs = df_all_parsed.count()
#     print(f"\u2713 gcs_parsed ripristinato: {n_gcs} PDF")
# except Exception as e:
#     df_all_parsed = None
#     print(f"\u26a0\ufe0f  gcs_parsed non trovato in TMP_BASE ({type(e).__name__})")
#     print("   Il run precedente è probabilmente già terminato e TMP_BASE è stato pulito.")
#     print("   Riesegui dal Cell 6 (GCS Primary Pass) per rigenerare gcs_parsed.")

# # ── Ripristina temp views da UC tables (serve per Cell 9 verification) ──
# spark.sql(f"SELECT * FROM `{CATALOG}`.`{SCHEMA}`.`page_signals`").createOrReplaceTempView("page_signals")
# print(f"\u2713 page_signals temp view ripristinata")

# spark.sql(f"SELECT * FROM `{CATALOG}`.`{SCHEMA}`.`package_summaries`").createOrReplaceTempView("package_summaries")
# print(f"\u2713 package_summaries temp view ripristinata")

# print(f"\n\u2713 Recovery completata \u2014 puoi rieseguire da Cell 8")

# COMMAND ----------

# DBTITLE 1,Load parsed documents (unprocessed only)
# ── Load parsed documents of THIS batch not yet split (key: day_id + filename) ──
from pyspark.sql.functions import col

df_already_split = spark.sql(
    f"SELECT filename FROM {TABLE_SPLIT_RESULTS} WHERE day_id = '{DAY_ID}'"
)

df_parsed = (
    spark.sql(f"SELECT filename, folder_id, n_pages, parsed_content FROM {TABLE_PARSED} "
              f"WHERE day_id = '{DAY_ID}'")
    .join(df_already_split, on="filename", how="left_anti")
)

n_to_split = df_parsed.count()

if n_to_split == 0:
    events.log("run_completed", new_status="NO_FILES_TO_SPLIT")
    events.flush()
    print("ℹ️  No new parsed documents to split. Early termination.")
    dbutils.notebook.exit("NO_FILES_TO_SPLIT")

df_parsed.createOrReplaceTempView("parsed_docs")
print(f"Documents to split: {n_to_split}")
print(f"Page range: {df_parsed.agg({'n_pages': 'min'}).first()[0]} - {df_parsed.agg({'n_pages': 'max'}).first()[0]}")

# COMMAND ----------

# DBTITLE 1,Feature extraction: page_signals
# ── Stage 1: Feature extraction (page elements → page signals) ──
from pyspark.sql.functions import lit

# Step 1: Explode elements per page
df_page_elements = spark.sql("""
SELECT
  filename,
  CAST(elem:bbox[0]:page_id AS INT) + 1 AS page_num,
  CAST(elem:type AS STRING)             AS element_type,
  CAST(elem:content AS STRING)          AS content,
  CAST(elem:id AS INT)                  AS element_id
FROM (
  SELECT filename, explode(try_cast(parse_json(parsed_content):document:elements AS ARRAY<VARIANT>)) AS elem
  FROM parsed_docs
)
""")
df_page_elements.createOrReplaceTempView("page_elements")

# Step 2: Aggregate to page signals
df_page_signals = spark.sql("""
WITH first_hdrs AS (
  SELECT filename, page_num,
    first_value(content) AS first_element
  FROM page_elements
  WHERE element_type IN ('title', 'sectionHeading') AND element_id <= 3
  GROUP BY filename, page_num
),
page_text AS (
  SELECT filename, page_num,
    concat_ws(' ', collect_list(content)) AS full_text
  FROM page_elements
  GROUP BY filename, page_num
),
max_pages AS (
  SELECT filename, MAX(page_num) AS total_pages
  FROM page_elements
  GROUP BY filename
)
SELECT
  pt.filename,
  pt.page_num,
  mp.total_pages,
  COALESCE(fh.first_element, '') AS first_element,
  -- Header: first 200 chars
  LEFT(pt.full_text, 200) AS header,
  -- Doc reference patterns
  COALESCE(regexp_extract(pt.full_text, '(INV[A-Z]*[\\\\s:# -]*[A-Z0-9-]+)', 0), '') AS doc_ref,
  -- Page indicator (Page X of Y)
  COALESCE(regexp_extract(pt.full_text, '([Pp]age\\\\s+\\\\d+\\\\s*(/|of)\\\\s*\\\\d+|-\\\\s*\\\\d+\\\\s*/\\\\s*\\\\d+\\\\s*-)', 0), '') AS page_indicator,
  -- Form number (CBP forms)
  COALESCE(regexp_extract(pt.full_text, '(Form\\\\s*(?:CBP\\\\s*)?(?:7501|3461|3311|7512|3299))', 0), '') AS form_number,
  -- Invoice number
  COALESCE(regexp_extract(pt.full_text, '(?i)invoice\\\\s*(?:no|number|#|:)?\\\\s*[:\\\\s]?\\\\s*([A-Z0-9][A-Z0-9/.-]+)', 1), '') AS invoice_number,
  -- Footer pagination
  COALESCE(regexp_extract(RIGHT(pt.full_text, 100), '([Pp]age\\\\s+\\\\d+\\\\s*/\\\\s*\\\\d+)', 0), '') AS footer_pagination,
  -- Text start/end for context
  LEFT(pt.full_text, 300) AS text_start,
  RIGHT(pt.full_text, 300) AS text_end,
  -- Page block (compact representation for LLM)
  CONCAT(
    'first_element: ', COALESCE(fh.first_element, ''), '\n',
    'header: ', LEFT(pt.full_text, 150), '\n',
    'doc_ref: ', COALESCE(regexp_extract(pt.full_text, '(INV[A-Z]*[\\\\s:# -]*[A-Z0-9-]+)', 0), ''), '\n',
    'page_indicator: ', COALESCE(regexp_extract(pt.full_text, '([Pp]age\\\\s+\\\\d+\\\\s*(/|of)\\\\s*\\\\d+|-\\\\s*\\\\d+\\\\s*/\\\\s*\\\\d+\\\\s*-)', 0), ''), '\n',
    'form_number: ', COALESCE(regexp_extract(pt.full_text, '(Form\\\\s*(?:CBP\\\\s*)?(?:7501|3461|3311|7512|3299))', 0), ''), '\n',
    'invoice_number: ', COALESCE(regexp_extract(pt.full_text, '(?i)invoice\\\\s*(?:no|number|#|:)?\\\\s*[:\\\\s]?\\\\s*([A-Z0-9][A-Z0-9/.-]+)', 1), ''), '\n',
    'footer_pagination: ', COALESCE(regexp_extract(RIGHT(pt.full_text, 100), '([Pp]age\\\\s+\\\\d+\\\\s*/\\\\s*\\\\d+)', 0), ''), '\n',
    'text_start: ', LEFT(pt.full_text, 300), '\n',
    'text_end: ', RIGHT(pt.full_text, 300)
  ) AS page_block,
  '{RUN_ID}' AS run_id,
  '{DAY_ID}' AS day_id
FROM page_text pt
JOIN max_pages mp ON pt.filename = mp.filename
LEFT JOIN first_hdrs fh ON pt.filename = fh.filename AND pt.page_num = fh.page_num
""".replace("{RUN_ID}", RUN_ID).replace("{DAY_ID}", DAY_ID))

# Delete-before-append: a rerun must not duplicate this batch's page signals
spark.sql(f"""
    DELETE FROM {TABLE_PAGE_SIGNALS}
    WHERE day_id = '{DAY_ID}'
      AND filename IN (SELECT filename FROM parsed_docs)
""")
df_page_signals.write.format("delta").mode("append").saveAsTable(
    f"`{CATALOG}`.`{SCHEMA}`.`page_signals`"
)
df_page_signals.createOrReplaceTempView("page_signals")

n_pages_total = df_page_signals.count()
print(f"✓ Feature extraction complete: {n_pages_total} pages from {n_to_split} PDFs")

# COMMAND ----------

# DBTITLE 1,Package summary builder
# ── Stage 2: Build package summaries (one string per PDF) ──
from pyspark.sql.functions import (
    col, collect_list, struct, sort_array, concat_ws,
    max as spark_max, count as spark_count, lit
)

df_pkg = (
    df_page_signals
    .groupBy("filename")
    .agg(
        spark_max("total_pages").alias("total_pages"),
        sort_array(
            collect_list(
                struct(col("page_num").alias("sort_key"), col("page_block").alias("text"))
            )
        ).alias("page_blocks_sorted")
    )
    .selectExpr(
        "filename",
        "total_pages",
        """concat_ws('\n',
            transform(page_blocks_sorted, x -> concat('=== PAGE ', x.sort_key, ' ===\n', x.text))
        ) AS package_summary"""
    )
)

# Add folder_id from parsed_docs
df_pkg_final = (
    df_pkg
    .join(
        spark.sql("SELECT filename, folder_id FROM parsed_docs"),
        on="filename"
    )
    .withColumn("run_id", lit(RUN_ID))
    .withColumn("day_id", lit(DAY_ID))
)

# Delete-before-append: a rerun must not duplicate this batch's summaries
spark.sql(f"""
    DELETE FROM {TABLE_PKG_SUMMARIES}
    WHERE day_id = '{DAY_ID}'
      AND filename IN (SELECT filename FROM parsed_docs)
""")
df_pkg_final.select("filename", "folder_id", "total_pages", "package_summary", "run_id", "day_id") \
    .write.format("delta").mode("append").saveAsTable(
        f"`{CATALOG}`.`{SCHEMA}`.`package_summaries`"
    )

df_pkg_final.createOrReplaceTempView("package_summaries")

stats = spark.sql("""
    SELECT COUNT(*) AS n_pdfs,
           AVG(total_pages) AS avg_pages,
           MAX(total_pages) AS max_pages,
           SUM(CASE WHEN total_pages > 50 THEN 1 ELSE 0 END) AS n_large
    FROM package_summaries
""").first()

print(f"✓ Package summaries: {stats['n_pdfs']} PDFs")
print(f"  Avg pages: {stats['avg_pages'] if stats['avg_pages'] is not None else 0:.1f}, Max: {stats['max_pages']}")
print(f"  Large (>{MAX_PAGES_SINGLE_CALL}p, chunked): {stats['n_large']}")

# COMMAND ----------

# DBTITLE 1,GCS Primary Pass (Sonnet)
# ── Stage 3: GCS Primary Pass — Sonnet ──
from pyspark.sql.functions import lit, col, concat, when, length

GCS_SYSTEM = """<SYSTEM_INSTRUCTIONS>
You are an expert logistics document analyst.
You receive a summary of an entire multi-page PDF package containing multiple concatenated documents.
Each page is described with: first_element, header, doc_ref, page_indicator, form_number,
invoice_number, footer_pagination, text_start, text_end.

Your task: identify the FIRST PAGE of each distinct document in the package.
Page 1 is ALWAYS the start of the first document.

Key signals for a new document boundary:
- invoice_number changes between pages → CERTAIN boundary
- invoice_number is present but text_end is nearly identical to previous page → DUPLICATE COPY,
  each copy is a SEPARATE document. Mark every duplicate as a new boundary.
  Look for 'Page 1/1' or 'Page 1/ 1' in text_end — if multiple consecutive pages all end 
  with 'Page 1/1', each is a separate single-page document.
- page_indicator shows '1 of N' or '- 1 / N -' → very strong signal
- form_number changes or appears → certain boundary
- header changes significantly → strong signal
- doc_ref changes → strong signal
- SAME_TYPE_NEW_INSTANCE: same type, different instance → boundary

Key signals that a page CONTINUES the previous document:
- page_indicator shows 'Page N of M' where N > 1 → strong continuation signal
- text_end shows page number > 1 (e.g. 'Page 2/5') → continuation
- Same invoice_number AND text flows naturally → continuation

Return ONLY a valid JSON object, no explanation, no markdown:
{"predicted_starts": [1, <page>, <page>, ...]}

The list must be sorted ascending and always include 1.
</SYSTEM_INSTRUCTIONS>"""

# Build prompts with token budgets
df_prompts = spark.sql(f"""
    SELECT
      filename, total_pages, package_summary,
      CASE
        WHEN total_pages <= 30 THEN 'S'
        WHEN total_pages <= 60 THEN 'M'
        ELSE 'L'
      END AS token_bucket
    FROM package_summaries
    WHERE total_pages <= {MAX_PAGES_SINGLE_CALL}
""")
df_prompts.createOrReplaceTempView("gcs_prompts")

n_single = df_prompts.count()
print(f"GCS Primary Pass: {n_single} PDFs (≤{MAX_PAGES_SINGLE_CALL} pages)")
print(f"  Calling {PRIMARY_MODEL} with failOnError=false...")

# Run ai_query per token bucket
for bucket, max_tokens in [("S", MAX_TOKENS_S), ("M", MAX_TOKENS_M), ("L", MAX_TOKENS_L)]:
    spark.sql(f"""
        SELECT
          filename, total_pages, token_bucket,
          ai_query(
            '{PRIMARY_MODEL}',
            CONCAT('{GCS_SYSTEM.replace(chr(39), chr(39)+chr(39))}',
                   '\n\n<INPUT>\n', package_summary, '\n</INPUT>\n\nOutput JSON:'),
            failOnError => false,
            modelParameters => named_struct('temperature', 0.1, 'max_tokens', {max_tokens})
          ) AS llm_result
        FROM gcs_prompts
        WHERE token_bucket = '{bucket}'
    """).createOrReplaceTempView(f"gcs_raw_{bucket}")

df_gcs_raw = spark.sql("""
    SELECT * FROM gcs_raw_S
    UNION ALL SELECT * FROM gcs_raw_M
    UNION ALL SELECT * FROM gcs_raw_L
""")

df_gcs_raw = (
    df_gcs_raw
    .withColumn("raw_response", col("llm_result.result"))
    .withColumn("error_message", col("llm_result.errorMessage"))
    .drop("llm_result")
)

# ── Materializzazione: spezza lazy chain per evitare ri-esecuzioni ai_query ──
df_gcs_raw.write.format("delta").mode("overwrite").save(f"{TMP_BASE}/gcs_raw_primary")
df_gcs_raw = spark.read.format("delta").load(f"{TMP_BASE}/gcs_raw_primary")
df_gcs_raw.createOrReplaceTempView("gcs_raw_primary")

n_errors = df_gcs_raw.filter(col("error_message").isNotNull()).count()
n_success = df_gcs_raw.filter(col("error_message").isNull()).count()
print(f"✓ Primary pass complete: {n_success} success, {n_errors} errors")

# COMMAND ----------

# DBTITLE 1,Parse responses and identify failures for fallback
# ── Parse LLM responses + identify failures for Maverick fallback ──
from pyspark.sql.functions import udf, col, lit, current_timestamp, array
from pyspark.sql.types import ArrayType, IntegerType
import re as _re_parse, json as _json_parse

@udf(returnType=ArrayType(IntegerType()))
def parse_gcs_response(raw: str, total_pages: int) -> list:
    """Parse LLM JSON response into sorted list of boundary pages."""
    if raw is None or not isinstance(raw, str):
        return None  # Signal failure
    cleaned = _re_parse.sub(r'```(?:json)?\s*', '', raw).strip()
    match = _re_parse.search(r'\{.*?\}', cleaned, _re_parse.DOTALL)
    if not match:
        return None  # Unparsable
    try:
        obj = _json_parse.loads(match.group(0))
        starts = sorted(set(int(x) for x in obj.get('predicted_starts', [1])))
        starts = [p for p in starts if 1 <= p <= total_pages]
        if 1 not in starts:
            starts = [1] + starts
        return sorted(set(starts))
    except Exception:
        return None  # JSON parse failure

df_parsed_primary = (
    df_gcs_raw
    .withColumn("parsed_starts", parse_gcs_response(col("raw_response"), col("total_pages")))
)

# Failures = LLM error OR unparsable response
df_failures = df_parsed_primary.filter(
    (col("error_message").isNotNull()) | (col("parsed_starts").isNull())
)
df_successes = df_parsed_primary.filter(
    (col("error_message").isNull()) & (col("parsed_starts").isNotNull())
)

n_failures = df_failures.count()
n_successes_primary = df_successes.count()

print(f"Primary pass results:")
print(f"  Success (parsable): {n_successes_primary}")
print(f"  Failures (need fallback): {n_failures}")

# Log primary responses to UC table
df_log_primary = df_parsed_primary.select(
    col("filename"),
    lit("primary_pass").alias("stage"),
    lit(PRIMARY_MODEL).alias("model_used"),
    col("token_bucket").alias("prompt_token_bucket"),
    col("raw_response"),
    col("error_message"),
    col("parsed_starts"),
    lit(False).alias("is_fallback"),
    current_timestamp().alias("processing_timestamp"),
    lit(RUN_ID).alias("run_id"),
    lit(DAY_ID).alias("day_id")
)
df_log_primary.write.format("delta").mode("append").saveAsTable(
    f"`{CATALOG}`.`{SCHEMA}`.`gcs_llm_responses`"
)
print(f"✓ Primary responses logged to gcs_llm_responses")

# COMMAND ----------

# DBTITLE 1,Fallback: retry failures with Maverick
# ── Stage 3b: Retry failures with Maverick ──
if n_failures == 0:
    print("✓ No failures — fallback not needed.")
    df_all_parsed = (
        df_successes.select("filename", "total_pages", "parsed_starts", "token_bucket")
        .withColumn("boundary_source", lit("primary"))
    )
else:
    print(f"Retrying {n_failures} failures with {FALLBACK_MODEL}...")
    
    # Get failed filenames and rejoin with package_summaries for prompts
    failed_filenames = df_failures.select("filename")
    
    df_fallback_prompts = spark.sql(f"""
        SELECT filename, total_pages, package_summary,
          CASE
            WHEN total_pages <= 30 THEN 'S'
            WHEN total_pages <= 60 THEN 'M'
            ELSE 'L'
          END AS token_bucket
        FROM package_summaries
        WHERE total_pages <= {MAX_PAGES_SINGLE_CALL}
    """).join(failed_filenames, on="filename", how="inner")
    
    df_fallback_prompts.createOrReplaceTempView("fallback_prompts")
    
    # Run Maverick
    for bucket, max_tokens in [("S", MAX_TOKENS_S), ("M", MAX_TOKENS_M), ("L", MAX_TOKENS_L)]:
        spark.sql(f"""
            SELECT
              filename, total_pages, token_bucket,
              ai_query(
                '{FALLBACK_MODEL}',
                CONCAT('{GCS_SYSTEM.replace(chr(39), chr(39)+chr(39))}',
                       '\n\n<INPUT>\n', package_summary, '\n</INPUT>\n\nOutput JSON:'),
                failOnError => false,
                modelParameters => named_struct('temperature', 0.1, 'max_tokens', {max_tokens})
              ) AS llm_result
            FROM fallback_prompts
            WHERE token_bucket = '{bucket}'
        """).createOrReplaceTempView(f"fallback_raw_{bucket}")
    
    df_fallback_raw = spark.sql("""
        SELECT * FROM fallback_raw_S
        UNION ALL SELECT * FROM fallback_raw_M
        UNION ALL SELECT * FROM fallback_raw_L
    """)
    
    df_fallback_parsed = (
        df_fallback_raw
        .withColumn("raw_response", col("llm_result.result"))
        .withColumn("error_message", col("llm_result.errorMessage"))
        .drop("llm_result")
        .withColumn("parsed_starts", parse_gcs_response(col("raw_response"), col("total_pages")))
    )
    
    # Log fallback responses
    df_log_fallback = df_fallback_parsed.select(
        col("filename"),
        lit("retry_fallback").alias("stage"),
        lit(FALLBACK_MODEL).alias("model_used"),
        col("token_bucket").alias("prompt_token_bucket"),
        col("raw_response"),
        col("error_message"),
        col("parsed_starts"),
        lit(True).alias("is_fallback"),
        current_timestamp().alias("processing_timestamp"),
        lit(RUN_ID).alias("run_id"),
        lit(DAY_ID).alias("day_id")
    )
    df_log_fallback.write.format("delta").mode("append").saveAsTable(
        f"`{CATALOG}`.`{SCHEMA}`.`gcs_llm_responses`"
    )
    
    # Fallback successes
    df_fallback_ok = df_fallback_parsed.filter(
        (col("error_message").isNull()) & (col("parsed_starts").isNotNull())
    )
    # Ultimate failures: fallback to [1] BUT tagged needs_review — a package that
    # defeated both models must never be delivered unsplit silently.
    df_ultimate_failures = df_fallback_parsed.filter(
        (col("error_message").isNotNull()) | (col("parsed_starts").isNull())
    ).withColumn("parsed_starts", array(lit(1)))

    n_fallback_ok = df_fallback_ok.count()
    n_ultimate_fail = df_ultimate_failures.count()
    print(f"  Maverick results: {n_fallback_ok} recovered, {n_ultimate_fail} ultimate failures (→ [1], needs_review)")

    for r in df_ultimate_failures.select("filename").collect():
        events.log("needs_review", filename=r["filename"],
                   detail="both LLMs failed — [1] fallback, delivery blocked until approved")
    events.flush()

    # Combine all results, tracking where each boundary set came from
    df_all_parsed = (
        df_successes.select("filename", "total_pages", "parsed_starts", "token_bucket")
        .withColumn("boundary_source", lit("primary"))
        .unionByName(
            df_fallback_ok.select("filename", "total_pages", "parsed_starts", "token_bucket")
            .withColumn("boundary_source", lit("fallback"))
        )
        .unionByName(
            df_ultimate_failures.select("filename", "total_pages", "parsed_starts", "token_bucket")
            .withColumn("boundary_source", lit("ultimate_fallback"))
        )
    )
    print(f"✓ Fallback complete")

# ── Materializzazione: fissa risultati primary+fallback prima di verification ──
df_all_parsed.write.format("delta").mode("overwrite").save(f"{TMP_BASE}/gcs_parsed")
df_all_parsed = spark.read.format("delta").load(f"{TMP_BASE}/gcs_parsed")
df_all_parsed.createOrReplaceTempView("gcs_parsed")
print(f"\nTotal GCS parsed: {df_all_parsed.count()} PDFs")

# COMMAND ----------

# DBTITLE 1,Chunked processing for large PDFs (>50 pages)
# ── Stage 3c: Chunked strategy for PDFs > MAX_PAGES_SINGLE_CALL ──
from pyspark.sql import Row
from pyspark.sql.types import IntegerType as _IntType

df_large_pdfs = spark.sql(f"""
    SELECT filename, total_pages, package_summary
    FROM package_summaries
    WHERE total_pages > {MAX_PAGES_SINGLE_CALL}
""")
n_large = df_large_pdfs.count()

if n_large == 0:
    print(f"No large PDFs (>{MAX_PAGES_SINGLE_CALL} pages) — chunked processing skipped.")
else:
    print(f"Chunked processing: {n_large} large PDFs (>{MAX_PAGES_SINGLE_CALL} pages)")
    large_pdfs = df_large_pdfs.collect()
    chunk_rows = []
    
    for pdf_row in large_pdfs:
        fname = pdf_row['filename']
        total_p = int(pdf_row['total_pages'])
        pkg_summary = pdf_row['package_summary']
        
        page_blocks = {}
        for block in re.split(r'(?==== PAGE )', pkg_summary):
            m = re.match(r'=== PAGE (\d+) ===', block.strip())
            if m:
                page_blocks[int(m.group(1))] = block
        
        chunk_start = 1
        chunk_idx = 0
        while chunk_start <= total_p:
            chunk_end = min(chunk_start + MAX_PAGES_SINGLE_CALL - 1, total_p)
            chunk_summary = '\n'.join(
                page_blocks[p] for p in range(chunk_start, chunk_end + 1) if p in page_blocks
            )
            chunk_rows.append(Row(
                filename=fname, total_pages=total_p,
                chunk_idx=chunk_idx, chunk_start=chunk_start,
                chunk_end=chunk_end, chunk_summary=chunk_summary
            ))
            chunk_idx += 1
            if chunk_end >= total_p:
                break
            chunk_start = chunk_end + 1 - CHUNK_OVERLAP
    
    print(f"  Generated {len(chunk_rows)} chunks")
    
    df_chunks = (
        spark.createDataFrame(chunk_rows)
        .withColumn("total_pages", col("total_pages").cast(_IntType()))
        .withColumn("chunk_idx", col("chunk_idx").cast(_IntType()))
        .withColumn("chunk_start", col("chunk_start").cast(_IntType()))
        .withColumn("chunk_end", col("chunk_end").cast(_IntType()))
    )
    
    # Build chunk prompts
    df_chunk_prompts = (
        df_chunks
        .withColumn("prompt", concat(
            lit(GCS_SYSTEM),
            lit("\n\n<INPUT>\nThis is a CHUNK (pages "),
            col("chunk_start").cast("string"), lit("-"), col("chunk_end").cast("string"),
            lit(") of a larger package with "),
            col("total_pages").cast("string"), lit(" total pages.\n"),
            lit("Identify document boundaries ONLY within this page range.\n\n"),
            col("chunk_summary"),
            lit("\n</INPUT>\n\nOutput JSON:")
        ))
        .withColumn("token_bucket",
            when(col("chunk_end") - col("chunk_start") <= 30, "S")
            .when(col("chunk_end") - col("chunk_start") <= 60, "M")
            .otherwise("L"))
    )
    df_chunk_prompts.createOrReplaceTempView("chunk_prompts")
    
    # Run LLM on chunks (primary model)
    for bucket, max_tokens in [("S", MAX_TOKENS_S), ("M", MAX_TOKENS_M), ("L", MAX_TOKENS_L)]:
        spark.sql(f"""
            SELECT filename, total_pages, chunk_idx, chunk_start, chunk_end,
              ai_query('{PRIMARY_MODEL}', prompt,
                failOnError => false,
                modelParameters => named_struct('temperature', 0.1, 'max_tokens', {max_tokens})
              ) AS llm_result
            FROM chunk_prompts WHERE token_bucket = '{bucket}'
        """).createOrReplaceTempView(f"chunk_raw_{bucket}")
    
    df_chunk_raw = spark.sql("SELECT * FROM chunk_raw_S UNION ALL SELECT * FROM chunk_raw_M UNION ALL SELECT * FROM chunk_raw_L")
    
    df_chunk_parsed = (
        df_chunk_raw
        .withColumn("raw_response", col("llm_result.result"))
        .withColumn("error_message", col("llm_result.errorMessage"))
        .drop("llm_result")
        .withColumn("chunk_starts", parse_gcs_response(col("raw_response"), col("total_pages")))
    )
    
    # ── Materializzazione: risolve raw_response nel piano lazy prima della union ──
    df_chunk_parsed.write.format("delta").mode("overwrite").save(f"{TMP_BASE}/chunk_parsed")
    df_chunk_parsed = spark.read.format("delta").load(f"{TMP_BASE}/chunk_parsed")

    # Merge chunk boundaries per PDF
    from pyspark.sql.functions import explode, collect_set, sort_array, array_union, array as spark_array
    
    df_large_merged = (
        df_chunk_parsed
        .withColumn("boundary", explode("chunk_starts"))
        .filter((col("boundary") >= col("chunk_start")) & (col("boundary") <= col("chunk_end")))
        .groupBy("filename", "total_pages")
        .agg(sort_array(array_union(collect_set("boundary"), spark_array(lit(1)))).alias("parsed_starts"))
        .withColumn("token_bucket", lit("L"))
        .withColumn("boundary_source", lit("chunked"))
    )

    # Reconciliation: a large PDF whose chunk calls ALL failed would vanish from
    # df_large_merged (explode of NULL drops the rows) and stay 'parsed' forever.
    # Give it [1] + needs_review instead — no file may disappear.
    df_large_missing = (
        df_large_pdfs.select("filename", "total_pages")
        .join(df_large_merged.select("filename"), on="filename", how="left_anti")
        .withColumn("parsed_starts", spark_array(lit(1)))
        .withColumn("token_bucket", lit("L"))
        .withColumn("boundary_source", lit("ultimate_fallback"))
    )
    n_large_missing = df_large_missing.count()
    if n_large_missing > 0:
        for r in df_large_missing.select("filename").collect():
            events.log("needs_review", filename=r["filename"],
                       detail="all chunk LLM calls failed — [1] fallback, delivery blocked until approved")
        events.flush()
        print(f"  ⚠️  {n_large_missing} large PDFs lost all chunks → [1] + needs_review")

    # Combine with single-call results
    cols = ["filename", "total_pages", "parsed_starts", "token_bucket", "boundary_source"]
    df_all_with_large = (
        df_all_parsed.select(*cols)
        .unionByName(df_large_merged.select(*cols))
        .unionByName(df_large_missing.select(*cols))
    )
    df_all_with_large.write.format("delta").mode("overwrite").save(f"{TMP_BASE}/gcs_parsed_with_large")
    df_all_with_large = spark.read.format("delta").load(f"{TMP_BASE}/gcs_parsed_with_large")
    df_all_with_large.createOrReplaceTempView("gcs_parsed")
    df_all_parsed = df_all_with_large

    print(f"  ✓ Chunked merge complete: {df_large_merged.count()} large PDFs added")
    print(f"  Total GCS parsed: {df_all_parsed.count()} PDFs")

# COMMAND ----------

# DBTITLE 1,Boundary Verification Pass
# ── Stage 4: Boundary Verification (two-pass batched) ──
from pyspark.sql.functions import explode, concat_ws, sort_array, collect_list, collect_set

VERIFY_SYSTEM = """<SYSTEM_INSTRUCTIONS>
You are verifying document boundary predictions in a logistics PDF package.
For each PROPOSED BOUNDARY at page B, check:
1. Is B correct? (page B is indeed the first page of a new document)
2. Should it be B-1 or B+1 instead? (off-by-one error)
3. Should the boundary be removed? (false positive)

IMPORTANT: Do NOT add new boundaries. Only confirm, move, or remove.

Return ONLY a JSON array of corrections (empty [] if all correct):
[{"action": "confirm|move|remove", "from_page": N, "to_page": M}]
</SYSTEM_INSTRUCTIONS>"""

# Build verification context: window ±2 pages around each boundary
df_verify_batched = spark.sql("""
WITH boundaries AS (
    SELECT g.filename, boundary AS proposed_boundary, g.total_pages
    FROM gcs_parsed g
    LATERAL VIEW explode(g.parsed_starts) AS boundary
    WHERE boundary > 1
),
windows AS (
    SELECT b.filename, b.proposed_boundary, b.total_pages,
      concat(
        '\n=== PROPOSED BOUNDARY AT PAGE ', CAST(b.proposed_boundary AS STRING), ' ===\n',
        coalesce(max(CASE WHEN p.page_num = b.proposed_boundary - 2
            THEN concat('PAGE ', CAST(p.page_num AS STRING), ' (B-2):\n', p.page_block) END), ''),
        coalesce(max(CASE WHEN p.page_num = b.proposed_boundary - 1
            THEN concat('\nPAGE ', CAST(p.page_num AS STRING), ' (B-1):\n', p.page_block) END), ''),
        coalesce(max(CASE WHEN p.page_num = b.proposed_boundary
            THEN concat('\nPAGE ', CAST(p.page_num AS STRING), ' (B):\n', p.page_block) END), ''),
        coalesce(max(CASE WHEN p.page_num = b.proposed_boundary + 1
            THEN concat('\nPAGE ', CAST(p.page_num AS STRING), ' (B+1):\n', p.page_block) END), ''),
        coalesce(max(CASE WHEN p.page_num = b.proposed_boundary + 2
            THEN concat('\nPAGE ', CAST(p.page_num AS STRING), ' (B+2):\n', p.page_block) END), '')
      ) AS window_context
    FROM boundaries b
    JOIN page_signals p ON b.filename = p.filename
      AND p.page_num BETWEEN b.proposed_boundary - 2 AND b.proposed_boundary + 2
    GROUP BY b.filename, b.proposed_boundary, b.total_pages
),
batched AS (
    SELECT filename, total_pages,
      sort_array(collect_set(proposed_boundary)) AS all_proposed,
      concat_ws('\n', transform(
        sort_array(collect_list(named_struct('sort_key', proposed_boundary, 'text', window_context))),
        x -> x.text
      )) AS all_windows_context
    FROM windows
    GROUP BY filename, total_pages
)
SELECT * FROM batched
""")

df_verify_batched.createOrReplaceTempView("verify_batched")
n_to_verify = df_verify_batched.count()
print(f"Verification pass: {n_to_verify} PDFs with boundaries to verify")

# Build verification prompts and call LLM
df_verify_prompts = (
    df_verify_batched
    .withColumn("prompt", concat(
        lit(VERIFY_SYSTEM),
        lit("\n\n<INPUT>\nProposed boundaries: "),
        col("all_proposed").cast("string"),
        lit("\n\nPage details:\n"),
        col("all_windows_context"),
        lit("\n</INPUT>\n\nCorrections JSON array:")
    ))
)
df_verify_prompts.createOrReplaceTempView("verify_prompts")

df_verify_raw = spark.sql(f"""
    SELECT filename, total_pages, all_proposed,
      ai_query('{PRIMARY_MODEL}', prompt,
        failOnError => false,
        modelParameters => named_struct('temperature', 0.1, 'max_tokens', 1000)
      ) AS llm_result
    FROM verify_prompts
""")

df_verify_raw = (
    df_verify_raw
    .withColumn("verify_response", col("llm_result.result"))
    .withColumn("verify_error", col("llm_result.errorMessage"))
    .drop("llm_result")
)
# ── Materializzazione: fissa risposte verification prima di apply_corrections ──
df_verify_raw.write.format("delta").mode("overwrite").save(f"{TMP_BASE}/verify_raw")
df_verify_raw = spark.read.format("delta").load(f"{TMP_BASE}/verify_raw")
df_verify_raw.createOrReplaceTempView("verify_raw")

n_verify_errors = df_verify_raw.filter(col("verify_error").isNotNull()).count()
print(f"✓ Verification complete: {n_to_verify} PDFs, {n_verify_errors} LLM errors")

# ── Log verification responses to gcs_llm_responses ──
from pyspark.sql.types import ArrayType, IntegerType as _IntTypeLog
df_log_verify = df_verify_raw.select(
    col("filename"),
    lit("verification").alias("stage"),
    lit(PRIMARY_MODEL).alias("model_used"),
    lit("L").alias("prompt_token_bucket"),
    col("verify_response").alias("raw_response"),
    col("verify_error").alias("error_message"),
    lit(None).cast(ArrayType(_IntTypeLog())).alias("parsed_starts"),
    lit(False).alias("is_fallback"),
    current_timestamp().alias("processing_timestamp"),
    lit(RUN_ID).alias("run_id"),
    lit(DAY_ID).alias("day_id")
)
df_log_verify.write.format("delta").mode("append").saveAsTable(
    f"`{CATALOG}`.`{SCHEMA}`.`gcs_llm_responses`"
)
print(f"✓ Verification responses logged to gcs_llm_responses")

# COMMAND ----------

# DBTITLE 1,Apply corrections and build final boundaries
# ── Stage 5: Apply verification corrections → final boundaries ──
from pyspark.sql.functions import udf, col, size
from pyspark.sql.types import ArrayType, IntegerType
import re as _re_v, json as _json_v

@udf(returnType=ArrayType(IntegerType()))
def apply_corrections(proposed: list, verify_response: str, total_pages: int) -> list:
    """Apply verification corrections to proposed boundaries."""
    if not verify_response:
        return sorted(set([1] + list(proposed)))
    cleaned = _re_v.sub(r'```(?:json)?\s*', '', verify_response).strip()
    match = _re_v.search(r'\[.*\]', cleaned, _re_v.DOTALL)
    if not match:
        return sorted(set([1] + list(proposed)))
    try:
        corrections = _json_v.loads(match.group(0))
    except Exception:
        return sorted(set([1] + list(proposed)))
    
    result = set([1] + list(proposed))
    for c in corrections:
        action = c.get('action', '').lower()
        from_p = c.get('from_page')
        to_p = c.get('to_page')
        if action == 'move':
            if from_p and from_p in result and from_p != 1:
                result.discard(from_p)
            if to_p and 1 <= to_p <= total_pages:
                result.add(to_p)
        elif action == 'remove':
            if from_p and from_p in result and from_p != 1:
                result.discard(from_p)
        # 'confirm' and 'add' are no-ops
    return sorted(result)

# Apply corrections for verified PDFs
df_corrected = (
    df_verify_raw
    .withColumn("corrected_starts",
        apply_corrections(col("all_proposed"), col("verify_response"), col("total_pages")))
)
df_corrected.createOrReplaceTempView("corrections")

# Build final boundaries: verified + unverified (single-doc) + verify-failed
df_final = spark.sql("""
    -- Verified PDFs
    SELECT filename, corrected_starts AS predicted_starts
    FROM corrections WHERE verify_error IS NULL
    UNION ALL
    -- Verify-failed PDFs: keep original GCS boundaries
    SELECT c.filename, g.parsed_starts AS predicted_starts
    FROM corrections c
    JOIN gcs_parsed g ON c.filename = g.filename
    WHERE c.verify_error IS NOT NULL
    UNION ALL
    -- Single-doc PDFs (not sent to verification)
    SELECT g.filename, g.parsed_starts AS predicted_starts
    FROM gcs_parsed g
    LEFT ANTI JOIN corrections c ON g.filename = c.filename
""")

# ── Materializzazione: fissa final_boundaries su Delta prima di Cell 11 ──
df_final.write.format("delta").mode("overwrite").save(f"{TMP_BASE}/final_boundaries")
df_final = spark.read.format("delta").load(f"{TMP_BASE}/final_boundaries")
df_final.createOrReplaceTempView("final_boundaries")

stats = spark.sql("""
    SELECT count(*) AS n, avg(size(predicted_starts)) AS avg_docs, max(size(predicted_starts)) AS max_docs
    FROM final_boundaries
""").first()
print(f"✓ Final boundaries: {stats['n']} PDFs")
print(f"  Docs per PDF: avg={stats['avg_docs']:.1f}, max={stats['max_docs']}")

# COMMAND ----------

# DBTITLE 1,Write split_results and update processing_log
# ── Write final results to split_results UC table ──
from pyspark.sql.functions import col, lit, size, current_timestamp, when

# Determine model used per file
df_model_info = spark.sql(f"""
    SELECT filename,
        MAX(CASE WHEN is_fallback = true THEN true ELSE false END) AS fallback_used,
        MAX(CASE WHEN is_fallback = true THEN model_used ELSE NULL END) AS fallback_model,
        MAX(CASE WHEN is_fallback = false THEN model_used ELSE NULL END) AS primary_model
    FROM `{CATALOG}`.`{SCHEMA}`.`gcs_llm_responses`
    WHERE run_id = '{RUN_ID}' AND day_id = '{DAY_ID}'
    GROUP BY filename
""")

# boundary_source per file (primary / fallback / chunked / ultimate_fallback)
df_source = spark.sql("SELECT filename, boundary_source FROM gcs_parsed")

df_results = (
    spark.sql("SELECT filename, predicted_starts FROM final_boundaries")
    .join(
        spark.sql("SELECT filename, folder_id, total_pages FROM package_summaries"),
        on="filename"
    )
    .join(df_model_info, on="filename", how="left")
    .join(df_source, on="filename", how="left")
    .select(
        col("filename"),
        col("folder_id"),
        col("total_pages"),
        col("predicted_starts"),
        size(col("predicted_starts")).alias("n_documents"),
        when(col("fallback_used"), col("fallback_model"))
            .otherwise(col("primary_model")).alias("model_used"),
        col("fallback_used").alias("fallback_used"),
        lit(True).alias("verification_applied"),
        current_timestamp().alias("processing_timestamp"),
        lit(RUN_ID).alias("run_id"),
        lit(DAY_ID).alias("day_id"),
        (col("boundary_source") == "ultimate_fallback").alias("needs_review"),
        col("boundary_source"),
    )
)

# Materializza df_results prima di write+count per avere risultato coerente
df_results.write.format("delta").mode("overwrite").save(f"{TMP_BASE}/final_results")
df_results_mat = spark.read.format("delta").load(f"{TMP_BASE}/final_results")

n_results = df_results_mat.count()
df_results_mat.write.format("delta").mode("append").saveAsTable(
    f"`{CATALOG}`.`{SCHEMA}`.`split_results`"
)
print(f"✓ {n_results} entries written to split_results")

# Update processing_log: status = done (key: day_id + filename; run_id stamped)
spark.sql(f"""
    MERGE INTO {TABLE_LOG} AS log
    USING (
        SELECT filename, size(predicted_starts) AS n_documents
        FROM final_boundaries
    ) AS fb
    ON log.day_id = '{DAY_ID}' AND log.filename = fb.filename
    WHEN MATCHED AND log.status = 'parsed' THEN UPDATE SET
        log.status = 'done',
        log.n_documents_found = fb.n_documents,
        log.completed_at = current_timestamp(),
        log.run_id = '{RUN_ID}'
""")
events.log("status_change", old_status="parsed", new_status="done",
           detail=f"{n_results} files split")
events.flush()
print(f"✓ processing_log updated: status='done'")

# COMMAND ----------

# DBTITLE 1,Run summary and cleanup
# ── Run Summary & Cleanup ──
from datetime import datetime

RUN_END = datetime.now()
run_duration_min = (RUN_END - RUN_START).total_seconds() / 60

# Stats for this batch
stats_log = spark.sql(f"""
    SELECT
        SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errored
    FROM {TABLE_LOG}
    WHERE day_id = '{DAY_ID}'
""").first()

run_status = "success" if (stats_log['errored'] or 0) == 0 else "partial_failure"

# Write run_history
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
        {n_to_split},
        {stats_log['done'] or 0},
        0,
        {stats_log['errored'] or 0},
        '{PRIMARY_MODEL}',
        '{FALLBACK_MODEL}',
        {run_duration_min:.2f},
        'day_id={DAY_ID}'
    )
""")

events.log("run_completed", new_status=run_status,
           detail=f"split={n_results} done={stats_log['done'] or 0} errored={stats_log['errored'] or 0}")
events.flush()

# Cleanup temp Delta ONLY on success — on failure the materializations are
# needed by the RECOVERY cell to resume without re-invoking the LLM.
if run_status == "success":
    try:
        dbutils.fs.rm(TMP_BASE, recurse=True)
        print(f"Temp cleanup: {TMP_BASE} removed")
    except Exception as e:
        print(f"⚠️  Temp cleanup failed (non-fatal): {e}")
else:
    print(f"⚠️  run_status={run_status} — TMP_BASE kept for recovery: {TMP_BASE}")

print(f"\n{'═' * 60}")
print(f"  SPLIT RUN SUMMARY")
print(f"{'═' * 60}")
print(f"  Run ID:       {RUN_ID}")
print(f"  Status:       {run_status}")
print(f"  Duration:     {run_duration_min:.1f} min")
print(f"  PDFs split:   {stats_log['done'] or 0}")
print(f"  Errors:       {stats_log['errored'] or 0}")
print(f"  Primary LLM:  {PRIMARY_MODEL}")
print(f"  Fallback LLM: {FALLBACK_MODEL}")
print(f"  Fallbacks:    {n_failures if 'n_failures' in dir() else 0}")
print(f"{'═' * 60}")

dbutils.notebook.exit(f'{{"run_id": "{RUN_ID}", "day_id": "{DAY_ID}", "status": "{run_status}", "split": {stats_log["done"] or 0}}}')