-- Greenfield DDL for `sbx-logistics`.`multidocument-prod` — the 7 pipeline tables.
-- Run ONCE, before anything else, then ddl_pipeline_events.sql,
-- ddl_evaluation_results.sql and views.sql.
--
-- These tables were never version-controlled: multidocument-us was built by hand
-- and its shape only ever existed in the notebooks' write paths. Every column
-- below is reconstructed from the code that writes it (file:line in comments).
--
-- day_id / needs_review / boundary_source are NATIVE here. On the retired -us
-- schema they were bolted on by a migration; this schema is born with them.
--
-- ⚠ COLUMN ORDER IS LOAD-BEARING. Every writer is either a positional
--   .saveAsTable(mode="append") or an INSERT INTO ... VALUES with no column
--   list. Reordering a column here silently corrupts the writes.

USE CATALOG `sbx-logistics`;
USE SCHEMA `multidocument-prod`;

-- ─────────────────────────────────────────────────────────────────────────────
-- processing_log — one row per (day_id, filename); the state machine.
-- status:               pending → parsing → parsed → done
--                       + skipped (oversized) | error | manual (terminal)
-- sftp_delivery_status: NULL → pending → delivered | failed | deferred
-- Writer: nb_parse_documents.py:138-170 (append). Updated only via
-- nb_helpers.merge_processing_log (temp-view MERGE) and actions.py.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS processing_log (
  filename             STRING,       -- nb_parse_documents.py:141
  folder_id            STRING,       -- :142
  file_size_mb         DOUBLE,       -- :143 — rounded to 2 decimals
  status               STRING,       -- :144
  error_message        STRING,       -- :146
  error_stage          STRING,       -- :148
  model_used           STRING,       -- :149
  n_pages              INT,          -- :150 — reconciled vs the physical PDF in nb_pdf_split
  n_documents_found    INT,          -- :151 — same fact as split_results.n_documents
  retry_count          INT,          -- :152 — always written 0; actions.py:50 increments
  created_at           TIMESTAMP,    -- :153
  started_at           TIMESTAMP,    -- :154
  completed_at         TIMESTAMP,    -- :155
  run_id               STRING,       -- :156
  archived_path        STRING,       -- :157 — set by nb_pdf_split pass C
  sftp_delivered_at    TIMESTAMP,    -- :158
  sftp_target_folder   STRING,       -- :159
  sftp_delivery_status STRING,       -- :160
  sftp_delivery_error  STRING,       -- :161
  day_id               STRING        -- :162
) USING DELTA
CLUSTER BY (day_id, filename)
COMMENT 'Per-file pipeline state. Composite key (day_id, filename).';

-- ─────────────────────────────────────────────────────────────────────────────
-- parsed_documents — ai_parse_document output. Writer: nb_parse_documents.py:242-249.
-- parsed_content is the VARIANT stringified on write (:244) and re-parsed with
-- parse_json on read (nb_split_documents.py:196) — keep it STRING.
-- n_pages is deliberately NULL (never 1) when the parse yields no elements.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS parsed_documents (
  filename        STRING,       -- nb_parse_documents.py:243
  folder_id       STRING,       -- :243
  source_path     STRING,       -- :243
  file_size_mb    DOUBLE,       -- :233 — NOT rounded here, unlike processing_log
  n_pages         INT,          -- :240 — NULL when the parse has no elements
  parsed_content  STRING,       -- :244 — CAST(VARIANT AS STRING)
  parsed_at       TIMESTAMP,    -- :245
  run_id          STRING,       -- :245
  day_id          STRING        -- :245
) USING DELTA
CLUSTER BY (day_id, filename)
COMMENT 'ai_parse_document raw output, one row per parsed PDF.';

-- ─────────────────────────────────────────────────────────────────────────────
-- split_results — the LLM boundary prediction. Writer: nb_split_documents.py:992-1017.
-- needs_review=true (boundary_source='ultimate_fallback') BLOCKS delivery until
-- approved in the dashboard or a ground-truth JSON exists.
-- Left nullable on purpose: nb_check_export.py:139 and nb_pdf_split.py:85 both
-- read it through COALESCE(needs_review, false).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS split_results (
  filename              STRING,       -- nb_split_documents.py:993
  folder_id             STRING,       -- :994
  total_pages           INT,          -- :995 — from package_summaries (MAX(page_num))
  predicted_starts      ARRAY<INT>,   -- :996 — UDF returns ArrayType(IntegerType())
  n_documents           INT,          -- :997
  model_used            STRING,       -- :998
  fallback_used         BOOLEAN,      -- :1000
  verification_applied  BOOLEAN,      -- :1001
  processing_timestamp  TIMESTAMP,    -- :1002
  run_id                STRING,       -- :1003
  day_id                STRING,       -- :1004
  needs_review          BOOLEAN,      -- :1005
  boundary_source       STRING        -- :1006 — primary|fallback|chunked|ultimate_fallback
) USING DELTA
CLUSTER BY (day_id, filename)
COMMENT 'Predicted document boundaries per PDF, with provenance and review flag.';

-- ─────────────────────────────────────────────────────────────────────────────
-- page_signals — per-page features fed to the LLM prompt.
-- Writer: nb_split_documents.py:222-269 — DELETE for this day_id, then append.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS page_signals (
  filename           STRING,    -- nb_split_documents.py:223
  page_num           INT,       -- :224 — 1-based
  total_pages        INT,       -- :225
  first_element      STRING,    -- :226
  header             STRING,    -- :228 — LEFT(...,200)
  doc_ref            STRING,    -- :230
  page_indicator     STRING,    -- :232
  form_number        STRING,    -- :234
  invoice_number     STRING,    -- :236
  footer_pagination  STRING,    -- :238
  text_start         STRING,    -- :240 — LEFT(...,300)
  text_end           STRING,    -- :241 — RIGHT(...,300)
  page_block         STRING,    -- :243 — the assembled prompt block
  run_id             STRING,    -- :254
  day_id             STRING     -- :255
) USING DELTA
CLUSTER BY (day_id, filename)
COMMENT 'Per-page boundary signals extracted from parsed_content.';

-- ─────────────────────────────────────────────────────────────────────────────
-- package_summaries — one prompt-sized summary per PDF.
-- Writer: nb_split_documents.py:316-324 — DELETE for this day_id, then append.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS package_summaries (
  filename         STRING,    -- nb_split_documents.py:321
  folder_id        STRING,    -- :321
  total_pages      INT,       -- :321
  package_summary  STRING,    -- :321 — concat_ws of the page blocks
  run_id           STRING,    -- :311
  day_id           STRING     -- :312
) USING DELTA
CLUSTER BY (day_id, filename)
COMMENT 'Concatenated page blocks per PDF — the LLM prompt body.';

-- ─────────────────────────────────────────────────────────────────────────────
-- gcs_llm_responses — every LLM call, verbatim. Three writers, identical shape:
-- nb_split_documents.py:482-497 (primary), :560-575 (fallback), :870-885 (verify).
-- Same ARRAY<INT> as split_results.predicted_starts, named parsed_starts here.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gcs_llm_responses (
  filename              STRING,       -- nb_split_documents.py:483
  stage                 STRING,       -- :484 — primary_pass|retry_fallback|verification
  model_used            STRING,       -- :485
  prompt_token_bucket   STRING,       -- :486 — S|M|L
  raw_response          STRING,       -- :487
  error_message         STRING,       -- :488
  parsed_starts         ARRAY<INT>,   -- :489 — cast pinned at :877
  is_fallback           BOOLEAN,      -- :490
  processing_timestamp  TIMESTAMP,    -- :491
  run_id                STRING,       -- :492
  day_id                STRING        -- :493
) USING DELTA
CLUSTER BY (day_id, filename)
COMMENT 'Raw LLM responses for audit and offline prompt evaluation.';

-- ─────────────────────────────────────────────────────────────────────────────
-- run_history — one row per notebook task run.
-- ⚠ Written by THREE positional INSERT INTO ... VALUES with no column list:
--   nb_parse_documents.py:520-536, nb_split_documents.py:1067-1083,
--   nb_pdf_split.py:311-327. All three send exactly 13 values in this order.
--   Adding, removing or reordering a column here breaks all three call sites.
-- No day_id column: the batch is smuggled into notes as 'day_id={DAY_ID}'.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS run_history (
  run_id          STRING,       -- 1  nb_parse_documents.py:522
  job_run_id      STRING,       -- 2  :523 — {{job.run_id}}, NULL on manual runs
  started_at      TIMESTAMP,    -- 3  :524
  completed_at    TIMESTAMP,    -- 4  :525
  status          STRING,       -- 5  :526 — success|partial_failure|failed
  n_total         BIGINT,       -- 6  :527 — COUNT(*) is BIGINT
  n_success       BIGINT,       -- 7  :528
  n_skipped       BIGINT,       -- 8  :529
  n_error         BIGINT,       -- 9  :530
  primary_model   STRING,       -- 10 :531
  fallback_model  STRING,       -- 11 :532
  duration_min    DOUBLE,       -- 12 :533
  notes           STRING        -- 13 :534 — carries 'day_id={DAY_ID}'
) USING DELTA
COMMENT 'Per-task run outcomes. Column ORDER is a contract with 3 positional INSERTs.';
