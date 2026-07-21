-- P3 — Control-tower SQL views. One definition, shared by the Flask dashboard
-- and nb_pipeline_status. All views expose day_id (batch identity).
-- Run after ddl_prod_schema.sql + ddl_pipeline_events.sql + ddl_evaluation_results.sql.

USE CATALOG `sbx-logistics`;
USE SCHEMA `multidocument-prod`;

-- ─────────────────────────────────────────────────────────────────────────────
-- v_file_status — one row per (day_id, filename): furthest-progressed log entry
-- joined with split prediction facts. The dedup ranking mirrors the old
-- nb_pipeline_status logic (done > parsed > parsing > error > skipped > pending).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_file_status AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY day_id, filename
      ORDER BY CASE status
                 WHEN 'done'    THEN 1
                 WHEN 'parsed'  THEN 2
                 WHEN 'manual'  THEN 3
                 WHEN 'parsing' THEN 4
                 WHEN 'error'   THEN 5
                 WHEN 'skipped' THEN 6
                 WHEN 'pending' THEN 7
                 ELSE 8 END,
               created_at DESC
    ) AS rn
  FROM processing_log
)
SELECT
  l.day_id, l.filename, l.folder_id, l.file_size_mb, l.status,
  l.error_message, l.error_stage, l.n_pages, l.n_documents_found,
  l.retry_count, l.created_at, l.started_at, l.completed_at, l.run_id,
  l.archived_path, l.sftp_delivered_at, l.sftp_target_folder,
  l.sftp_delivery_status, l.sftp_delivery_error,
  s.needs_review, s.boundary_source, s.n_documents, s.model_used,
  s.predicted_starts
FROM ranked l
-- Dedup split_results to the latest row per (day_id, filename): a non-idempotent
-- rerun can leave >1 prediction row per file, and a raw join would fan v_funnel's
-- counts out (100 files → 200). srn=1 keeps the join strictly 1:1.
LEFT JOIN (
  SELECT * FROM (
    SELECT s.*,
      ROW_NUMBER() OVER (
        PARTITION BY day_id, filename
        ORDER BY processing_timestamp DESC
      ) AS srn
    FROM split_results s
  ) WHERE srn = 1
) s
  ON s.day_id = l.day_id AND s.filename = l.filename
WHERE l.rn = 1;

-- ─────────────────────────────────────────────────────────────────────────────
-- v_funnel — per-batch per-stage counts. Poll source for the live flow view.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_funnel AS
SELECT
  day_id,
  COUNT(*)                                                          AS n_files,
  SUM(CASE WHEN status = 'pending'  THEN 1 ELSE 0 END)              AS n_pending,
  SUM(CASE WHEN status = 'parsing'  THEN 1 ELSE 0 END)              AS n_parsing,
  SUM(CASE WHEN status = 'parsed'   THEN 1 ELSE 0 END)              AS n_parsed,
  SUM(CASE WHEN status = 'done'     THEN 1 ELSE 0 END)              AS n_predicted,
  SUM(CASE WHEN status = 'error'    THEN 1 ELSE 0 END)              AS n_error,
  SUM(CASE WHEN status = 'skipped'  THEN 1 ELSE 0 END)              AS n_skipped,
  SUM(CASE WHEN status = 'manual'   THEN 1 ELSE 0 END)              AS n_manual,
  SUM(CASE WHEN sftp_delivery_status = 'pending'   THEN 1 ELSE 0 END) AS n_sftp_pending,
  SUM(CASE WHEN sftp_delivery_status = 'delivered' THEN 1 ELSE 0 END) AS n_delivered,
  SUM(CASE WHEN sftp_delivery_status = 'failed'    THEN 1 ELSE 0 END) AS n_sftp_failed,
  SUM(CASE WHEN sftp_delivery_status = 'deferred'  THEN 1 ELSE 0 END) AS n_deferred,
  SUM(CASE WHEN needs_review THEN 1 ELSE 0 END)                     AS n_needs_review
FROM v_file_status
GROUP BY day_id;

-- ─────────────────────────────────────────────────────────────────────────────
-- v_batch_status — coarse lifecycle per day_id. The app refines
-- 'awaiting_annotation' with the gate counts (volume listings).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_batch_status AS
SELECT
  f.*,
  g.last_event_ts,
  g.gate_opened,
  CASE
    WHEN f.n_delivered > 0
         AND f.n_delivered >= (f.n_files - f.n_skipped - f.n_manual - f.n_error)
      THEN 'delivered'
    WHEN f.n_sftp_pending + f.n_sftp_failed + f.n_deferred > 0
      THEN 'delivering'
    WHEN g.gate_opened AND f.n_delivered = 0
      THEN 'awaiting_annotation'
    WHEN f.n_predicted > 0 THEN 'predicted'
    WHEN f.n_parsing > 0 OR f.n_parsed > 0 THEN 'parsing'
    WHEN f.n_pending > 0 THEN 'uploaded'
    ELSE 'unknown'
  END AS lifecycle,
  (f.n_error + f.n_sftp_failed) > 0 AS has_errors
FROM v_funnel f
LEFT JOIN (
  SELECT day_id,
         MAX(event_ts) AS last_event_ts,
         MAX(CASE WHEN event_type = 'awaiting_annotation' THEN true ELSE false END) AS gate_opened
  FROM pipeline_events
  GROUP BY day_id
) g ON g.day_id = f.day_id;

-- ─────────────────────────────────────────────────────────────────────────────
-- v_run_summary — per run/stage: window + event counts (durations for the UI).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_run_summary AS
SELECT
  run_id, day_id, stage,
  MIN(event_ts) AS started_at,
  MAX(event_ts) AS last_event_at,
  CAST((UNIX_TIMESTAMP(MAX(event_ts)) - UNIX_TIMESTAMP(MIN(event_ts))) / 60.0 AS DECIMAL(10,1))
    AS duration_min,
  SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END)         AS n_errors,
  SUM(CASE WHEN event_type = 'needs_review' THEN 1 ELSE 0 END)  AS n_needs_review,
  SUM(CASE WHEN event_type = 'delivered' THEN 1 ELSE 0 END)     AS n_delivered,
  SUM(CASE WHEN event_type = 'deferred' THEN 1 ELSE 0 END)      AS n_deferred,
  SUM(CASE WHEN event_type = 'quarantined' THEN 1 ELSE 0 END)   AS n_quarantined,
  MAX(CASE WHEN event_type = 'run_completed' THEN new_status END) AS run_outcome
FROM pipeline_events
GROUP BY run_id, day_id, stage;

-- ─────────────────────────────────────────────────────────────────────────────
-- v_stuck_files — every file that needs attention, with WHY.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_stuck_files AS
SELECT *,
  CASE
    WHEN status = 'error'
      THEN CONCAT('error at ', COALESCE(error_stage, '?'), ': ', COALESCE(error_message, ''))
    WHEN sftp_delivery_status = 'failed'
      THEN CONCAT('sftp failed: ', COALESCE(sftp_delivery_error, ''))
    WHEN sftp_delivery_status = 'deferred'
      THEN CONCAT('deferred: ', COALESCE(sftp_delivery_error, 'remote folder missing'))
    WHEN status = 'parsing' AND started_at < current_timestamp() - INTERVAL 2 HOURS
      THEN 'stuck in parsing > 2h'
    WHEN status = 'parsed' AND completed_at < current_timestamp() - INTERVAL 12 HOURS
      THEN 'parsed but never split (> 12h)'
    WHEN sftp_delivery_status = 'pending'
         AND completed_at < current_timestamp() - INTERVAL 24 HOURS
      THEN 'awaiting sftp > 24h'
    WHEN needs_review AND sftp_delivery_status IS NULL
      THEN CONCAT('needs review (', COALESCE(boundary_source, '?'), ') — delivery blocked')
    WHEN sftp_delivery_status = 'pending' AND archived_path IS NULL
         AND completed_at < current_timestamp() - INTERVAL 2 HOURS
      THEN 'split but not archived (crash between passes?)'
  END AS stuck_reason
FROM v_file_status
WHERE
     status = 'error'
  OR sftp_delivery_status IN ('failed', 'deferred')
  OR (status = 'parsing' AND started_at < current_timestamp() - INTERVAL 2 HOURS)
  OR (status = 'parsed' AND completed_at < current_timestamp() - INTERVAL 12 HOURS)
  OR (sftp_delivery_status = 'pending' AND completed_at < current_timestamp() - INTERVAL 24 HOURS)
  OR (needs_review AND sftp_delivery_status IS NULL);

-- ─────────────────────────────────────────────────────────────────────────────
-- v_sftp_board — delivery completeness per (day_id, folder_id).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_sftp_board AS
-- Counts are in PHYSICAL PDF units, not packages: each package (one row here)
-- produces n_documents split PDFs under {base}/{folder_id}/, and every PDF of a
-- package shares that package's delivery status. Weighting each package by
-- n_documents makes n_files = split PDFs in the folder and keeps the status
-- columns reconciling (delivered + pending + failed + deferred = n_files).
SELECT
  day_id, folder_id,
  SUM(COALESCE(n_documents, 0)) AS n_files,
  SUM(CASE WHEN sftp_delivery_status = 'delivered' THEN COALESCE(n_documents, 0) ELSE 0 END) AS n_delivered,
  SUM(CASE WHEN sftp_delivery_status = 'pending'   THEN COALESCE(n_documents, 0) ELSE 0 END) AS n_pending,
  SUM(CASE WHEN sftp_delivery_status = 'failed'    THEN COALESCE(n_documents, 0) ELSE 0 END) AS n_failed,
  SUM(CASE WHEN sftp_delivery_status = 'deferred'  THEN COALESCE(n_documents, 0) ELSE 0 END) AS n_deferred,
  MAX(sftp_delivered_at) AS last_delivered_at,
  MAX(sftp_target_folder) AS sftp_target_folder
FROM v_file_status
WHERE sftp_delivery_status IS NOT NULL
GROUP BY day_id, folder_id;

-- ─────────────────────────────────────────────────────────────────────────────
-- v_needs_review — [1]-fallback queue for the review page.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_needs_review AS
SELECT day_id, filename, folder_id, total_pages, predicted_starts, n_documents,
       model_used, boundary_source, processing_timestamp
FROM split_results
WHERE needs_review = true;

-- ─────────────────────────────────────────────────────────────────────────────
-- v_events_recent — activity feed (newest first, capped by the caller's LIMIT).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_events_recent AS
SELECT event_ts, day_id, run_id, stage, event_type, filename, folder_id,
       old_status, new_status, detail, error_message, actor
FROM pipeline_events
ORDER BY event_ts DESC;
