-- P0 — pipeline_events: append-only audit trail, source of truth for the control tower.
-- Every state transition, error, quarantine, delivery and dashboard action lands here.
-- Run once on the sbx-logistics workspace (SQL warehouse or notebook %sql).

CREATE TABLE IF NOT EXISTS `sbx-logistics`.`multidocument-prod`.`pipeline_events` (
  event_id      STRING    NOT NULL,   -- uuid4
  run_id        STRING,               -- Databricks job run id ({{job.run_id}}), shared by all tasks
  day_id        STRING,               -- batch identity = inbox/{day_id}/ folder name
  stage         STRING    NOT NULL,   -- parse | split | check_export | pdf_split | sftp_upload | dashboard
  event_type    STRING    NOT NULL,   -- run_started | registered | status_change | error | quarantined
                                      -- | needs_review | blocked_needs_review | archived | delivered
                                      -- | deferred | verify_failed | folder_incomplete | awaiting_annotation
                                      -- | requeued | marked_manual | review_approved | run_completed
  filename      STRING,               -- NULL for run/batch-level events
  folder_id     STRING,
  old_status    STRING,
  new_status    STRING,
  detail        STRING,               -- free text / small JSON payload (counts, paths, ...)
  error_message STRING,
  actor         STRING    NOT NULL,   -- 'pipeline' or the user email for dashboard actions
  event_ts      TIMESTAMP NOT NULL
) USING DELTA
CLUSTER BY (day_id, filename);
