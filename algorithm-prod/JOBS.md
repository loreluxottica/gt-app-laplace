# Databricks Jobs — multidocument pipeline

Two jobs replace the old single 4-task job. The human annotation gate sits between them:
`job_ingest` produces predictions + the annotation sample; the operator annotates in the
Ground Truth app; the control tower unlocks `job_deliver` when the gate is complete.

## job_ingest  (parse → split → check_export)

Tasks (each a notebook task, sequential dependency):
1. `nb_parse_documents`
2. `nb_split_documents`
3. `nb_check_export`

Job parameters (inherited by all tasks as widgets):
| name | value | note |
|------|-------|------|
| `run_id` | `{{job.run_id}}` | shared execution id — verify reference syntax in your Jobs UI version |
| `day_id` | (required, no default) | inbox/{day_id}/ batch — typed/selected in the control tower |
| `sample_pct` | `10` | annotation sample %, overridable per run from the control tower |

## job_deliver  (pdf_split → sftp_upload)

Tasks:
1. `nb_pdf_split`
2. `nb_sftp_upload`

Job parameters:
| name | value | note |
|------|-------|------|
| `run_id` | `{{job.run_id}}` | |
| `day_id` | (required, no default) | same batch as the ingest run |
| `sftp_remote_base` | (required, no default) | full remote path, typed by the user per delivery (e.g. `/Laplace/LAPLACE/US/20260711`). Preflight aborts if the path doesn't exist. Missing `{base}/{folder_id}/` subfolders → files marked `deferred`, re-deliverable later to another base. |

## Before first run (P0 — one-time)

1. Run `sql/ddl_pipeline_events.sql` (creates `pipeline_events`).
2. Run `sql/ddl_day_id_migration.sql` (adds `day_id` to the 7 tables + backfills `'legacy'`,
   adds `needs_review`/`boundary_source` to split_results, creates the `quarantine` volume).
3. Sync this folder to the workspace (`databricks workspace import-dir algorithm-prod /path/...`)
   — `nb_helpers.py` must sit next to the task notebooks (they `%run ./nb_helpers`).
4. Uploader convention: PDFs are dropped in `inbox/{day_id}/` (e.g. `inbox/20260707/`).
   The pipeline propagates day_id to manual/, quarantine/, check/, ground_truth/, archive/, output/.

## Notes

- The old `multidocument-job/` folder contains stale June copies — deprecated, do not edit.
- `nb_pipeline_status` is a read-only monitor; the control tower app supersedes it.
- Requeue semantics (used by the control tower) are documented in the repo plan and
  implemented in `pipeline-dashboard/src/actions.py`.
