# Multidocument Control Tower

Local Flask app (same architecture as the Ground Truth app) that drives the
multidocument pipeline end to end:

1. **Batches** — every `inbox/{day_id}/` folder with its lifecycle badge and
   contextual actions.
2. **Run ingest** — pick the annotation sample % → launches `job_ingest`
   (parse → split → check_export).
3. **Live Flow** — animated funnel (inbox → parsed → predicted → ground truth →
   split → delivered), polls every 4s while a job runs; errors show as red
   counters on the failing stage.
4. **Annotation Gate** — sample annotation progress, link to the GT app,
   model-vs-GT metrics when complete, then unlocks delivery.
5. **Split & upload** — type the SFTP remote path for this delivery →
   launches `job_deliver` (pdf_split → sftp_upload). Server refuses if the
   gate is incomplete.
6. **Errors / SFTP / Review** — every failure documented (pipeline_events),
   requeue buttons, deferred re-delivery to a different folder, unsplit
   approval queue.

## Run locally

```bash
cd pipeline-dashboard
pip install -r requirements.txt

# Databricks auth: DEFAULT profile in ~/.databrickscfg, or:
#   set DATABRICKS_HOST=https://adb-....azuredatabricks.net
#   set DATABRICKS_TOKEN=dapi...

set DATABRICKS_WAREHOUSE_ID=<sql-warehouse-id>
set JOB_INGEST_ID=<job id of job_ingest>
set JOB_DELIVER_ID=<job id of job_deliver>
set GT_APP_URL=http://localhost:8000        # where the GT app runs
set DASHBOARD_ACTOR=lorenzo.muscillo@luxottica.com

python app.py       # → http://localhost:8001
```

Prerequisites on the workspace (one-time, see ../algorithm-prod/JOBS.md):
`sql/ddl_prod_schema.sql`, `sql/ddl_pipeline_events.sql`,
`sql/ddl_evaluation_results.sql`, `sql/views.sql` — all already executed on
`multidocument-prod` — and the two jobs created from the notebooks.
