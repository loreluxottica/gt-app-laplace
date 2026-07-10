# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Install pymupdf
# MAGIC %pip install pymupdf --quiet

# COMMAND ----------

# DBTITLE 1,Restart Python kernel
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Load shared helpers
# MAGIC %run ./nb_helpers

# COMMAND ----------

# DBTITLE 1,Config
# ══════════════════════════════════════════════════════════════════════
# nb_pdf_split — Physical PDF Splitting
# ══════════════════════════════════════════════════════════════════════
# Task 1 of job_deliver (pdf_split → sftp_upload).
# Reads split_results for the day_id batch, physically splits PDFs
# with PyMuPDF. Ground-truth boundaries (from the annotation gate)
# override model predictions when present.
# Output:  /Volumes/.../output/{day_id}/{folder_id}/{filename}_001.pdf
# Archive: /Volumes/.../archive/{day_id}/
#
# Crash-safe 3-pass structure:
#   A) split all PDFs to output/            (per-file try/except)
#   B) batched MERGE → sftp pending + target folder   (log BEFORE any move)
#   C) archive originals (idempotent) + batched MERGE archived_path
# A crash between passes self-heals on rerun.
# needs_review files (LLM ultimate fallback) are BLOCKED unless a
# ground-truth JSON exists or the review was approved in the dashboard.
# ══════════════════════════════════════════════════════════════════════

import fitz  # pymupdf
import os, json, shutil
from datetime import datetime

TABLE_SPLIT   = f"`{CATALOG}`.`{SCHEMA}`.`split_results`"
TABLE_HISTORY = f"`{CATALOG}`.`{SCHEMA}`.`run_history`"

# ── Batch + run identity (job parameters) ──
DAY_ID = get_day_id()
RUN_ID = get_run_id()
PATHS = volume_paths(DAY_ID)

INBOX_PATH   = PATHS["inbox"]
ARCHIVE_PATH = PATHS["archive"]
OUTPUT_PATH  = PATHS["output"]
GT_PATH      = PATHS["ground_truth"]

RUN_START = datetime.now()
events = EventLogger(RUN_ID, DAY_ID, stage="pdf_split")
events.log("run_started")
events.flush()

print(f"{'═' * 60}")
print(f"  nb_pdf_split — Production Run")
print(f"{'═' * 60}")
print(f"  Run ID:   {RUN_ID}")
print(f"  Day ID:   {DAY_ID}")
print(f"  Output:   {OUTPUT_PATH}/{{folder_id}}/")
print(f"  Archive:  {ARCHIVE_PATH}/")
print(f"{'═' * 60}")

# COMMAND ----------

# DBTITLE 1,Load files to split
# ── Load split_results not yet physically split (key: day_id + filename) ──
# Un file è già stato splittato se sftp_delivery_status IS NOT NULL.
df_already_split = spark.sql(f"""
    SELECT filename FROM {TABLE_LOG}
    WHERE day_id = '{DAY_ID}' AND sftp_delivery_status IS NOT NULL
""")

df_candidates = (
    spark.sql(f"""
        SELECT filename, folder_id, total_pages, predicted_starts, n_documents,
               COALESCE(needs_review, false) AS needs_review
        FROM {TABLE_SPLIT}
        WHERE day_id = '{DAY_ID}'
    """)
    .join(df_already_split, on="filename", how="left_anti")
)

# Pre-load available ground truth for this batch (dated volume)
gt_by_filename = {}
if os.path.exists(GT_PATH):
    for _f in os.listdir(GT_PATH):
        if _f.endswith(".json"):
            _name = os.path.splitext(_f)[0]
            with open(f"{GT_PATH}/{_f}") as _fp:
                gt_by_filename[_name] = json.load(_fp)

# needs_review gate: blocked unless ground truth exists (GT overrides the model
# anyway) or the review was approved in the dashboard (needs_review=false).
candidates = df_candidates.collect()
split_tasks = []
blocked = []
for row in candidates:
    if row["needs_review"] and row["filename"] not in gt_by_filename:
        blocked.append(row["filename"])
    else:
        split_tasks.append(row)

for fname in blocked:
    events.log("blocked_needs_review", filename=fname,
               detail="needs_review=true and no ground-truth JSON — delivery blocked")
events.flush()
if blocked:
    print(f"⚠️  {len(blocked)} file blocked (needs_review, no GT): {blocked[:10]}")

n_to_split = len(split_tasks)
if n_to_split == 0:
    events.log("run_completed", new_status="NO_FILES_TO_SPLIT",
               detail=f"blocked={len(blocked)}")
    events.flush()
    print("ℹ️  Nessun file da splittare. Terminazione anticipata.")
    dbutils.notebook.exit("NO_FILES_TO_SPLIT")

n_gt = sum(1 for t in split_tasks if t["filename"] in gt_by_filename)
print(f"File da splittare: {n_to_split}")
print(f"Ground truth disponibili: {n_gt}/{n_to_split} file")

# COMMAND ----------

# DBTITLE 1,PASS A — Split PDFs with PyMuPDF (no moves, no log writes)
import time

split_ok    = []
split_error = []
docs_created = 0

for row in split_tasks:
    filename         = row["filename"]
    folder_id        = row["folder_id"]
    predicted_starts = list(row["predicted_starts"])
    total_pages      = row["total_pages"]

    src_path      = f"{INBOX_PATH}/{filename}.pdf"
    output_folder = f"{OUTPUT_PATH}/{folder_id}"
    archive_dest  = f"{ARCHIVE_PATH}/{filename}.pdf"

    try:
        # Rerun healing: if the original is gone but the archive copy exists,
        # a previous run crashed after PASS C's move — treat split as done.
        if not os.path.exists(src_path) and os.path.exists(archive_dest):
            split_ok.append({
                "filename": filename, "folder_id": folder_id,
                "output_folder": output_folder, "archive_dest": archive_dest,
                "split_source": "already_archived", "n_docs": 0,
            })
            continue

        doc = fitz.open(src_path)

        # ── Page-count truth: the physical PDF wins over the parse heuristic ──
        real_pages = doc.page_count
        if total_pages is None or total_pages != real_pages:
            events.log("status_change", filename=filename,
                       detail=f"page count reconciled: split_results={total_pages} pdf={real_pages}")
            total_pages = real_pages
        predicted_starts = [p for p in predicted_starts if 1 <= p <= total_pages]
        if not predicted_starts or predicted_starts[0] != 1:
            predicted_starts = sorted(set([1] + predicted_starts))

        # ── Scegli source: GT se disponibile, altrimenti predicted_starts ──
        if filename in gt_by_filename:
            gt = gt_by_filename[filename]
            segments     = [{"from": d["start"] - 1, "to": min(d["end"], total_pages) - 1}
                            for d in gt["documents"] if d["start"] <= total_pages]
            split_source = "ground_truth"
        else:
            segments = []
            for i, sp in enumerate(predicted_starts):
                ep = (predicted_starts[i + 1] - 2) if i + 1 < len(predicted_starts) else (total_pages - 1)
                segments.append({"from": sp - 1, "to": ep})
            split_source = "model"

        os.makedirs(output_folder, exist_ok=True)
        n_docs_file = 0
        for i, seg in enumerate(segments):
            new_doc = fitz.open()
            new_doc.insert_pdf(doc, from_page=seg["from"], to_page=seg["to"])
            out_name = f"{filename}_{str(i + 1).zfill(3)}.pdf"
            new_doc.save(f"{output_folder}/{out_name}")
            new_doc.close()
            n_docs_file += 1
        doc.close()
        docs_created += n_docs_file

        split_ok.append({
            "filename": filename, "folder_id": folder_id,
            "output_folder": output_folder, "archive_dest": archive_dest,
            "split_source": split_source, "n_docs": n_docs_file,
            "n_pages": total_pages,
        })

    except Exception as e:
        split_error.append({"filename": filename, "error": str(e)[:500]})
        events.log("error", filename=filename, error_message=str(e)[:500],
                   detail="pdf_split failed")
        print(f"  ✗ {filename}: {str(e)[:120]}")

events.flush()
n_gt_splits    = sum(1 for r in split_ok if r.get("split_source") == "ground_truth")
n_model_splits = sum(1 for r in split_ok if r.get("split_source") == "model")
print(f"\n✓ Split completato: {len(split_ok)} PDF → {docs_created} documenti")
print(f"  Con ground truth:       {n_gt_splits}")
print(f"  Con predizione modello: {n_model_splits}")
if split_error:
    print(f"⚠️  Errori: {len(split_error)}")
    for e in split_error:
        print(f"  ✗ {e['filename']}: {e['error'][:100]}")

# COMMAND ----------

# DBTITLE 1,PASS B — Mark delivery pending (log BEFORE any file move)
from datetime import datetime as _dt

now = _dt.now()
merge_processing_log(
    DAY_ID, RUN_ID,
    rows=[{
        "filename": r["filename"],
        "sftp_delivery_status": "pending",
        "sftp_target_folder": r["output_folder"],
        "completed_at": now,
    } for r in split_ok],
    set_cols=["sftp_delivery_status", "sftp_target_folder", "completed_at"],
)

# Reconciled page counts back into the log (physical PDF is the truth)
merge_processing_log(
    DAY_ID, RUN_ID,
    rows=[{"filename": r["filename"], "n_pages": r["n_pages"]}
          for r in split_ok if r.get("n_pages") is not None],
    set_cols=["n_pages"],
)

# Errors: batched, guarded on current status
merge_processing_log(
    DAY_ID, RUN_ID,
    rows=[{
        "filename": e["filename"],
        "status": "error",
        "error_message": e["error"][:400],
        "error_stage": "pdf_split",
        "completed_at": now,
    } for e in split_error],
    set_cols=["status", "error_message", "error_stage", "completed_at"],
    match_status=["done"],
)

print(f"✓ processing_log aggiornato: {len(split_ok)} pending, {len(split_error)} error")

# COMMAND ----------

# DBTITLE 1,PASS C — Archive originals (idempotent) + record archived_path
archived = []
archive_errors = []

os.makedirs(ARCHIVE_PATH, exist_ok=True)
for r in split_ok:
    src_path = f"{INBOX_PATH}/{r['filename']}.pdf"
    try:
        if os.path.exists(src_path):
            shutil.move(src_path, r["archive_dest"])
        elif not os.path.exists(r["archive_dest"]):
            raise FileNotFoundError(f"missing from both inbox and archive: {r['filename']}.pdf")
        archived.append(r)
        events.log("archived", filename=r["filename"], folder_id=r["folder_id"],
                   detail=r["archive_dest"])
    except Exception as e:
        archive_errors.append({"filename": r["filename"], "error": str(e)[:400]})
        events.log("error", filename=r["filename"],
                   error_message=f"archive move failed: {str(e)[:400]}")
        print(f"  ✗ archive {r['filename']}: {str(e)[:120]}")

merge_processing_log(
    DAY_ID, RUN_ID,
    rows=[{"filename": r["filename"], "archived_path": r["archive_dest"]}
          for r in archived],
    set_cols=["archived_path"],
)
events.flush()
print(f"✓ Archiviati: {len(archived)} file → {ARCHIVE_PATH}/ ({len(archive_errors)} errori)")

# COMMAND ----------

# DBTITLE 1,Run summary
from datetime import datetime

RUN_END = datetime.now()
run_duration_min = (RUN_END - RUN_START).total_seconds() / 60

try:
    JOB_RUN_ID = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
except Exception:
    JOB_RUN_ID = None

run_status = "success" if (len(split_error) + len(archive_errors)) == 0 else "partial_failure"
job_run_id_sql = f"'{JOB_RUN_ID}'" if JOB_RUN_ID else "NULL"

spark.sql(f"""
    INSERT INTO {TABLE_HISTORY} VALUES (
        '{RUN_ID}',
        {job_run_id_sql},
        TIMESTAMP '{RUN_START:%Y-%m-%d %H:%M:%S}',
        current_timestamp(),
        '{run_status}',
        {n_to_split},
        {len(split_ok)},
        0,
        {len(split_error)},
        'pymupdf',
        NULL,
        {run_duration_min:.2f},
        'pdf_split task day_id={DAY_ID}'
    )
""")

events.log("run_completed", new_status=run_status,
           detail=f"split={len(split_ok)} docs={docs_created} blocked={len(blocked)} "
                  f"errors={len(split_error)} archive_errors={len(archive_errors)}")
events.flush()

print(f"\n{'═' * 60}")
print(f"  PDF SPLIT SUMMARY")
print(f"{'═' * 60}")
print(f"  Run ID:      {RUN_ID}")
print(f"  Day ID:      {DAY_ID}")
print(f"  Status:      {run_status}")
print(f"  Duration:    {run_duration_min:.1f} min")
print(f"  PDF splittati: {len(split_ok)} / {n_to_split}")
print(f"  Documenti creati: {docs_created}")
print(f"  Blocked (needs_review): {len(blocked)}")
print(f"  Errori:      {len(split_error)} split, {len(archive_errors)} archive")
print(f"  Output:      {OUTPUT_PATH}/")
print(f"{'═' * 60}")

dbutils.notebook.exit(
    f'{{"run_id": "{RUN_ID}", "day_id": "{DAY_ID}", "status": "{run_status}", '
    f'"split": {len(split_ok)}, "docs": {docs_created}, "blocked": {len(blocked)}}}'
)
