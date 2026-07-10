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

# DBTITLE 1,Config
# ══════════════════════════════════════════════════════════════════════
# nb_pdf_split — Physical PDF Splitting
# ══════════════════════════════════════════════════════════════════════
# Reads split_results, physically splits PDFs with PyMuPDF.
# Output: /Volumes/.../output/YYYYMMDD/folder_id/filename_001.pdf
# Moves originals to archive/YYYY/MM/ after successful split.
# Task 3 of the multi-document splitting job.
# ══════════════════════════════════════════════════════════════════════

import fitz  # pymupdf
import os, json
import uuid
from datetime import datetime

# ── Schema & Paths ──
CATALOG = "sbx-logistics"
SCHEMA  = "multidocument-us"

INBOX_PATH   = f"/Volumes/{CATALOG}/{SCHEMA}/inbox"
ARCHIVE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/archive"
OUTPUT_PATH  = f"/Volumes/{CATALOG}/{SCHEMA}/output"
GT_PATH      = f"/Volumes/{CATALOG}/{SCHEMA}/ground_truth"

# ── Tables ──
TABLE_SPLIT   = f"`{CATALOG}`.`{SCHEMA}`.`split_results`"
TABLE_LOG     = f"`{CATALOG}`.`{SCHEMA}`.`processing_log`"
TABLE_HISTORY = f"`{CATALOG}`.`{SCHEMA}`.`run_history`"

# ── Run config ──
try:
    RUN_ID = dbutils.widgets.get("run_id")
except:
    RUN_ID = str(uuid.uuid4())

RUN_DATE  = datetime.now().strftime("%Y%m%d")
RUN_START = datetime.now()
OUTPUT_DATE_PATH = f"{OUTPUT_PATH}/{RUN_DATE}"

print(f"{'═' * 60}")
print(f"  nb_pdf_split — Production Run")
print(f"{'═' * 60}")
print(f"  Run ID:   {RUN_ID}")
print(f"  Date:     {RUN_DATE}")
print(f"  Output:   {OUTPUT_DATE_PATH}/{{folder_id}}/")
print(f"{'═' * 60}")

# COMMAND ----------

# DBTITLE 1,Load files to split
# ── Load split_results not yet physically split ──
# Un file è già stato splittato se sftp_delivery_status IS NOT NULL in processing_log
df_already_split = spark.sql(f"""
    SELECT filename FROM {TABLE_LOG}
    WHERE sftp_delivery_status IS NOT NULL
""")

df_to_split = (
    spark.sql(f"SELECT filename, folder_id, total_pages, predicted_starts, n_documents FROM {TABLE_SPLIT}")
    .join(df_already_split, on="filename", how="left_anti")
)

n_to_split = df_to_split.count()

if n_to_split == 0:
    print("ℹ️  Nessun file da splittare. Terminazione anticipata.")
    dbutils.notebook.exit("NO_FILES_TO_SPLIT")

print(f"File da splittare: {n_to_split}")
print(f"Documenti totali attesi: {df_to_split.agg({'n_documents': 'sum'}).first()[0]}")

split_tasks = df_to_split.collect()

# COMMAND ----------

# DBTITLE 1,Split PDFs with PyMuPDF
# ── Physical PDF split loop ──
import time

split_ok    = []
split_error = []
docs_created = 0

# Pre-carica ground truth disponibili
gt_by_filename = {}
if os.path.exists(GT_PATH):
    for _f in os.listdir(GT_PATH):
        if _f.endswith(".json"):
            _name = os.path.splitext(_f)[0]
            with open(f"{GT_PATH}/{_f}") as _fp:
                gt_by_filename[_name] = json.load(_fp)
n_gt = sum(1 for t in split_tasks if t["filename"] in gt_by_filename)
print(f"Ground truth disponibili: {n_gt}/{len(split_tasks)} file")

for row in split_tasks:
    filename         = row["filename"]
    folder_id        = row["folder_id"]
    predicted_starts = list(row["predicted_starts"])
    total_pages      = row["total_pages"]
    n_docs           = row["n_documents"]

    src_path      = f"{INBOX_PATH}/{filename}.pdf"
    output_folder = f"{OUTPUT_DATE_PATH}/{folder_id}"

    try:
        # Crea cartella output: output/YYYYMMDD/folder_id/
        os.makedirs(output_folder, exist_ok=True)

        # ── Scegli source: GT se disponibile, altrimenti predicted_starts ──
        if filename in gt_by_filename:
            gt = gt_by_filename[filename]
            segments     = [{"from": d["start"] - 1, "to": d["end"] - 1} for d in gt["documents"]]
            split_source = "ground_truth"
        else:
            segments = []
            for i, sp in enumerate(predicted_starts):
                ep = (predicted_starts[i + 1] - 2) if i + 1 < len(predicted_starts) else (total_pages - 1)
                segments.append({"from": sp - 1, "to": ep})
            split_source = "model"

        # Apri PDF e applica i segmenti
        doc = fitz.open(src_path)
        for i, seg in enumerate(segments):
            new_doc = fitz.open()
            new_doc.insert_pdf(doc, from_page=seg["from"], to_page=seg["to"])
            out_name = f"{filename}_{str(i + 1).zfill(3)}.pdf"
            out_path = f"{output_folder}/{out_name}"
            new_doc.save(out_path)
            new_doc.close()
            docs_created += 1
        doc.close()

        # Sposta originale in archive/YYYY/MM/
        archive_folder = f"{ARCHIVE_PATH}/{datetime.now():%Y/%m}"
        os.makedirs(archive_folder, exist_ok=True)
        archive_dest = f"{archive_folder}/{filename}.pdf"
        import shutil
        shutil.move(src_path, archive_dest)

        split_ok.append({
            "filename":          filename,
            "folder_id":         folder_id,
            "n_docs":            n_docs,
            "output_folder":     output_folder,
            "archive_path":      archive_dest,
            "split_source":      split_source
        })

    except Exception as e:
        split_error.append({"filename": filename, "error": str(e)[:500]})
        print(f"  ✗ {filename}: {str(e)[:120]}")

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

# DBTITLE 1,Update processing_log
# ── Aggiorna processing_log ──
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql import Row

for r in split_ok:
    fname        = r["filename"].replace("'", "''")
    out_folder   = r["output_folder"].replace("'", "''")
    archive_path = r["archive_path"].replace("'", "''")
    spark.sql(f"""
        UPDATE {TABLE_LOG}
        SET sftp_delivery_status = 'pending',
            sftp_target_folder   = '{out_folder}',
            archived_path        = '{archive_path}',
            completed_at         = current_timestamp()
        WHERE filename = '{fname}'
          AND status = 'done'
    """)

# File con errore
for e in split_error:
    fname = e["filename"].replace("'", "''")
    err   = e["error"][:400].replace("'", "''")
    spark.sql(f"""
        UPDATE {TABLE_LOG}
        SET status        = 'error',
            error_message = '{err}',
            error_stage   = 'pdf_split',
            completed_at  = current_timestamp()
        WHERE filename = '{fname}'
    """)

print(f"✓ processing_log aggiornato: {len(split_ok)} pending, {len(split_error)} error")

# COMMAND ----------

# DBTITLE 1,Run summary
# ── Run Summary ──
from datetime import datetime

RUN_END = datetime.now()
run_duration_min = (RUN_END - RUN_START).total_seconds() / 60

try:
    JOB_RUN_ID = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
except Exception:
    JOB_RUN_ID = None

run_status = "success" if len(split_error) == 0 else "partial_failure"
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
        'pdf_split task'
    )
""")

print(f"\n{'═' * 60}")
print(f"  PDF SPLIT SUMMARY")
print(f"{'═' * 60}")
print(f"  Run ID:      {RUN_ID}")
print(f"  Status:      {run_status}")
print(f"  Duration:    {run_duration_min:.1f} min")
print(f"  PDF splittati: {len(split_ok)} / {n_to_split}")
print(f"  Documenti creati: {docs_created}")
print(f"  Errori:      {len(split_error)}")
print(f"  Output:      {OUTPUT_DATE_PATH}/")
print(f"{'═' * 60}")

dbutils.notebook.exit(
    f'{{"run_id": "{RUN_ID}", "status": "{run_status}", '
    f'"split": {len(split_ok)}, "docs": {docs_created}, "date": "{RUN_DATE}"}}'
)