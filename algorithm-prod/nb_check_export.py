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
# nb_check_export — Annotation sample + export ZIP (annotation gate入口)
# ══════════════════════════════════════════════════════════════════════
# Task 3 (final) of job_ingest (parse → split → check_export).
# 1. Samples sample_pct% of the batch's split files (seeded random —
#    a rerun picks the same sample) and copies the PDFs from
#    inbox/{day_id}/ to validation/{day_id}/ for the ground-truth app.
# 2. Builds validation/{day_id}/check_export_{day_id}.zip with each PDF +
#    its model prediction JSON, excluding files already annotated in
#    ground_truth/{day_id}/.
# 3. Emits the 'awaiting_annotation' batch event — the control tower
#    gate opens here and job_deliver stays locked until every sampled
#    file is annotated.
# ══════════════════════════════════════════════════════════════════════
import os, json, random
from datetime import datetime

TABLE_SPLIT = f"`{CATALOG}`.`{SCHEMA}`.`split_results`"

# ── Batch + run identity (job parameters) ──
DAY_ID = get_day_id()
RUN_ID = get_run_id()
PATHS = volume_paths(DAY_ID)
INBOX_PATH = PATHS["inbox"]
VALIDATION_PATH = PATHS["validation"]
GT_PATH = PATHS["ground_truth"]

# ── Annotation sample % (job parameter, user picks per batch in the UI) ──
dbutils.widgets.text("sample_pct", "10")
try:
    SAMPLE_PCT = float(dbutils.widgets.get("sample_pct"))
except ValueError:
    raise ValueError("Widget 'sample_pct' must be a number between 1 and 100")
if not (0 < SAMPLE_PCT <= 100):
    raise ValueError(f"sample_pct={SAMPLE_PCT} out of range (1-100)")

OUT_ZIP = f"{VALIDATION_PATH}/check_export_{DAY_ID}.zip"

events = EventLogger(RUN_ID, DAY_ID, stage="check_export")
events.log("run_started", detail=f"sample_pct={SAMPLE_PCT}")
events.flush()

print(f"Day ID:      {DAY_ID}")
print(f"Sample:      {SAMPLE_PCT}%")
print(f"Validation dir: {VALIDATION_PATH}/")
print(f"Output ZIP:  {OUT_ZIP}")

# COMMAND ----------

# DBTITLE 1,Sample files for annotation (seeded, idempotent)
# ── Files of this batch that have a split prediction ──
split_filenames = [
    r["filename"] for r in
    spark.sql(f"SELECT filename FROM {TABLE_SPLIT} WHERE day_id = '{DAY_ID}'").collect()
]

if not split_filenames:
    events.log("run_completed", new_status="NO_SPLIT_RESULTS")
    events.flush()
    print("⚠️  Nessun split_results per questo batch. Esegui prima nb_split_documents.")
    dbutils.notebook.exit("NO_SPLIT_RESULTS")

# Seeded by day_id → same sample on rerun (idempotent, reproducible)
rng = random.Random(f"check-sample-{DAY_ID}")
sample_size = max(1, round(len(split_filenames) * SAMPLE_PCT / 100))
check_sample = sorted(rng.sample(sorted(split_filenames),
                                 min(sample_size, len(split_filenames))))

print(f"Batch files with predictions: {len(split_filenames)}")
print(f"Annotation sample ({SAMPLE_PCT}%):  {len(check_sample)} file → validation/{DAY_ID}/")

os.makedirs(VALIDATION_PATH, exist_ok=True)
n_copied, n_already, n_copy_err = 0, 0, 0
for fname in check_sample:
    src = f"{INBOX_PATH}/{fname}.pdf"
    dst = f"{VALIDATION_PATH}/{fname}.pdf"
    if os.path.exists(dst):
        n_already += 1
        continue
    try:
        dbutils.fs.cp(src, dst)
        n_copied += 1
    except Exception as e:
        n_copy_err += 1
        events.log("error", filename=fname,
                   error_message=f"validation copy failed: {str(e)[:300]}")
        print(f"  ⚠️  Copy failed for {fname}: {e}")

events.flush()
print(f"✓ Copied {n_copied} (already present {n_already}, errors {n_copy_err})")

# COMMAND ----------

# DBTITLE 1,Load model predictions (temp-view join, no IN-clause)
all_pdf_files = [f for f in os.listdir(VALIDATION_PATH) if f.endswith(".pdf")]
if not all_pdf_files:
    events.log("run_completed", new_status="NO_FILES_IN_VALIDATION")
    events.flush()
    print("⚠️  Nessun PDF in validation/. Terminazione.")
    dbutils.notebook.exit("NO_FILES_IN_VALIDATION")

# ── Escludi file già annotati (ground truth presente in ground_truth/{day_id}/) ──
gt_annotated = set()
if os.path.exists(GT_PATH):
    gt_annotated = {os.path.splitext(f)[0] for f in os.listdir(GT_PATH) if f.endswith(".json")}

pdf_files    = [f for f in all_pdf_files if os.path.splitext(f)[0] not in gt_annotated]
already_done = [f for f in all_pdf_files if os.path.splitext(f)[0] in gt_annotated]
filenames    = [os.path.splitext(f)[0] for f in pdf_files]

print(f"PDF totali in validation/{DAY_ID}/:  {len(all_pdf_files)}")
print(f"  ✓ Già annotati (esclusi):  {len(already_done)}")
print(f"  → Da annotare (nel ZIP):   {len(pdf_files)}")

if not pdf_files:
    events.log("run_completed", new_status="ALL_ANNOTATED")
    events.flush()
    print("✓ Tutti i file sono già stati annotati. Nessun ZIP da creare.")
    dbutils.notebook.exit("ALL_ANNOTATED")

# ── Predizioni via temp-view join (filenames mai interpolati nella SQL) ──
spark.createDataFrame([(f,) for f in filenames], "filename STRING") \
    .createOrReplaceTempView("_check_filenames")
df_preds = spark.sql(f"""
    SELECT s.filename, s.folder_id, s.total_pages, s.n_documents,
           s.predicted_starts, s.model_used, s.fallback_used,
           s.verification_applied, s.processing_timestamp,
           COALESCE(s.needs_review, false) AS needs_review,
           s.boundary_source
    FROM {TABLE_SPLIT} s
    JOIN _check_filenames c ON s.filename = c.filename
    WHERE s.day_id = '{DAY_ID}'
""").collect()

preds_by_filename = {row["filename"]: row for row in df_preds}

n_with_pred = sum(1 for f in filenames if f in preds_by_filename)
n_no_pred   = len(filenames) - n_with_pred
print(f"  Con predizione in split_results: {n_with_pred}")
if n_no_pred:
    print(f"  ⚠️  Senza predizione (non ancora processati): {n_no_pred}")

# COMMAND ----------

# DBTITLE 1,Crea ZIP
import zipfile, shutil

TMP_ZIP = f"/tmp/check_export_{DAY_ID}.zip"

# Rimuovi zip precedente dello stesso batch se esiste
if os.path.exists(OUT_ZIP):
    os.remove(OUT_ZIP)
    print(f"ZIP precedente rimosso: {OUT_ZIP}")

n_pdf_added  = 0
n_json_added = 0
n_skipped    = 0

# Scrivi prima in /tmp (supporta seek), poi copia sul volume UC
with zipfile.ZipFile(TMP_ZIP, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for pdf_file in sorted(pdf_files):
        filename = os.path.splitext(pdf_file)[0]
        pdf_path = f"{VALIDATION_PATH}/{pdf_file}"

        if os.path.exists(pdf_path):
            zf.write(pdf_path, arcname=pdf_file)
            n_pdf_added += 1
        else:
            print(f"  ⚠️  PDF non trovato: {pdf_file}")
            n_skipped += 1
            continue

        row = preds_by_filename.get(filename)
        if row:
            pred_dict = {
                "filename":             filename,
                "folder_id":            row["folder_id"],
                "day_id":               DAY_ID,
                "total_pages":          row["total_pages"],
                "n_documents":          row["n_documents"],
                "predicted_starts":     list(row["predicted_starts"]),
                "model_used":           row["model_used"],
                "fallback_used":        row["fallback_used"],
                "verification_applied": row["verification_applied"],
                "needs_review":         row["needs_review"],
                "boundary_source":      row["boundary_source"],
                "processed_at":         str(row["processing_timestamp"])
            }
        else:
            pred_dict = {
                "filename":    filename,
                "day_id":      DAY_ID,
                "note":        "not_yet_processed",
                "predicted_starts": None
            }

        zf.writestr(f"{filename}_prediction.json",
                    json.dumps(pred_dict, indent=2, ensure_ascii=False))
        n_json_added += 1

# Copia da /tmp al volume UC
shutil.copy2(TMP_ZIP, OUT_ZIP)
os.remove(TMP_ZIP)

zip_size_mb = os.path.getsize(OUT_ZIP) / (1024 * 1024)

# ── The annotation gate opens here ──
events.log("awaiting_annotation",
           detail=f"n_sampled={len(all_pdf_files)} to_annotate={len(pdf_files)} "
                  f"already_annotated={len(already_done)} sample_pct={SAMPLE_PCT}")
events.log("run_completed", new_status="success",
           detail=f"zip={OUT_ZIP} pdf={n_pdf_added} json={n_json_added}")
events.flush()

print(f"\n{'═' * 55}")
print(f"  ZIP creato: {OUT_ZIP}")
print(f"  Dimensione: {zip_size_mb:.1f} MB")
print(f"  PDF aggiunti:  {n_pdf_added}")
print(f"  JSON aggiunti: {n_json_added}")
if n_skipped:
    print(f"  Skippati:      {n_skipped}")
print(f"{'=' * 55}")
print(f"\nAnnotation gate OPEN: annotate {len(pdf_files)} file in the Ground Truth app.")
print(f"job_deliver stays locked until every sampled file has a ground_truth JSON.")

dbutils.notebook.exit(
    f'{{"run_id": "{RUN_ID}", "day_id": "{DAY_ID}", "status": "awaiting_annotation", '
    f'"n_sampled": {len(all_pdf_files)}, "to_annotate": {len(pdf_files)}}}'
)
