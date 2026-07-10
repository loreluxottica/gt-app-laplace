# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Config
# ══════════════════════════════════════════════════════════════════════
# nb_check_export — ZIP dei file in check/ + predizioni del modello
# ══════════════════════════════════════════════════════════════════════
# Per ogni PDF nel volume check/:
#   - aggiunge il PDF originale allo ZIP
#   - aggiunge un JSON con le predizioni del modello da split_results
#     (predicted_starts, n_documents, model_used, verification_applied)
# Output: /Volumes/.../check/check_export_YYYYMMDD.zip
# ══════════════════════════════════════════════════════════════════════
import os, json
from datetime import datetime

CATALOG = "sbx-logistics"
SCHEMA  = "multidocument-us"

CHECK_PATH   = f"/Volumes/{CATALOG}/{SCHEMA}/check"
TABLE_SPLIT  = f"`{CATALOG}`.`{SCHEMA}`.`split_results`"

RUN_DATE  = datetime.now().strftime("%Y%m%d")
OUT_ZIP   = f"{CHECK_PATH}/check_export_{RUN_DATE}.zip"

print(f"Source:  {CHECK_PATH}/")
print(f"Output:  {OUT_ZIP}")

# COMMAND ----------

# DBTITLE 1,Carica predizioni del modello
# ── Tutti i PDF in check/ ──
all_pdf_files = [f for f in os.listdir(CHECK_PATH) if f.endswith(".pdf")]
if not all_pdf_files:
    print("⚠️  Nessun PDF trovato in check/. Esegui prima nb_parse_documents.")
    dbutils.notebook.exit("NO_FILES_IN_CHECK")

# ── Escludi file già annotati (ground truth presente in ground_truth/) ──
GT_PATH     = f"/Volumes/{CATALOG}/{SCHEMA}/ground_truth"
gt_annotated = {os.path.splitext(f)[0] for f in os.listdir(GT_PATH) if f.endswith(".json")}

pdf_files       = [f for f in all_pdf_files if os.path.splitext(f)[0] not in gt_annotated]
already_done    = [f for f in all_pdf_files if os.path.splitext(f)[0] in gt_annotated]
filenames       = [os.path.splitext(f)[0] for f in pdf_files]

print(f"PDF totali in check/:        {len(all_pdf_files)}")
print(f"  ✓ Già annotati (esclusi):  {len(already_done)}")
print(f"  → Da annotare (nel ZIP):   {len(pdf_files)}")

if not pdf_files:
    print("✓ Tutti i file sono già stati annotati. Nessun ZIP da creare.")
    dbutils.notebook.exit("ALL_ANNOTATED")

# ── Recupera predizioni del modello per i file da annotare ──
df_preds = spark.sql(f"""
    SELECT
        filename,
        folder_id,
        total_pages,
        n_documents,
        predicted_starts,
        model_used,
        fallback_used,
        verification_applied,
        processing_timestamp
    FROM {TABLE_SPLIT}
    WHERE filename IN ({','.join(f"'{f}'" for f in filenames)})
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

TMP_ZIP = f"/tmp/check_export_{RUN_DATE}.zip"

# Rimuovi zip precedente dello stesso giorno se esiste
if os.path.exists(OUT_ZIP):
    os.remove(OUT_ZIP)
    print(f"ZIP precedente rimosso: {OUT_ZIP}")

n_pdf_added  = 0
n_json_added = 0
n_skipped    = 0

# Scrivi prima in /tmp (supporta seek), poi copia sul volume UC
with zipfile.ZipFile(TMP_ZIP, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for pdf_file in sorted(pdf_files):
        filename   = os.path.splitext(pdf_file)[0]
        pdf_path   = f"{CHECK_PATH}/{pdf_file}"

        # ── Aggiungi PDF ──
        if os.path.exists(pdf_path):
            zf.write(pdf_path, arcname=pdf_file)
            n_pdf_added += 1
        else:
            print(f"  ⚠️  PDF non trovato: {pdf_file}")
            n_skipped += 1
            continue

        # ── Aggiungi JSON con predizioni ──
        row = preds_by_filename.get(filename)
        if row:
            pred_dict = {
                "filename":             filename,
                "folder_id":            row["folder_id"],
                "total_pages":          row["total_pages"],
                "n_documents":          row["n_documents"],
                "predicted_starts":     list(row["predicted_starts"]),
                "model_used":           row["model_used"],
                "fallback_used":        row["fallback_used"],
                "verification_applied": row["verification_applied"],
                "processed_at":         str(row["processing_timestamp"])
            }
        else:
            pred_dict = {
                "filename":    filename,
                "note":        "not_yet_processed",
                "predicted_starts": None
            }

        json_name = f"{filename}_prediction.json"
        zf.writestr(json_name, json.dumps(pred_dict, indent=2, ensure_ascii=False))
        n_json_added += 1

# Copia da /tmp al volume UC
shutil.copy2(TMP_ZIP, OUT_ZIP)
os.remove(TMP_ZIP)

zip_size_mb = os.path.getsize(OUT_ZIP) / (1024 * 1024)

print(f"\n{'═' * 55}")
print(f"  ZIP creato: {OUT_ZIP}")
print(f"  Dimensione: {zip_size_mb:.1f} MB")
print(f"  PDF aggiunti:  {n_pdf_added}")
print(f"  JSON aggiunti: {n_json_added}")
if n_skipped:
    print(f"  Skippati:      {n_skipped}")
print(f"{'=' * 55}")
print(f"\nPuoi scaricarlo da: Catalog → Volumes → {CATALOG}/{SCHEMA}/check/")

# COMMAND ----------

# DBTITLE 1,Verifica contenuto ZIP
# ── Mostra i contenuti dello ZIP per verifica ──
with zipfile.ZipFile(OUT_ZIP, "r") as zf:
    names = sorted(zf.namelist())

pdfs  = [n for n in names if n.endswith(".pdf")]
jsons = [n for n in names if n.endswith(".json")]

print(f"Totale file nello ZIP: {len(names)} ({len(pdfs)} PDF + {len(jsons)} JSON)")
print(f"\nEsempio (primi 5):")
for name in names[:10]:
    info = zf.getinfo(name) if False else None  # solo listing
    print(f"  {name}")

# Mostra un esempio di JSON per verifica
if jsons:
    with zipfile.ZipFile(OUT_ZIP, "r") as zf:
        sample = json.loads(zf.read(jsons[0]))
    print(f"\nEsempio JSON ({jsons[0]}):")
    print(json.dumps(sample, indent=2, ensure_ascii=False)[:600])