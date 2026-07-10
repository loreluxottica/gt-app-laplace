# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Config
CATALOG        = "sbx-logistics"
SCHEMA         = "multidocument-us"
TOTAL_EXPECTED = 2438  # file originali in inbox

# COMMAND ----------

# DBTITLE 1,Stato pipeline per file distinti
# Per ogni filename prende l'entry più avanzata nel pipeline
# (status_rank: done > parsed > parsing > error > skipped > pending)
df_status = spark.sql(f"""
    WITH ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY filename
                ORDER BY
                    CASE status
                        WHEN 'done'    THEN 1
                        WHEN 'parsed'  THEN 2
                        WHEN 'parsing' THEN 3
                        WHEN 'error'   THEN 4
                        WHEN 'skipped' THEN 5
                        ELSE 6
                    END
            ) AS rn
        FROM `{CATALOG}`.`{SCHEMA}`.`processing_log`
    )
    SELECT
        status,
        sftp_delivery_status,
        COUNT(*) AS n_files
    FROM ranked
    WHERE rn = 1
    GROUP BY status, sftp_delivery_status
    ORDER BY
        CASE status WHEN 'done' THEN 1 WHEN 'parsed' THEN 2
                    WHEN 'parsing' THEN 3 WHEN 'error' THEN 4
                    WHEN 'skipped' THEN 5 ELSE 6 END,
        sftp_delivery_status NULLS LAST
""")
display(df_status)

# COMMAND ----------

# DBTITLE 1,Riepilogo avanzamento
rows = df_status.collect()

# Aggrega
totals = {}
for r in rows:
    key = (r["status"], r["sftp_delivery_status"])
    totals[key] = r["n_files"]

n_distinct = spark.sql(f"""
    SELECT COUNT(DISTINCT filename) AS n
    FROM `{CATALOG}`.`{SCHEMA}`.`processing_log`
""").first()["n"]

n_delivered = sum(v for (s, sftp), v in totals.items() if sftp == "delivered")
n_pending   = sum(v for (s, sftp), v in totals.items() if sftp == "pending")
n_failed    = sum(v for (s, sftp), v in totals.items() if sftp == "failed")
n_done_no_sftp = sum(v for (s, sftp), v in totals.items() if s == "done" and sftp is None)
n_parsed    = sum(v for (s, _), v in totals.items() if s == "parsed")
n_parsing   = sum(v for (s, _), v in totals.items() if s == "parsing")
n_error     = sum(v for (s, _), v in totals.items() if s == "error")
n_skipped   = sum(v for (s, _), v in totals.items() if s == "skipped")
n_not_started = TOTAL_EXPECTED - n_distinct

def bar(n, total, width=30):
    filled = int(round(n / total * width)) if total > 0 else 0
    return f"[{'█' * filled}{'░' * (width - filled)}] {n:>5} / {total}"

print(f"\n{'═'*62}")
print(f"  STATO PIPELINE — {TOTAL_EXPECTED} file originali")
print(f"{'═'*62}")
print(f"  Nel log (distinti):  {n_distinct:>5}")
print(f"  Non ancora iniziati: {n_not_started:>5}")
print(f"{'─'*62}")
print(f"  PARSING")
print(f"    In corso (parsing):  {n_parsing:>5}")
print(f"    Parsati:             {n_parsed:>5}")
print(f"  SPLIT + SFTP")
print(f"    Splittati, pending:  {n_pending:>5}")
print(f"    Consegnati su SFTP:  {n_delivered:>5}")
print(f"    Falliti SFTP:        {n_failed:>5}")
print(f"  ALTRI")
print(f"    Errori:              {n_error:>5}")
print(f"    Skippati (>100MB):   {n_skipped:>5}")
print(f"{'─'*62}")
print(f"  Completamento SFTP:")
print(f"  {bar(n_delivered, TOTAL_EXPECTED)}  ({n_delivered/TOTAL_EXPECTED*100:.1f}%)")
print(f"{'═'*62}")

# COMMAND ----------

# DBTITLE 1,File da riprocessare
# MAGIC %sql
# MAGIC -- File da riprocessare: parsing stuck, parsed (non splittati), errori, SFTP falliti
# MAGIC WITH ranked AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY filename
# MAGIC             ORDER BY
# MAGIC                 CASE status
# MAGIC                     WHEN 'done'    THEN 1 WHEN 'parsed'  THEN 2
# MAGIC                     WHEN 'parsing' THEN 3 WHEN 'error'   THEN 4
# MAGIC                     WHEN 'skipped' THEN 5 ELSE 6
# MAGIC                 END
# MAGIC         ) AS rn
# MAGIC     FROM `sbx-logistics`.`multidocument-us`.`processing_log`
# MAGIC )
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN status = 'parsing'                          THEN '1_parsing_stuck'
# MAGIC         WHEN status = 'parsed'                           THEN '2_parsed_not_split'
# MAGIC         WHEN status = 'done' AND sftp_delivery_status = 'failed' THEN '3_sftp_failed'
# MAGIC         WHEN status = 'error'                            THEN '4_error'
# MAGIC     END                          AS categoria,
# MAGIC     filename,
# MAGIC     folder_id,
# MAGIC     status,
# MAGIC     sftp_delivery_status,
# MAGIC     error_stage,
# MAGIC     COALESCE(sftp_delivery_error, error_message) AS dettaglio_errore,
# MAGIC     created_at,
# MAGIC     completed_at
# MAGIC FROM ranked
# MAGIC WHERE rn = 1
# MAGIC   AND (
# MAGIC         status = 'parsing'
# MAGIC      OR status = 'parsed'
# MAGIC      OR (status = 'done'  AND sftp_delivery_status = 'failed')
# MAGIC      OR status = 'error'
# MAGIC   )
# MAGIC ORDER BY categoria, filename

# COMMAND ----------

# DBTITLE 1,File non ancora processati
import os

INBOX   = f"/Volumes/{CATALOG}/{SCHEMA}/inbox"
ARCHIVE = f"/Volumes/{CATALOG}/{SCHEMA}/archive"
MANUAL  = f"/Volumes/{CATALOG}/{SCHEMA}/manual"

# File ancora in inbox (non ancora parsati)
n_inbox   = len([f for f in os.listdir(INBOX)   if f.endswith(".pdf")]) if os.path.exists(INBOX)   else 0
n_manual  = len([f for f in os.listdir(MANUAL)  if f.endswith(".pdf")]) if os.path.exists(MANUAL)  else 0

# File in archive (parsati + splittati, originale archiviato)
n_archive = sum(
    len([f for f in os.listdir(f"{ARCHIVE}/{y}/{m}") if f.endswith(".pdf")])
    for y in os.listdir(ARCHIVE) if os.path.isdir(f"{ARCHIVE}/{y}")
    for m in os.listdir(f"{ARCHIVE}/{y}") if os.path.isdir(f"{ARCHIVE}/{y}/{m}")
) if os.path.exists(ARCHIVE) else 0

print(f"Volumi fisici:")
print(f"  inbox/:   {n_inbox:>5} PDF  (da parsare)")
print(f"  archive/: {n_archive:>5} PDF  (elaborati, originale conservato)")
print(f"  manual/:  {n_manual:>5} PDF  (>100MB, skip automatico)")
print(f"  Totale:   {n_inbox + n_archive + n_manual:>5} PDF")

# COMMAND ----------

# DBTITLE 1,Dettaglio SFTP per run
# MAGIC %sql
# MAGIC -- Consegne SFTP per run_id (le ultime 10 run)
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     COUNT(DISTINCT filename)                                          AS n_files,
# MAGIC     SUM(CASE WHEN sftp_delivery_status = 'delivered' THEN 1 ELSE 0 END) AS delivered,
# MAGIC     SUM(CASE WHEN sftp_delivery_status = 'pending'   THEN 1 ELSE 0 END) AS pending,
# MAGIC     SUM(CASE WHEN sftp_delivery_status = 'failed'    THEN 1 ELSE 0 END) AS failed,
# MAGIC     MIN(completed_at)   AS first_completed,
# MAGIC     MAX(sftp_delivered_at) AS last_delivered
# MAGIC FROM `sbx-logistics`.`multidocument-us`.`processing_log`
# MAGIC WHERE status = 'done'
# MAGIC GROUP BY run_id
# MAGIC ORDER BY last_delivered DESC NULLS LAST
# MAGIC LIMIT 10