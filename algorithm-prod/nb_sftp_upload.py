# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Install paramiko
# MAGIC %pip install paramiko --quiet

# COMMAND ----------

# DBTITLE 1,Restart kernel
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Config and SFTP connection
# ══════════════════════════════════════════════════════════════════════
# nb_sftp_upload — Upload split PDFs to SFTP
# ══════════════════════════════════════════════════════════════════════
# Reads files from output/YYYYMMDD/folder_id/ and uploads to SFTP.
# Updates processing_log with sftp_delivered_at and sftp_delivery_status.
# Task 4 of the multi-document splitting job.
# ══════════════════════════════════════════════════════════════════════

import paramiko
import os
import uuid
from datetime import datetime

# ── Schema & Paths ──
CATALOG = "sbx-logistics"
SCHEMA  = "multidocument-us"

OUTPUT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/output"
TABLE_LOG   = f"`{CATALOG}`.`{SCHEMA}`.`processing_log`"

# ── Run date: da widget o data odierna ──
try:
    RUN_DATE = dbutils.widgets.get("run_date")
except:
    RUN_DATE = datetime.now().strftime("%Y%m%d")

try:
    RUN_ID = dbutils.widgets.get("run_id")
except:
    RUN_ID = str(uuid.uuid4())

OUTPUT_DATE_PATH = f"{OUTPUT_PATH}/{RUN_DATE}"
RUN_START = datetime.now()

# ── SFTP config ──
SFTP_HOST = "ftpservice.luxottica.com"
SFTP_PORT = 22
SFTP_USER = "LAPLACE"

# Password da Databricks secret scope (mai in chiaro nel codice)
try:
    SFTP_PASS = dbutils.secrets.get(scope="sftp-laplace", key="sftp_password")
except Exception:
    # Fallback a widget per test manuali (NON usare in produzione)
    SFTP_PASS = dbutils.widgets.get("sftp_password")

# ── Remote base path su SFTP ──
# I file vengono caricati in: SFTP_REMOTE_BASE/folder_id/filename_001.pdf
SFTP_REMOTE_BASE = "/Laplace/LAPLACE/US/20260711"

print(f"{'═' * 60}")
print(f"  nb_sftp_upload — Production Run")
print(f"{'═' * 60}")
print(f"  Run ID:       {RUN_ID}")
print(f"  Run Date:     {RUN_DATE}")
print(f"  Source:       {OUTPUT_DATE_PATH}")
print(f"  SFTP Host:    {SFTP_HOST}:{SFTP_PORT}")
print(f"  SFTP User:    {SFTP_USER}")
print(f"  Remote base:  {SFTP_REMOTE_BASE}/{{folder_id}}/")
print(f"{'═' * 60}")

# COMMAND ----------

# DBTITLE 1,Build upload list
# ── Carica lista file da uploadare (sftp_delivery_status = 'pending') ──
df_pending = spark.sql(f"""
    SELECT filename, folder_id, sftp_target_folder
    FROM {TABLE_LOG}
    WHERE sftp_delivery_status = 'pending'
      AND status = 'done'
""").collect()

n_pending = len(df_pending)

if n_pending == 0:
    print("ℹ️  Nessun file in stato 'pending' da caricare su SFTP. Terminazione.")
    dbutils.notebook.exit("NO_FILES_PENDING")

# Per ogni file pending, trova i file splittati nella cartella output
upload_tasks = []
for row in df_pending:
    folder_path = row["sftp_target_folder"]  # es. /Volumes/.../output/20260625/34572
    if not folder_path or not os.path.exists(folder_path):
        continue
    for fname in sorted(os.listdir(folder_path)):
        if fname.endswith(".pdf"):
            upload_tasks.append({
                "filename":     row["filename"],
                "folder_id":    row["folder_id"],
                "local_path":   f"{folder_path}/{fname}",
                "remote_folder": f"{SFTP_REMOTE_BASE}/{row['folder_id']}",
                "remote_path":  f"{SFTP_REMOTE_BASE}/{row['folder_id']}/{fname}"
            })

print(f"File originali da processare: {n_pending}")
print(f"PDF splittati da caricare:     {len(upload_tasks)}")

# COMMAND ----------

# DBTITLE 1,SFTP upload
# ── SFTP upload + verifica integrità + completezza cartelle (parallelo) ──
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from queue import Queue
import threading

N_WORKERS = 6  # connessioni SFTP parallele — ridurre a 2-4 se il server limita

upload_ok    = []
upload_error = []
verify_fail  = []

# ── Pool di N_WORKERS connessioni SFTP ──
print(f"Apertura pool di {N_WORKERS} connessioni SFTP...")
conn_pool  = Queue()
transports = []
for _ in range(N_WORKERS):
    t = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    t.connect(username=SFTP_USER, password=SFTP_PASS)
    conn_pool.put(paramiko.SFTPClient.from_transport(t))
    transports.append(t)
print(f"✓ {N_WORKERS} connessioni aperte a {SFTP_HOST}:{SFTP_PORT}")

try:
    # ── Upload parallelo ──
    _progress_lock = threading.Lock()
    _count = [0]

    def upload_one(task):
        sftp = conn_pool.get()
        try:
            sftp.put(task["local_path"], task["remote_path"])
            with _progress_lock:
                _count[0] += 1
                if _count[0] % 200 == 0:
                    print(f"  ... {_count[0]}/{len(upload_tasks)} caricati")
            return {"ok": True, "task": task}
        except Exception as e:
            return {"ok": False, "task": task, "error": str(e)[:400]}
        finally:
            conn_pool.put(sftp)

    print(f"\n── Upload ({len(upload_tasks)} file, {N_WORKERS} worker paralleli) ──")
    with ThreadPoolExecutor(max_workers=N_WORKERS) as ex:
        for res in as_completed({ex.submit(upload_one, t): t for t in upload_tasks}):
            r = res.result()
            if r["ok"]:
                upload_ok.append(r["task"])
            else:
                upload_error.append({**r["task"], "error": r["error"]})
                print(f"  ✗ {os.path.basename(r['task']['local_path'])}: {r['error'][:100]}")
    print(f"✓ Upload: {len(upload_ok)} OK, {len(upload_error)} errori")

    # ── Verifica integrità parallela ──
    def verify_one(task):
        sftp       = conn_pool.get()
        local_size = os.path.getsize(task["local_path"])
        try:
            remote_size = sftp.stat(task["remote_path"]).st_size
            if remote_size == local_size:
                return {"ok": True, "task": task}
            return {"ok": False, "task": task,
                    "issue": f"size mismatch: locale={local_size}B remoto={remote_size}B"}
        except FileNotFoundError:
            return {"ok": False, "task": task, "issue": "file non trovato su SFTP dopo upload"}
        except Exception as e:
            return {"ok": False, "task": task, "issue": str(e)[:200]}
        finally:
            conn_pool.put(sftp)

    print(f"\n── Verifica integrità ({len(upload_ok)} file) ──")
    verified_ok = []
    with ThreadPoolExecutor(max_workers=N_WORKERS) as ex:
        for res in as_completed({ex.submit(verify_one, t): t for t in upload_ok}):
            r = res.result()
            if r["ok"]:
                verified_ok.append(r["task"])
            else:
                verify_fail.append({**r["task"], "issue": r["issue"]})
                print(f"  ⚠️  {os.path.basename(r['task']['local_path'])}: {r['issue'][:100]}")
    upload_ok = verified_ok
    print(f"✓ Verifica: {len(upload_ok)} OK, {len(verify_fail)} falliti")

    # ── Completezza cartelle parallela ──
    expected_by_folder = defaultdict(set)
    for task in upload_tasks:
        expected_by_folder[task["remote_folder"]].add(os.path.basename(task["local_path"]))

    def check_folder(item):
        remote_folder, expected = item
        sftp = conn_pool.get()
        fid  = remote_folder.split("/")[-1]
        try:
            actual  = set(sftp.listdir(remote_folder))
            missing = expected - actual
            return {"folder_id": fid, "ok": not missing,
                    "missing": sorted(missing), "present": len(expected & actual),
                    "total": len(expected)}
        except FileNotFoundError:
            return {"folder_id": fid, "ok": False, "missing": sorted(expected)}
        except Exception as e:
            return {"folder_id": fid, "ok": False, "missing": [], "error": str(e)[:200]}
        finally:
            conn_pool.put(sftp)

    print(f"\n── Completezza cartelle ({len(expected_by_folder)} cartelle) ──")
    folder_ok, folder_incomplete = [], []
    with ThreadPoolExecutor(max_workers=N_WORKERS) as ex:
        for r in ex.map(check_folder, expected_by_folder.items()):
            if r["ok"]:
                folder_ok.append(r["folder_id"])
            else:
                folder_incomplete.append(r)
                if r.get("missing"):
                    print(f"  ✗ {r['folder_id']}/  mancanti: {r['missing']}")
    print(f"✓ Cartelle: {len(folder_ok)} complete, {len(folder_incomplete)} incomplete")

    incomplete_folders = {r["folder_id"] for r in folder_incomplete}
    upload_ok = [t for t in upload_ok if t["folder_id"] not in incomplete_folders]

finally:
    for t in transports:
        try: t.close()
        except: pass
    print(f"\n✓ {len(transports)} connessioni SFTP chiuse")

print(f"\n{'═'*55}")
print(f"  Caricati e verificati: {len(upload_ok) + len(verify_fail)}")
print(f"  Cartelle complete:     {len(folder_ok)}")
print(f"  Cartelle incomplete:   {len(folder_incomplete)}")
print(f"  Marcati delivered:     {len(upload_ok)}")
print(f"  Errori upload:         {len(upload_error)}")
print(f"  Errori verifica size:  {len(verify_fail)}")
print(f"{'═'*55}")

# COMMAND ----------

# DBTITLE 1,Update processing_log
# ── Aggiorna processing_log post-upload (batch MERGE, 1 query per stato) ──
from collections import defaultdict
from pyspark.sql import Row

# Conta attesi vs ok per filename originale
expected_count = defaultdict(int)
ok_count       = defaultdict(int)
for t in upload_tasks:
    expected_count[t["filename"]] += 1
for t in upload_ok:
    ok_count[t["filename"]] += 1

# Delivered solo se TUTTI i file splittati sono stati caricati e verificati
filenames_ok    = {fname for fname, cnt in ok_count.items() if cnt == expected_count[fname]}
filenames_error = {t["filename"] for t in upload_error} - filenames_ok

# ── MERGE delivered: 1 query per tutti i file OK ──
if filenames_ok:
    remote_by_fname = {t["filename"]: t["remote_folder"] for t in upload_ok}
    df_ok = spark.createDataFrame([
        Row(filename=fname, sftp_folder=remote_by_fname[fname])
        for fname in filenames_ok
    ])
    df_ok.createOrReplaceTempView("_sftp_delivered")
    spark.sql(f"""
        MERGE INTO {TABLE_LOG} AS log
        USING _sftp_delivered AS upd
          ON  log.filename = upd.filename
          AND log.status   = 'done'
        WHEN MATCHED THEN UPDATE SET
            log.sftp_delivery_status = 'delivered',
            log.sftp_delivered_at    = current_timestamp(),
            log.sftp_target_folder   = upd.sftp_folder
    """)

# ── MERGE failed: 1 query per tutti i file falliti ──
if filenames_error:
    error_by_fname = {}
    for t in upload_error:
        if t["filename"] not in filenames_ok:
            error_by_fname[t["filename"]] = t["error"][:400]
    df_err = spark.createDataFrame([
        Row(filename=fname, err_msg=msg)
        for fname, msg in error_by_fname.items()
    ])
    df_err.createOrReplaceTempView("_sftp_failed")
    spark.sql(f"""
        MERGE INTO {TABLE_LOG} AS log
        USING _sftp_failed AS upd
          ON  log.filename = upd.filename
        WHEN MATCHED THEN UPDATE SET
            log.sftp_delivery_status = 'failed',
            log.sftp_delivery_error  = upd.err_msg
    """)

print(f"✓ processing_log aggiornato: {len(filenames_ok)} delivered, {len(filenames_error)} failed")

# COMMAND ----------

# DBTITLE 1,Run summary
# ── Run Summary ──
from datetime import datetime

RUN_END = datetime.now()
run_duration_min = (RUN_END - RUN_START).total_seconds() / 60
run_status = "success" if len(upload_error) == 0 and len(verify_fail) == 0 else "partial_failure"

print(f"\n{'═' * 60}")
print(f"  SFTP UPLOAD SUMMARY")
print(f"{'═' * 60}")
print(f"  Run ID:      {RUN_ID}")
print(f"  Status:      {run_status}")
print(f"  Duration:    {run_duration_min:.1f} min")
print(f"  File pending:   {n_pending}")
print(f"  PDF caricati:   {len(upload_ok)}")
print(f"  Cartelle complete:   {len(folder_ok)}")
print(f"  Cartelle incomplete: {len(folder_incomplete)}")
print(f"  Errori upload:       {len(upload_error)}")
print(f"  Errori verifica:     {len(verify_fail)}")
if upload_error:
    print(f"\n  File con errori upload:")
    for e in upload_error:
        print(f"    ✗ {os.path.basename(e['local_path'])}: {e['error'][:80]}")
if verify_fail:
    print(f"\n  File con verifica fallita:")
    for e in verify_fail:
        print(f"    ⚠️  {os.path.basename(e['local_path'])}: {e['issue'][:80]}")
print(f"{'═' * 60}")

dbutils.notebook.exit(
    f'{{"run_id": "{RUN_ID}", "status": "{run_status}", '
    f'"uploaded": {len(upload_ok)}, "folders_ok": {len(folder_ok)}, '
    f'"folders_incomplete": {len(folder_incomplete)}, '
    f'"upload_errors": {len(upload_error)}, "verify_errors": {len(verify_fail)}}}'  
)