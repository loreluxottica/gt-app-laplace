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

# DBTITLE 1,Load shared helpers
# MAGIC %run ./nb_helpers

# COMMAND ----------

# DBTITLE 1,Config and SFTP connection
# ══════════════════════════════════════════════════════════════════════
# nb_sftp_upload — Upload split PDFs to SFTP
# ══════════════════════════════════════════════════════════════════════
# Task 2 of job_deliver (pdf_split → sftp_upload).
# Reads files from output/{day_id}/{folder_id}/ and uploads to SFTP.
#
# Rules (all errors documented in pipeline_events):
# - sftp_remote_base is a REQUIRED job parameter typed by the user per
#   delivery. Preflight verifies it exists on the server — abort if not
#   (typo guard). The base is NEVER auto-created.
# - A missing {base}/{folder_id}/ remote subfolder is NOT an error and
#   NOT auto-created: its files are marked 'deferred' and can be
#   re-delivered later to a different base from the control tower.
# - Uploads retry 3× with exponential backoff; dead connections are
#   replaced transparently.
# ══════════════════════════════════════════════════════════════════════

import paramiko
import os, time
from datetime import datetime

# ── Batch + run identity (job parameters) ──
DAY_ID = get_day_id()
RUN_ID = get_run_id()
PATHS = volume_paths(DAY_ID)
OUTPUT_DATE_PATH = PATHS["output"]

# ── Remote base path su SFTP (REQUIRED, typed by the user per delivery) ──
dbutils.widgets.text("sftp_remote_base", "")
SFTP_REMOTE_BASE = dbutils.widgets.get("sftp_remote_base").strip().rstrip("/")
if not SFTP_REMOTE_BASE:
    raise ValueError(
        "Widget 'sftp_remote_base' is required: full remote path for this delivery "
        "(e.g. /Laplace/LAPLACE/US/20260711). It changes every delivery — no default."
    )

RUN_START = datetime.now()
events = EventLogger(RUN_ID, DAY_ID, stage="sftp_upload")
events.log("run_started", detail=f"remote_base={SFTP_REMOTE_BASE}")
events.flush()

# ── SFTP config ──
SFTP_HOST = "ftpservice.luxottica.com"
SFTP_PORT = 22
SFTP_USER = "LAPLACE"

MAX_PUT_RETRIES = 3
RETRY_BACKOFF_SEC = [5, 15, 45]

# Password da Databricks secret scope (mai in chiaro nel codice)
try:
    SFTP_PASS = dbutils.secrets.get(scope="sftp-laplace", key="sftp_password")
except Exception:
    # Fallback a widget per test manuali (NON usare in produzione)
    SFTP_PASS = dbutils.widgets.get("sftp_password")

print(f"{'═' * 60}")
print(f"  nb_sftp_upload — Production Run")
print(f"{'═' * 60}")
print(f"  Run ID:       {RUN_ID}")
print(f"  Day ID:       {DAY_ID}")
print(f"  Source:       {OUTPUT_DATE_PATH}")
print(f"  SFTP Host:    {SFTP_HOST}:{SFTP_PORT}")
print(f"  SFTP User:    {SFTP_USER}")
print(f"  Remote base:  {SFTP_REMOTE_BASE}/{{folder_id}}/")
print(f"{'═' * 60}")

# COMMAND ----------

# DBTITLE 1,Preflight — remote base must exist + probe folder_id subfolders
# One short-lived connection: validate the user-typed base path BEFORE any upload.
_t = paramiko.Transport((SFTP_HOST, SFTP_PORT))
_t.connect(username=SFTP_USER, password=SFTP_PASS)
_sftp = paramiko.SFTPClient.from_transport(_t)

try:
    _sftp.stat(SFTP_REMOTE_BASE)
    print(f"✓ Preflight: remote base exists: {SFTP_REMOTE_BASE}")
except FileNotFoundError:
    events.log("error", error_message=f"remote base not found: {SFTP_REMOTE_BASE}",
               detail="preflight failed — nothing uploaded (typo guard, base is never auto-created)")
    events.flush()
    _t.close()
    raise ValueError(
        f"SFTP remote base does not exist: {SFTP_REMOTE_BASE} — "
        f"nothing was uploaded. Fix the path and re-run job_deliver."
    )

# Existing folder_id subfolders under the base
try:
    existing_remote_folders = set(_sftp.listdir(SFTP_REMOTE_BASE))
except Exception as e:
    existing_remote_folders = set()
    print(f"⚠️  Could not list remote base ({e}) — all folders treated as missing")
finally:
    _t.close()

print(f"✓ Preflight: {len(existing_remote_folders)} existing remote subfolders")

# COMMAND ----------

# DBTITLE 1,Build upload list (missing remote folders → deferred)
# ── Carica lista file da uploadare (sftp_delivery_status = 'pending') ──
df_pending = spark.sql(f"""
    SELECT filename, folder_id, sftp_target_folder
    FROM {TABLE_LOG}
    WHERE day_id = '{DAY_ID}'
      AND sftp_delivery_status = 'pending'
      AND status = 'done'
""").collect()

n_pending = len(df_pending)

if n_pending == 0:
    events.log("run_completed", new_status="NO_FILES_PENDING")
    events.flush()
    print("ℹ️  Nessun file in stato 'pending' da caricare su SFTP. Terminazione.")
    dbutils.notebook.exit("NO_FILES_PENDING")

# Files whose remote folder_id subfolder is missing → deferred, not uploaded.
# The user re-delivers them later to another base from the control tower.
upload_tasks = []
deferred_rows = []
missing_local = []
for row in df_pending:
    folder_path = row["sftp_target_folder"]  # es. /Volumes/.../output/{day_id}/34572
    if not folder_path or not os.path.exists(folder_path):
        missing_local.append(row["filename"])
        events.log("error", filename=row["filename"], folder_id=row["folder_id"],
                   error_message=f"local output folder missing: {folder_path}")
        continue
    if row["folder_id"] not in existing_remote_folders:
        deferred_rows.append(row)
        continue
    for fname in sorted(os.listdir(folder_path)):
        if fname.endswith(".pdf"):
            upload_tasks.append({
                "filename":      row["filename"],
                "folder_id":     row["folder_id"],
                "local_path":    f"{folder_path}/{fname}",
                "remote_folder": f"{SFTP_REMOTE_BASE}/{row['folder_id']}",
                "remote_path":   f"{SFTP_REMOTE_BASE}/{row['folder_id']}/{fname}",
            })

# Mark deferred files now — documented and re-deliverable, never silent
if deferred_rows:
    merge_processing_log(
        DAY_ID, RUN_ID,
        rows=[{
            "filename": r["filename"],
            "sftp_delivery_status": "deferred",
            "sftp_delivery_error": f"remote folder missing: {SFTP_REMOTE_BASE}/{r['folder_id']}",
        } for r in deferred_rows],
        set_cols=["sftp_delivery_status", "sftp_delivery_error"],
    )
    for r in deferred_rows:
        events.log("deferred", filename=r["filename"], folder_id=r["folder_id"],
                   detail=f"remote folder missing: {SFTP_REMOTE_BASE}/{r['folder_id']}")
    print(f"⚠️  {len(deferred_rows)} file DEFERRED (remote folder_id missing) — "
          f"re-deliver later with a different base")
events.flush()

print(f"File originali da processare: {n_pending}")
print(f"  Deferred (cartella remota mancante): {len(deferred_rows)}")
print(f"  Output locale mancante:              {len(missing_local)}")
print(f"PDF splittati da caricare:     {len(upload_tasks)}")

if not upload_tasks:
    events.log("run_completed", new_status="ALL_DEFERRED",
               detail=f"deferred={len(deferred_rows)}")
    events.flush()
    dbutils.notebook.exit(
        f'{{"run_id": "{RUN_ID}", "day_id": "{DAY_ID}", "status": "all_deferred", '
        f'"deferred": {len(deferred_rows)}}}'
    )

# COMMAND ----------

# DBTITLE 1,SFTP upload (retry + reconnect)
# ── SFTP upload + verifica integrità + completezza cartelle (parallelo) ──
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from queue import Queue
import threading

N_WORKERS = 6  # connessioni SFTP parallele — ridurre a 2-4 se il server limita

upload_ok    = []
upload_error = []
verify_fail  = []

def open_sftp_client():
    t = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    t.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(t)
    sftp._transport_ref = t  # keep the transport findable for cleanup
    return sftp

# ── Pool di N_WORKERS connessioni SFTP ──
print(f"Apertura pool di {N_WORKERS} connessioni SFTP...")
conn_pool  = Queue()
transports = []
_transports_lock = threading.Lock()
for _ in range(N_WORKERS):
    c = open_sftp_client()
    conn_pool.put(c)
    transports.append(c._transport_ref)
print(f"✓ {N_WORKERS} connessioni aperte a {SFTP_HOST}:{SFTP_PORT}")


def with_retry(fn, task, what):
    """Run fn(sftp, task) with MAX_PUT_RETRIES attempts and exponential backoff.
    A dead connection is discarded and replaced with a fresh one."""
    last_err = None
    for attempt in range(MAX_PUT_RETRIES):
        sftp = conn_pool.get()
        try:
            result = fn(sftp, task)
            conn_pool.put(sftp)
            return result
        except FileNotFoundError:
            conn_pool.put(sftp)
            raise  # not transient — caller decides
        except Exception as e:
            last_err = e
            # transport may be dead: replace the pooled connection
            try:
                sftp._transport_ref.close()
            except Exception:
                pass
            try:
                fresh = open_sftp_client()
                conn_pool.put(fresh)
                with _transports_lock:
                    transports.append(fresh._transport_ref)
            except Exception:
                conn_pool.put(sftp)  # reconnection failed: return old, better than deadlock
            if attempt < MAX_PUT_RETRIES - 1:
                time.sleep(RETRY_BACKOFF_SEC[attempt])
    raise last_err

try:
    # ── Upload parallelo ──
    _progress_lock = threading.Lock()
    _count = [0]

    def upload_one(task):
        try:
            with_retry(lambda s, t: s.put(t["local_path"], t["remote_path"]), task, "put")
            with _progress_lock:
                _count[0] += 1
                if _count[0] % 200 == 0:
                    print(f"  ... {_count[0]}/{len(upload_tasks)} caricati")
            return {"ok": True, "task": task}
        except Exception as e:
            return {"ok": False, "task": task, "error": str(e)[:400]}

    print(f"\n── Upload ({len(upload_tasks)} file, {N_WORKERS} worker paralleli, "
          f"retry {MAX_PUT_RETRIES}x) ──")
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
        local_size = os.path.getsize(task["local_path"])
        try:
            remote_size = with_retry(
                lambda s, t: s.stat(t["remote_path"]).st_size, task, "stat")
            if remote_size == local_size:
                return {"ok": True, "task": task}
            return {"ok": False, "task": task,
                    "issue": f"size mismatch: locale={local_size}B remoto={remote_size}B"}
        except FileNotFoundError:
            return {"ok": False, "task": task, "issue": "file non trovato su SFTP dopo upload"}
        except Exception as e:
            return {"ok": False, "task": task, "issue": str(e)[:200]}

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
        fid = remote_folder.split("/")[-1]
        try:
            actual = with_retry(lambda s, t: set(s.listdir(remote_folder)), item, "listdir")
            missing = expected - actual
            return {"folder_id": fid, "ok": not missing,
                    "missing": sorted(missing), "present": len(expected & actual),
                    "total": len(expected)}
        except FileNotFoundError:
            return {"folder_id": fid, "ok": False, "missing": sorted(expected)}
        except Exception as e:
            return {"folder_id": fid, "ok": False, "missing": [], "error": str(e)[:200]}

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

# DBTITLE 1,Update processing_log (every outcome recorded, nothing silent)
from collections import defaultdict
from datetime import datetime as _dt

# Conta attesi vs ok per filename originale
expected_count = defaultdict(int)
ok_count       = defaultdict(int)
for t in upload_tasks:
    expected_count[t["filename"]] += 1
for t in upload_ok:
    ok_count[t["filename"]] += 1

# Delivered solo se TUTTI i file splittati sono stati caricati e verificati
filenames_ok = {fname for fname, cnt in ok_count.items() if cnt == expected_count[fname]}

# Failure reasons per filename, most specific first
fail_reason = {}
for t in upload_error:
    fail_reason.setdefault(t["filename"], f"upload error: {t['error'][:300]}")
for t in verify_fail:
    fail_reason.setdefault(t["filename"], f"verify failed: {t['issue'][:300]}")
incomplete_by_folder = {r["folder_id"]: r for r in folder_incomplete}
for t in upload_tasks:
    if t["filename"] not in filenames_ok and t["filename"] not in fail_reason:
        if t["folder_id"] in incomplete_by_folder:
            miss = incomplete_by_folder[t["folder_id"]].get("missing", [])
            fail_reason[t["filename"]] = (
                f"folder incomplete: {t['folder_id']} missing {len(miss)} files")
filenames_error = set(fail_reason) - filenames_ok

# ── Delivered ──
if filenames_ok:
    remote_by_fname = {t["filename"]: t["remote_folder"] for t in upload_ok}
    now = _dt.now()
    merge_processing_log(
        DAY_ID, RUN_ID,
        rows=[{
            "filename": fname,
            "sftp_delivery_status": "delivered",
            "sftp_delivered_at": now,
            "sftp_target_folder": remote_by_fname[fname],
            "sftp_delivery_error": None,
        } for fname in filenames_ok],
        set_cols=["sftp_delivery_status", "sftp_delivered_at",
                  "sftp_target_folder", "sftp_delivery_error"],
        match_status=["done"],
    )
    for fname in filenames_ok:
        events.log("delivered", filename=fname, detail=remote_by_fname[fname])

# ── Failed (upload error / verify fail / folder incomplete) — reason recorded ──
if filenames_error:
    merge_processing_log(
        DAY_ID, RUN_ID,
        rows=[{
            "filename": fname,
            "sftp_delivery_status": "failed",
            "sftp_delivery_error": fail_reason[fname],
        } for fname in filenames_error],
        set_cols=["sftp_delivery_status", "sftp_delivery_error"],
    )
    for fname in filenames_error:
        etype = ("verify_failed" if fail_reason[fname].startswith("verify")
                 else "folder_incomplete" if fail_reason[fname].startswith("folder")
                 else "error")
        events.log(etype, filename=fname, error_message=fail_reason[fname])

events.flush()
print(f"✓ processing_log aggiornato: {len(filenames_ok)} delivered, "
      f"{len(filenames_error)} failed, {len(deferred_rows)} deferred")

# COMMAND ----------

# DBTITLE 1,Run summary
from datetime import datetime

RUN_END = datetime.now()
run_duration_min = (RUN_END - RUN_START).total_seconds() / 60
run_status = "success" if len(upload_error) == 0 and len(verify_fail) == 0 else "partial_failure"

events.log("run_completed", new_status=run_status,
           detail=f"delivered={len(filenames_ok)} failed={len(filenames_error)} "
                  f"deferred={len(deferred_rows)} folders_ok={len(folder_ok)} "
                  f"folders_incomplete={len(folder_incomplete)}")
events.flush()

print(f"\n{'═' * 60}")
print(f"  SFTP UPLOAD SUMMARY")
print(f"{'═' * 60}")
print(f"  Run ID:      {RUN_ID}")
print(f"  Day ID:      {DAY_ID}")
print(f"  Status:      {run_status}")
print(f"  Duration:    {run_duration_min:.1f} min")
print(f"  File pending:   {n_pending}")
print(f"  PDF caricati:   {len(upload_ok)}")
print(f"  Deferred:       {len(deferred_rows)} (cartella remota mancante)")
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
    f'{{"run_id": "{RUN_ID}", "day_id": "{DAY_ID}", "status": "{run_status}", '
    f'"uploaded": {len(upload_ok)}, "delivered": {len(filenames_ok)}, '
    f'"deferred": {len(deferred_rows)}, "folders_ok": {len(folder_ok)}, '
    f'"folders_incomplete": {len(folder_incomplete)}, '
    f'"upload_errors": {len(upload_error)}, "verify_errors": {len(verify_fail)}}}'
)
