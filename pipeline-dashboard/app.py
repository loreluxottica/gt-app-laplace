"""Multidocument Control Tower — Flask backend.

Drives the whole batch journey: pick an inbox day folder, run ingest, watch the
live funnel, wait on the annotation gate (Ground Truth app), review metrics,
launch physical split + SFTP delivery to a user-typed remote folder.
Every error is a pipeline_events row and visible in the UI.

Run locally:  python app.py   →  http://localhost:8001
Auth: Databricks SDK DEFAULT profile or DATABRICKS_HOST + DATABRICKS_TOKEN.

Routes
  GET  /                       control-tower UI
  GET  /api/days               batch list: lifecycle + counts (v_batch_status ∪ inbox/)
  GET  /api/progress?day_id=   funnel counts + gate + active job runs + recent events (poll)
  GET  /api/gate?day_id=       n_sampled / n_annotated / missing / metrics
  GET  /api/files?day_id=&status=&q=   file search (v_file_status)
  GET  /api/file/<name>?day_id=        status + event timeline + LLM responses
  GET  /api/errors?day_id=     v_stuck_files with reasons
  GET  /api/sftp?day_id=       delivery board + deferred list
  GET  /api/review?day_id=     needs_review queue
  GET  /api/runs?day_id=       run summaries
  POST /api/run-ingest         {day_id, sample_pct} → jobs.run_now(job_ingest)
  POST /api/run-deliver        {day_id, sftp_remote_base} → job_deliver (gate-guarded)
  POST /api/redeliver          {day_id, filenames?, sftp_remote_base} → deferred → pending + job_deliver
  POST /api/action/<type>      retry-parse / retry-split / retry-sftp / mark-manual / approve-review
  GET  /api/health             liveness probe
"""
from __future__ import annotations

import re

from flask import Flask, jsonify, render_template, request

from src import actions, gate, jobs, queries, volumes
from src.config import config

app = Flask(__name__)

_SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]+$")


def _valid(name: str) -> bool:
    return bool(name) and bool(_SAFE_NAME.match(name)) and ".." not in name


def _day_id(required: bool = True):
    day = request.args.get("day_id", "").strip()
    if day and _valid(day):
        return day
    return None if (not required and not day) else (day if _valid(day) else None)


# ─────────────────────────────────────────────────────────────────────────────
@app.get("/")
def index():
    return render_template("control_tower.html", gt_app_url=config.GT_APP_URL)


@app.get("/api/health")
def health():
    return jsonify({"status": "ok", "actor": config.ACTOR})


@app.get("/api/days")
def days():
    """Batches = day folders in inbox/ ∪ day_ids in the tables, with lifecycle."""
    try:
        table_batches = {b["day_id"]: b for b in queries.batch_status()}
        inbox = volumes.inbox_days()
        out = []
        for day in sorted(set(inbox) | set(table_batches), reverse=True):
            b = table_batches.get(day)
            row = {
                "day_id": day,
                "lifecycle": b["lifecycle"] if b else "uploaded",
                "has_errors": (b or {}).get("has_errors") in (True, "true"),
                "n_files": int(b["n_files"]) if b else 0,
                "n_inbox": volumes.inbox_count(day) if day in inbox else 0,
                "counts": b or {},
            }
            # Refine: gate complete but not yet delivering → 'annotated'
            if row["lifecycle"] == "awaiting_annotation":
                g = gate.gate_state(day)
                if g["complete"]:
                    row["lifecycle"] = "annotated"
                row["gate"] = {k: g[k] for k in ("n_sampled", "n_annotated", "complete")}
            out.append(row)
        return jsonify({"days": out})
    except Exception as e:  # noqa: BLE001
        app.logger.exception("days failed")
        return jsonify({"error": str(e)}), 500


@app.get("/api/progress")
def progress():
    day = _day_id()
    if not day:
        return jsonify({"error": "day_id required"}), 400
    try:
        return jsonify({
            "day_id": day,
            "funnel": queries.funnel(day),
            "volumes": volumes.volume_counts(day),
            "gate": gate.gate_state(day),
            "active_runs": jobs.active_runs(),
            "events": queries.recent_events(day, limit=15),
        })
    except Exception as e:  # noqa: BLE001
        app.logger.exception("progress failed")
        return jsonify({"error": str(e)}), 500


@app.get("/api/gate")
def gate_route():
    day = _day_id()
    if not day:
        return jsonify({"error": "day_id required"}), 400
    try:
        return jsonify(gate.gate_state(day))
    except Exception as e:  # noqa: BLE001
        app.logger.exception("gate failed")
        return jsonify({"error": str(e)}), 500


@app.get("/api/files")
def files():
    day = request.args.get("day_id", "").strip() or None
    status = request.args.get("status", "").strip() or None
    q = request.args.get("q", "").strip() or None
    if day and not _valid(day):
        return jsonify({"error": "invalid day_id"}), 400
    try:
        return jsonify({"files": queries.files(day, status, q)})
    except Exception as e:  # noqa: BLE001
        app.logger.exception("files failed")
        return jsonify({"error": str(e)}), 500


@app.get("/api/file/<name>")
def file_detail(name: str):
    day = _day_id()
    if not _valid(name):
        return jsonify({"error": "invalid filename"}), 400
    if not day:
        return jsonify({"error": "day_id required"}), 400
    try:
        return jsonify(queries.file_detail(day, name))
    except Exception as e:  # noqa: BLE001
        app.logger.exception("file_detail failed")
        return jsonify({"error": str(e)}), 500


@app.get("/api/errors")
def errors():
    day = request.args.get("day_id", "").strip() or None
    if day and not _valid(day):
        return jsonify({"error": "invalid day_id"}), 400
    try:
        return jsonify({"stuck": queries.stuck_files(day)})
    except Exception as e:  # noqa: BLE001
        app.logger.exception("errors failed")
        return jsonify({"error": str(e)}), 500


@app.get("/api/sftp")
def sftp():
    day = request.args.get("day_id", "").strip() or None
    if day and not _valid(day):
        return jsonify({"error": "invalid day_id"}), 400
    try:
        return jsonify(queries.sftp_board(day))
    except Exception as e:  # noqa: BLE001
        app.logger.exception("sftp failed")
        return jsonify({"error": str(e)}), 500


@app.get("/api/review")
def review():
    day = request.args.get("day_id", "").strip() or None
    if day and not _valid(day):
        return jsonify({"error": "invalid day_id"}), 400
    try:
        return jsonify({"needs_review": queries.needs_review(day)})
    except Exception as e:  # noqa: BLE001
        app.logger.exception("review failed")
        return jsonify({"error": str(e)}), 500


@app.get("/api/runs")
def runs():
    day = request.args.get("day_id", "").strip() or None
    if day and not _valid(day):
        return jsonify({"error": "invalid day_id"}), 400
    try:
        return jsonify({"runs": queries.run_summary(day)})
    except Exception as e:  # noqa: BLE001
        app.logger.exception("runs failed")
        return jsonify({"error": str(e)}), 500


# ── Job launchers ────────────────────────────────────────────────────────────
@app.post("/api/run-ingest")
def run_ingest():
    body = request.get_json(silent=True) or {}
    day = (body.get("day_id") or "").strip()
    if not _valid(day):
        return jsonify({"error": "invalid day_id"}), 400
    try:
        pct = float(body.get("sample_pct", 10))
    except (TypeError, ValueError):
        return jsonify({"error": "sample_pct must be a number"}), 400
    if not (0 < pct <= 100):
        return jsonify({"error": "sample_pct must be in (0, 100]"}), 400
    try:
        res = jobs.run_ingest(day, pct)
        return jsonify({"launched": True, **res})
    except Exception as e:  # noqa: BLE001
        app.logger.exception("run_ingest failed")
        return jsonify({"error": str(e)}), 500


@app.post("/api/run-deliver")
def run_deliver():
    body = request.get_json(silent=True) or {}
    day = (body.get("day_id") or "").strip()
    remote = (body.get("sftp_remote_base") or "").strip()
    if not _valid(day):
        return jsonify({"error": "invalid day_id"}), 400
    if not remote or not remote.startswith("/"):
        return jsonify({"error": "sftp_remote_base must be an absolute remote path"}), 400
    try:
        # Annotation gate guard: never deliver before the sample is annotated.
        g = gate.gate_state(day)
        if g["n_sampled"] > 0 and not g["complete"]:
            return jsonify({
                "error": f"annotation gate incomplete: {g['n_annotated']}/{g['n_sampled']} "
                         f"annotated — finish the ground truth first",
                "gate": g,
            }), 409
        res = jobs.run_deliver(day, remote)
        return jsonify({"launched": True, **res})
    except Exception as e:  # noqa: BLE001
        app.logger.exception("run_deliver failed")
        return jsonify({"error": str(e)}), 500


@app.post("/api/redeliver")
def redeliver():
    body = request.get_json(silent=True) or {}
    day = (body.get("day_id") or "").strip()
    remote = (body.get("sftp_remote_base") or "").strip()
    filenames = body.get("filenames") or None
    if not _valid(day):
        return jsonify({"error": "invalid day_id"}), 400
    if not remote or not remote.startswith("/"):
        return jsonify({"error": "sftp_remote_base must be an absolute remote path"}), 400
    if filenames is not None:
        if not isinstance(filenames, list) or not all(_valid(f) for f in filenames):
            return jsonify({"error": "filenames must be a list of valid names"}), 400
    try:
        n = actions.reset_deferred(day, filenames)
        res = jobs.run_deliver(day, remote)
        return jsonify({"launched": True, "reset": n if n >= 0 else "all", **res})
    except Exception as e:  # noqa: BLE001
        app.logger.exception("redeliver failed")
        return jsonify({"error": str(e)}), 500


# ── File-level actions ───────────────────────────────────────────────────────
@app.post("/api/action/<action_type>")
def action(action_type: str):
    fn = actions.ACTIONS.get(action_type)
    if fn is None:
        return jsonify({"error": f"unknown action '{action_type}'"}), 404
    body = request.get_json(silent=True) or {}
    day = (body.get("day_id") or "").strip()
    name = (body.get("filename") or "").strip()
    if not _valid(day) or not _valid(name):
        return jsonify({"error": "valid day_id and filename required"}), 400
    try:
        msg = fn(day, name)
        return jsonify({"done": True, "message": msg})
    except Exception as e:  # noqa: BLE001
        app.logger.exception("action failed")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8001, debug=True)
