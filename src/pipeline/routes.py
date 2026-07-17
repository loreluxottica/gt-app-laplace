"""Control-tower API: batch journey, jobs, gate, errors, SFTP, review, files.

Drives the whole batch journey: pick an inbox day folder, run ingest, watch the
live funnel, wait on the annotation gate, review metrics, launch physical split
+ SFTP delivery to a user-typed remote folder. Every error is a pipeline_events
row and visible in the UI.

Routes (mounted at /api)
  GET  /days               batch list: lifecycle + counts (inbox/ ∪ validation/ ∪ tables)
  GET  /progress?day_id=   funnel counts + gate + active job runs + recent events (poll)
  GET  /gate?day_id=       n_sampled / n_annotated / missing / metrics
  GET  /files?day_id=&status=&q=   file search (v_file_status)
  GET  /file/<name>?day_id=        status + event timeline + LLM responses
  GET  /errors?day_id=     v_stuck_files with reasons
  GET  /sftp?day_id=       delivery board + deferred list
  GET  /review?day_id=     needs_review queue
  GET  /runs?day_id=       run summaries
  POST /run-ingest         {day_id, sample_pct} → jobs.run_now(job_ingest)
  POST /run-deliver        {day_id, sftp_remote_base} → job_deliver (gate-guarded)
  POST /redeliver          {day_id, filenames?, sftp_remote_base} → deferred → pending
  POST /action/<type>      retry-parse / retry-split / retry-sftp / mark-manual / approve-review
  GET  /health             liveness probe
"""
from __future__ import annotations

from flask import Blueprint, current_app, jsonify, request

from ..core import auth, volumes
from ..core.config import config
from ..core.http import day_id_arg, optional_day_arg, valid_name
from . import actions, gate, jobs, queries

bp = Blueprint("pipeline", __name__, url_prefix="/api")


def _forbidden():
    return jsonify({"error": "not authorised to operate the pipeline"}), 403


# ─────────────────────────────────────────────────────────────────────────────
@bp.get("/health")
def health():
    return jsonify({
        "status": "ok",
        "actor": auth.actor(),
        "can_operate": auth.can_operate(),
    })


@bp.get("/days")
def days():
    """Batches = day folders in inbox/ ∪ validation/ ∪ day_ids in the tables.

    validation/ is in the union because a batch whose inbox folder was cleaned
    still has an annotation sample to work through — dropping it would silently
    remove work from the annotator's picker.
    """
    try:
        table_batches = {b["day_id"]: b for b in queries.batch_status()}
        inbox = volumes.inbox_days()
        sampled = volumes.day_folders(config.VALIDATION_VOLUME)
        out = []
        for day in sorted(set(inbox) | set(sampled) | set(table_batches), reverse=True):
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
        current_app.logger.exception("days failed")
        return jsonify({"error": str(e)}), 500


@bp.get("/progress")
def progress():
    day = day_id_arg()
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
        current_app.logger.exception("progress failed")
        return jsonify({"error": str(e)}), 500


@bp.get("/gate")
def gate_route():
    day = day_id_arg()
    if not day:
        return jsonify({"error": "day_id required"}), 400
    try:
        return jsonify(gate.gate_state(day))
    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("gate failed")
        return jsonify({"error": str(e)}), 500


@bp.get("/files")
def files():
    day, ok = optional_day_arg()
    if not ok:
        return jsonify({"error": "invalid day_id"}), 400
    status = request.args.get("status", "").strip() or None
    q = request.args.get("q", "").strip() or None
    try:
        return jsonify({"files": queries.files(day, status, q)})
    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("files failed")
        return jsonify({"error": str(e)}), 500


@bp.get("/file/<name>")
def file_detail(name: str):
    day = day_id_arg()
    if not valid_name(name):
        return jsonify({"error": "invalid filename"}), 400
    if not day:
        return jsonify({"error": "day_id required"}), 400
    try:
        return jsonify(queries.file_detail(day, name))
    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("file_detail failed")
        return jsonify({"error": str(e)}), 500


@bp.get("/errors")
def errors():
    day, ok = optional_day_arg()
    if not ok:
        return jsonify({"error": "invalid day_id"}), 400
    try:
        return jsonify({"stuck": queries.stuck_files(day)})
    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("errors failed")
        return jsonify({"error": str(e)}), 500


@bp.get("/sftp")
def sftp():
    day, ok = optional_day_arg()
    if not ok:
        return jsonify({"error": "invalid day_id"}), 400
    try:
        return jsonify(queries.sftp_board(day))
    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("sftp failed")
        return jsonify({"error": str(e)}), 500


@bp.get("/review")
def review():
    day, ok = optional_day_arg()
    if not ok:
        return jsonify({"error": "invalid day_id"}), 400
    try:
        return jsonify({"needs_review": queries.needs_review(day)})
    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("review failed")
        return jsonify({"error": str(e)}), 500


@bp.get("/runs")
def runs():
    day, ok = optional_day_arg()
    if not ok:
        return jsonify({"error": "invalid day_id"}), 400
    try:
        return jsonify({"runs": queries.run_summary(day)})
    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("runs failed")
        return jsonify({"error": str(e)}), 500


# ── Job launchers ────────────────────────────────────────────────────────────
@bp.post("/run-ingest")
def run_ingest():
    if not auth.can_operate():
        return _forbidden()
    body = request.get_json(silent=True) or {}
    day = (body.get("day_id") or "").strip()
    if not valid_name(day):
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
        current_app.logger.exception("run_ingest failed")
        return jsonify({"error": str(e)}), 500


@bp.post("/run-deliver")
def run_deliver():
    if not auth.can_operate():
        return _forbidden()
    body = request.get_json(silent=True) or {}
    day = (body.get("day_id") or "").strip()
    remote = (body.get("sftp_remote_base") or "").strip()
    if not valid_name(day):
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
        current_app.logger.exception("run_deliver failed")
        return jsonify({"error": str(e)}), 500


@bp.post("/redeliver")
def redeliver():
    if not auth.can_operate():
        return _forbidden()
    body = request.get_json(silent=True) or {}
    day = (body.get("day_id") or "").strip()
    remote = (body.get("sftp_remote_base") or "").strip()
    filenames = body.get("filenames") or None
    if not valid_name(day):
        return jsonify({"error": "invalid day_id"}), 400
    if not remote or not remote.startswith("/"):
        return jsonify({"error": "sftp_remote_base must be an absolute remote path"}), 400
    if filenames is not None:
        if not isinstance(filenames, list) or not all(valid_name(f) for f in filenames):
            return jsonify({"error": "filenames must be a list of valid names"}), 400
    try:
        n = actions.reset_deferred(day, filenames)
        res = jobs.run_deliver(day, remote)
        return jsonify({"launched": True, "reset": n if n >= 0 else "all", **res})
    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("redeliver failed")
        return jsonify({"error": str(e)}), 500


# ── File-level actions ───────────────────────────────────────────────────────
@bp.post("/action/<action_type>")
def action(action_type: str):
    if not auth.can_operate():
        return _forbidden()
    fn = actions.ACTIONS.get(action_type)
    if fn is None:
        return jsonify({"error": f"unknown action '{action_type}'"}), 404
    body = request.get_json(silent=True) or {}
    day = (body.get("day_id") or "").strip()
    name = (body.get("filename") or "").strip()
    if not valid_name(day) or not valid_name(name):
        return jsonify({"error": "valid day_id and filename required"}), 400
    try:
        msg = fn(day, name)
        return jsonify({"done": True, "message": msg})
    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("action failed")
        return jsonify({"error": str(e)}), 500
