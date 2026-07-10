"""Ground Truth App — Flask backend.

Routes
  GET  /                      annotation UI
  GET  /dashboard            evaluation dashboard
  GET  /api/worklist         pending / completed PDFs from check/
  GET  /api/file/<name>      page count + existing GT (operator-blind: NO model)
  GET  /api/page/<name>/<n>  server-rendered JPEG of one PDF page
  GET  /api/stats            aggregate metrics for the dashboard
  POST /api/save             persist GT JSON + run eval -> evaluation_results
  GET  /api/health           liveness probe
"""
from __future__ import annotations

import re

from flask import Flask, Response, jsonify, render_template, request

from src.config import config
from src import annotation

app = Flask(__name__)

# Filenames are PDF stems: letters, digits, _, -, . — reject anything else to
# keep them safe inside SQL string literals and volume paths.
_SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]+$")


def _annotator() -> str:
    return request.headers.get(config.USER_HEADER, config.DEFAULT_ANNOTATOR)


def _valid_name(name: str) -> bool:
    return bool(name) and bool(_SAFE_NAME.match(name)) and ".." not in name


# ─────────────────────────────────────────────────────────────────────────────
@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/api/worklist")
def worklist():
    try:
        return jsonify(annotation.build_worklist())
    except Exception as e:  # noqa: BLE001
        app.logger.exception("worklist failed")
        return jsonify({"error": str(e)}), 500


@app.get("/api/file/<name>")
def file_detail(name: str):
    if not _valid_name(name):
        return jsonify({"error": "invalid filename"}), 400
    try:
        # Operator-blind: never return the model prediction. total_pages comes from
        # the PDF itself; existing GT (if any) is returned for re-annotation.
        gt = annotation.load_ground_truth(name)
        return jsonify({
            "filename": name,
            "total_pages": annotation.page_count(name),
            "folder_id": name.split("_")[0],
            "ground_truth": gt,
        })
    except Exception as e:  # noqa: BLE001
        app.logger.exception("file_detail failed")
        return jsonify({"error": str(e)}), 500


@app.get("/api/page/<name>/<int:n>")
def page_image(name: str, n: int):
    if not _valid_name(name):
        return jsonify({"error": "invalid filename"}), 400
    try:
        if n < 1 or n > annotation.page_count(name):
            return jsonify({"error": "page out of range"}), 404
        jpeg = annotation.render_page_jpeg(name, n)
        resp = Response(jpeg, mimetype="image/jpeg")
        resp.headers["Cache-Control"] = "public, max-age=3600"
        return resp
    except Exception as e:  # noqa: BLE001
        app.logger.exception("page_image failed")
        return jsonify({"error": str(e)}), 500


@app.get("/api/stats")
def stats():
    try:
        return jsonify(annotation.get_eval_stats())
    except Exception as e:  # noqa: BLE001
        app.logger.exception("stats failed")
        return jsonify({"error": str(e)}), 500


@app.get("/dashboard")
def dashboard():
    return render_template("dashboard.html")


@app.post("/api/save")
def save():
    body = request.get_json(silent=True) or {}
    name = body.get("filename", "")
    if not _valid_name(name):
        return jsonify({"error": "invalid filename"}), 400

    starts = body.get("predicted_starts")
    is_multidoc = bool(body.get("is_multidoc", False))
    if not isinstance(starts, list) or not starts:
        return jsonify({"error": "predicted_starts must be a non-empty list"}), 400

    try:
        model = annotation.get_model_prediction(name)

        # total_pages / folder_id: trust the request (from the loaded PDF), fall
        # back to the model row when available.
        total_pages = int(body.get("total_pages") or (model or {}).get("total_pages") or 0)
        if total_pages <= 0:
            return jsonify({"error": "total_pages missing or invalid"}), 400
        folder_id = body.get("folder_id") or (model or {}).get("folder_id")

        payload = annotation.build_gt_payload(
            filename=name,
            folder_id=folder_id,
            total_pages=total_pages,
            gt_starts=[int(x) for x in starts],
            is_multidoc=is_multidoc,
            annotator=_annotator(),
        )
        gt_path = annotation.save_ground_truth(payload)
        metrics = annotation.run_and_store_evaluation(payload, model)

        return jsonify({
            "saved": True,
            "ground_truth_path": gt_path,
            "ground_truth": payload,
            "evaluation": metrics,
        })
    except Exception as e:  # noqa: BLE001
        app.logger.exception("save failed")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
