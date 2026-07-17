"""Repository layer: worklist, model predictions, ground-truth persistence, eval.

Ties together the SQL warehouse (tables), the Files API (volumes), and the
evaluation logic into the operations the Flask routes call.
"""
from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timezone

import fitz  # PyMuPDF — server-side page rendering

from ..core.config import config
from ..core.db import get_sql
from ..core.volumes import get_volumes
from .evaluation import evaluate, aggregate_stats_grouped


# ─────────────────────────────────────────────────────────────────────────────
#  Batches (day_id) + Worklist
# ─────────────────────────────────────────────────────────────────────────────
def list_days() -> list[dict]:
    """day_id batches = dated subfolders of validation/, with annotation progress."""
    vols = get_volumes()
    days = []
    for day_id in vols.list_subdirs(config.validation_path()):
        n_total = len(vols.list_pdfs(config.validation_path(day_id)))
        n_done = len(vols.list_json_stems(config.ground_truth_path(day_id)))
        days.append({"day_id": day_id, "n_total": n_total, "n_completed": n_done})
    return days


def build_worklist(day_id: str) -> dict:
    """PDFs in validation/{day_id}/ split into pending (no GT yet) and done (GT exists)."""
    vols = get_volumes()
    validation_pdfs = vols.list_pdfs(config.validation_path(day_id))
    done = vols.list_json_stems(config.ground_truth_path(day_id))

    pending = [f for f in validation_pdfs if f not in done]
    completed = [f for f in validation_pdfs if f in done]
    return {
        "day_id": day_id,
        "pending": pending,
        "completed": completed,
        "n_total": len(validation_pdfs),
        "n_pending": len(pending),
        "n_completed": len(completed),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Model prediction (split_results)
# ─────────────────────────────────────────────────────────────────────────────
def get_model_prediction(filename: str, day_id: str | None = None) -> dict | None:
    """Latest split_results row for (day_id, filename). Same filename may exist
    in several day batches — day_id disambiguates; None falls back to latest."""
    sql = get_sql()
    day_filter = "AND day_id = :day" if day_id else ""
    params = [sql.str_param("fname", filename)]
    if day_id:
        params.append(sql.str_param("day", day_id))
    rows = sql.execute(
        f"""
        SELECT filename, folder_id, total_pages, predicted_starts,
               n_documents, model_used
        FROM {config.fq_table(config.TABLE_SPLIT_RESULTS)}
        WHERE filename = :fname {day_filter}
        ORDER BY processing_timestamp DESC
        LIMIT 1
        """,
        parameters=params,
    )
    if not rows:
        return None
    r = rows[0]
    return {
        "filename": r["filename"],
        "folder_id": r.get("folder_id"),
        "total_pages": _to_int(r.get("total_pages")),
        "predicted_starts": _parse_int_array(r.get("predicted_starts")),
        "n_documents": _to_int(r.get("n_documents")),
        "model_used": r.get("model_used"),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Server-side PDF rendering (download once, render pages on demand)
# ─────────────────────────────────────────────────────────────────────────────
PAGE_ZOOM = 1.6          # ~115 DPI
JPEG_QUALITY = 80
_DOC_CACHE: "OrderedDict[str, fitz.Document]" = OrderedDict()
_DOC_CACHE_MAX = 4       # keep the last few PDFs open (bytes can be large)


def _get_doc(day_id: str, filename: str) -> fitz.Document:
    key = f"{day_id}/{filename}"
    if key in _DOC_CACHE:
        _DOC_CACHE.move_to_end(key)
        return _DOC_CACHE[key]
    vols = get_volumes()
    data = vols.download_bytes(vols.pdf_path(config.validation_path(day_id), filename))
    doc = fitz.open(stream=data, filetype="pdf")
    _DOC_CACHE[key] = doc
    if len(_DOC_CACHE) > _DOC_CACHE_MAX:
        _, old = _DOC_CACHE.popitem(last=False)
        try:
            old.close()
        except Exception:
            pass
    return doc


def page_count(day_id: str, filename: str) -> int:
    return _get_doc(day_id, filename).page_count


def render_page_jpeg(day_id: str, filename: str, n: int) -> bytes:
    """Render 1-based page `n` to an RGB JPEG (csRGB avoids CMYK/colorspace glitches)."""
    page = _get_doc(day_id, filename).load_page(n - 1)
    pix = page.get_pixmap(matrix=fitz.Matrix(PAGE_ZOOM, PAGE_ZOOM),
                          colorspace=fitz.csRGB, alpha=False)
    return pix.tobytes("jpeg", jpg_quality=JPEG_QUALITY)


# ─────────────────────────────────────────────────────────────────────────────
#  Dashboard stats (evaluation_results aggregate)
# ─────────────────────────────────────────────────────────────────────────────
def get_eval_stats() -> dict:
    sql = get_sql()
    rows = sql.execute(
        f"""
        SELECT model_starts, exact_match, multidoc_correct,
               precision, recall, f1, f1_tol, n_offby1, gt_is_multidoc
        FROM {config.fq_table(config.TABLE_EVALUATION)}
        """
    )
    norm = []
    for r in rows:
        norm.append({
            "model_starts": r.get("model_starts"),  # None when no model row
            "exact_match": _to_bool(r.get("exact_match")),
            "multidoc_correct": _to_bool(r.get("multidoc_correct")),
            "precision": _to_float(r.get("precision")),
            "recall": _to_float(r.get("recall")),
            "f1": _to_float(r.get("f1")),
            "f1_tol": _to_float(r.get("f1_tol")),
            "n_offby1": _to_int(r.get("n_offby1")) or 0,
            "gt_is_multidoc": _to_bool(r.get("gt_is_multidoc")),
        })
    return aggregate_stats_grouped(norm)


# ─────────────────────────────────────────────────────────────────────────────
#  Ground truth read / build
# ─────────────────────────────────────────────────────────────────────────────
def load_ground_truth(day_id: str, filename: str) -> dict | None:
    vols = get_volumes()
    path = vols.json_path(config.ground_truth_path(day_id), filename)
    return vols.read_json(path)


def build_gt_payload(
    filename: str,
    folder_id: str | None,
    total_pages: int,
    gt_starts: list[int],
    is_multidoc: bool,
    annotator: str,
    day_id: str | None = None,
) -> dict:
    """Canonical ground-truth JSON. `documents` is derived from starts so future
    versions can attach a `type` per segment without changing the boundary model."""
    starts = sorted(set(int(p) for p in gt_starts if 1 <= int(p) <= total_pages))
    if 1 not in starts:
        starts = [1] + starts
    documents = _starts_to_documents(starts, total_pages)
    return {
        "filename": filename,
        "folder_id": folder_id,
        "day_id": day_id,
        "total_pages": total_pages,
        "is_multidoc": is_multidoc,
        "predicted_starts": starts,        # human ground-truth boundaries
        "n_documents": len(starts),
        "documents": documents,            # [{start, end, type=None}]  type reserved for v2
        "annotator": annotator,
        "annotated_at": _now_iso(),
        "schema_version": 1,
    }


def _starts_to_documents(starts: list[int], total_pages: int) -> list[dict]:
    docs = []
    for i, s in enumerate(starts):
        end = (starts[i + 1] - 1) if i + 1 < len(starts) else total_pages
        docs.append({"start": s, "end": end, "type": None})
    return docs


def save_ground_truth(payload: dict) -> str:
    vols = get_volumes()
    path = vols.json_path(config.ground_truth_path(payload.get("day_id")),
                          payload["filename"])
    vols.upload_json(path, payload)
    return path


# ─────────────────────────────────────────────────────────────────────────────
#  Evaluation + persistence
# ─────────────────────────────────────────────────────────────────────────────
def run_and_store_evaluation(gt_payload: dict, model: dict | None) -> dict:
    """Evaluate GT vs model prediction and append a row to evaluation_results.

    If no model prediction exists (file not yet split), persists the GT-side facts
    with null model metrics so the row still records that this file was annotated.
    """
    gt_starts = gt_payload["predicted_starts"]
    model_starts = model["predicted_starts"] if model else []

    ev = evaluate(gt_starts, model_starts) if model else None
    _insert_evaluation_row(gt_payload, model, ev)
    return ev.as_dict() if ev else {"model_prediction_missing": True}


def _insert_evaluation_row(gt: dict, model: dict | None, ev) -> None:
    sql = get_sql()
    table = config.fq_table(config.TABLE_EVALUATION)

    gt_starts = _int_array_literal(gt["predicted_starts"])
    model_starts = _int_array_literal(model["predicted_starts"]) if model else "NULL"
    model_n = model["n_documents"] if model else "NULL"
    model_used = _sql_str(model["model_used"]) if model and model.get("model_used") else "NULL"

    if ev:
        exact_match = "TRUE" if ev.exact_match else "FALSE"
        metrics = (
            f"{ev.n_true_positive}, {ev.n_false_positive}, {ev.n_false_negative}, "
            f"{ev.precision}, {ev.recall}, {ev.f1}, "
            f"{ev.n_offby1}, {ev.precision_tol}, {ev.recall_tol}, {ev.f1_tol}, "
            f"{'TRUE' if ev.multidoc_correct else 'FALSE'}"
        )
    else:
        exact_match = "NULL"
        metrics = "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL"

    stmt = f"""
        INSERT INTO {table} (
            filename, folder_id, day_id, total_pages,
            gt_starts, gt_n_documents, gt_is_multidoc,
            model_starts, model_n_documents, model_used,
            exact_match,
            n_true_positive, n_false_positive, n_false_negative,
            precision, recall, f1,
            n_offby1, precision_tol, recall_tol, f1_tol,
            multidoc_correct,
            annotator, annotated_at
        ) VALUES (
            {_sql_str(gt['filename'])}, {_sql_str(gt.get('folder_id'))}, {_sql_str(gt.get('day_id'))}, {gt['total_pages']},
            {gt_starts}, {gt['n_documents']}, {'TRUE' if gt['is_multidoc'] else 'FALSE'},
            {model_starts}, {model_n}, {model_used},
            {exact_match},
            {metrics},
            {_sql_str(gt['annotator'])}, current_timestamp()
        )
    """
    sql.execute(stmt)


# ─────────────────────────────────────────────────────────────────────────────
#  helpers
# ─────────────────────────────────────────────────────────────────────────────
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_int(v) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_float(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_bool(v):
    """StatementExecution returns booleans as the strings 'true'/'false'."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() == "true"


def _parse_int_array(v) -> list[int]:
    """StatementExecution returns ARRAY<INT> as a JSON-ish string like '[1,5,9]'."""
    if v is None:
        return []
    if isinstance(v, list):
        return [int(x) for x in v]
    s = str(v).strip().lstrip("[").rstrip("]")
    if not s:
        return []
    return [int(x.strip()) for x in s.split(",") if x.strip()]


def _int_array_literal(arr: list[int]) -> str:
    return "array(" + ", ".join(str(int(x)) for x in arr) + ")"


def _sql_str(v) -> str:
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"
