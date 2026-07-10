"""Boundary evaluation: human ground-truth vs model predicted_starts.

A "boundary" is the first page of a new document. Page 1 is always a boundary in
both GT and model output, so it is excluded from precision/recall/F1 — otherwise
every single-document PDF would score a perfect, meaningless 1.0. `exact_match`,
however, compares the FULL sets (page 1 included) for a strict identity check.

Two metric families are produced:
  - exact:   a predicted boundary is correct only if its page number is in GT.
  - tolerant (off-by-1): a predicted boundary counts if it is within +/-1 page of
    an unused GT boundary (greedy nearest match). `n_offby1` reports how many of
    those tolerant matches were NOT exact (i.e. genuine off-by-one errors).
"""
from __future__ import annotations

from dataclasses import dataclass, asdict


@dataclass
class EvalResult:
    exact_match: bool
    # exact
    n_true_positive: int
    n_false_positive: int
    n_false_negative: int
    precision: float
    recall: float
    f1: float
    # tolerant (+/-1 page)
    n_offby1: int
    precision_tol: float
    recall_tol: float
    f1_tol: float
    # multidoc classification
    gt_is_multidoc: bool
    model_is_multidoc: bool
    multidoc_correct: bool

    def as_dict(self) -> dict:
        return asdict(self)


def _f1(precision: float, recall: float) -> float:
    if precision + recall == 0:
        return 0.0
    return 2 * precision * recall / (precision + recall)


def _safe_div(num: int, den: int) -> float:
    return num / den if den else 0.0


def evaluate(gt_starts: list[int], model_starts: list[int]) -> EvalResult:
    """Compare two sorted boundary lists. Inputs are normalized defensively."""
    gt_full = sorted(set(int(x) for x in gt_starts))
    pred_full = sorted(set(int(x) for x in model_starts))

    exact_match = gt_full == pred_full

    # Non-trivial boundaries only (drop page 1).
    gt = [p for p in gt_full if p != 1]
    pred = [p for p in pred_full if p != 1]
    gt_set = set(gt)
    pred_set = set(pred)

    # ── exact ──────────────────────────────────────────────────────────────
    tp = len(gt_set & pred_set)
    fp = len(pred_set - gt_set)
    fn = len(gt_set - pred_set)
    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    f1 = _f1(precision, recall)

    # ── tolerant (+/-1, greedy nearest) ────────────────────────────────────
    tp_tol, n_offby1 = _tolerant_matches(gt, pred)
    fp_tol = len(pred) - tp_tol
    fn_tol = len(gt) - tp_tol
    precision_tol = _safe_div(tp_tol, tp_tol + fp_tol)
    recall_tol = _safe_div(tp_tol, tp_tol + fn_tol)
    f1_tol = _f1(precision_tol, recall_tol)

    # ── multidoc classification ────────────────────────────────────────────
    gt_multi = len(gt_full) > 1
    model_multi = len(pred_full) > 1

    return EvalResult(
        exact_match=exact_match,
        n_true_positive=tp,
        n_false_positive=fp,
        n_false_negative=fn,
        precision=round(precision, 4),
        recall=round(recall, 4),
        f1=round(f1, 4),
        n_offby1=n_offby1,
        precision_tol=round(precision_tol, 4),
        recall_tol=round(recall_tol, 4),
        f1_tol=round(f1_tol, 4),
        gt_is_multidoc=gt_multi,
        model_is_multidoc=model_multi,
        multidoc_correct=(gt_multi == model_multi),
    )


def aggregate_stats(rows: list[dict]) -> dict:
    """Aggregate evaluation rows (as stored in evaluation_results) into dashboard
    totals. Rows without a model prediction are counted in totals but excluded
    from the averaged boundary metrics. Pure: works the same for the local JSONL
    and the production table."""
    n = len(rows)
    with_model = [r for r in rows if r.get("model_starts") is not None
                  and r.get("exact_match") is not None]
    nm = len(with_model)

    def _avg(key):
        vals = [r[key] for r in with_model if r.get(key) is not None]
        return round(sum(vals) / len(vals), 4) if vals else 0.0

    n_exact = sum(1 for r in with_model if r.get("exact_match"))
    n_multi_ok = sum(1 for r in with_model if r.get("multidoc_correct"))
    n_gt_multi = sum(1 for r in rows if r.get("gt_is_multidoc"))

    return {
        "n_annotated": n,
        "n_with_model": nm,
        "n_no_model": n - nm,
        "n_exact": n_exact,
        "exact_rate": round(n_exact / nm, 4) if nm else 0.0,
        "n_multidoc_correct": n_multi_ok,
        "multidoc_rate": round(n_multi_ok / nm, 4) if nm else 0.0,
        "n_offby1_total": sum(int(r.get("n_offby1") or 0) for r in with_model),
        "avg_precision": _avg("precision"),
        "avg_recall": _avg("recall"),
        "avg_f1": _avg("f1"),
        "avg_f1_tol": _avg("f1_tol"),
        "n_gt_multidoc": n_gt_multi,
        "n_gt_single": n - n_gt_multi,
    }


def aggregate_stats_grouped(rows: list[dict]) -> dict:
    """Aggregate stats overall and split by the human's `gt_is_multidoc` label, so
    the dashboard can judge the model on multi-doc vs single-doc populations."""
    return {
        "all": aggregate_stats(rows),
        "multidoc": aggregate_stats([r for r in rows if r.get("gt_is_multidoc")]),
        "single": aggregate_stats([r for r in rows if not r.get("gt_is_multidoc")]),
    }


def _tolerant_matches(gt: list[int], pred: list[int]) -> tuple[int, int]:
    """Greedy 1-to-1 match where |gt - pred| <= 1. Each boundary used once.

    Returns (n_matched, n_offby1) where n_offby1 counts matches at distance 1.
    """
    used_pred = [False] * len(pred)
    matched = 0
    offby1 = 0
    for g in gt:
        best_j = -1
        best_dist = 2  # only distances 0 or 1 are acceptable
        for j, p in enumerate(pred):
            if used_pred[j]:
                continue
            dist = abs(g - p)
            if dist < best_dist:
                best_dist = dist
                best_j = j
                if dist == 0:
                    break
        if best_j >= 0:
            used_pred[best_j] = True
            matched += 1
            if best_dist == 1:
                offby1 += 1
    return matched, offby1
