"""Annotation gate: is the batch's check sample fully annotated?

Gate state comes from the volumes (the pipeline's own source of truth):
  n_sampled  = PDFs in validation/{day_id}/
  n_annotated = GT JSONs in ground_truth/{day_id}/ matching those PDFs
Metrics come from evaluation_results once files are annotated.
"""
from __future__ import annotations

from . import queries, volumes


def gate_state(day_id: str) -> dict:
    sampled = volumes.validation_pdfs(day_id)
    annotated = volumes.gt_jsons(day_id)
    missing = [f for f in sampled if f not in annotated]
    complete = len(sampled) > 0 and not missing

    state = {
        "day_id": day_id,
        "n_sampled": len(sampled),
        "n_annotated": len(sampled) - len(missing),
        "missing": missing,
        "complete": complete,
        "metrics": None,
    }
    if complete or (len(sampled) - len(missing)) > 0:
        state["metrics"] = queries.gate_metrics(day_id)
    return state
