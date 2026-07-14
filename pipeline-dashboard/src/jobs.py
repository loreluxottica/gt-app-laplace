"""Databricks Jobs integration: launch job_ingest / job_deliver, poll run state."""
from __future__ import annotations

from .config import config
from .db import get_sql


def _jobs():
    return get_sql().workspace.jobs


def run_ingest(day_id: str, sample_pct: float) -> dict:
    if not config.JOB_INGEST_ID:
        raise RuntimeError("JOB_INGEST_ID is not set — create job_ingest and set the env var.")
    run = _jobs().run_now(
        job_id=int(config.JOB_INGEST_ID),
        job_parameters={"day_id": day_id, "sample_pct": str(sample_pct)},
    )
    return {"run_id": run.response.run_id, "job": "ingest"}


def run_deliver(day_id: str, sftp_remote_base: str) -> dict:
    if not config.JOB_DELIVER_ID:
        raise RuntimeError("JOB_DELIVER_ID is not set — create job_deliver and set the env var.")
    run = _jobs().run_now(
        job_id=int(config.JOB_DELIVER_ID),
        job_parameters={"day_id": day_id, "sftp_remote_base": sftp_remote_base},
    )
    return {"run_id": run.response.run_id, "job": "deliver"}


def active_runs() -> list[dict]:
    """Currently running/pending runs of the two pipeline jobs."""
    out = []
    for label, job_id in (("ingest", config.JOB_INGEST_ID),
                          ("deliver", config.JOB_DELIVER_ID)):
        if not job_id:
            continue
        try:
            for r in _jobs().list_runs(job_id=int(job_id), active_only=True, limit=5):
                state = r.state.life_cycle_state.value if r.state and r.state.life_cycle_state else "?"
                out.append({
                    "job": label,
                    "run_id": r.run_id,
                    "state": state,
                    "start_time": r.start_time,
                    "run_page_url": r.run_page_url,
                })
        except Exception:  # job listing must never break the poll endpoint
            continue
    return out
