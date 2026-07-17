"""Thin SELECTs over the shared SQL views (sql/views.sql). No business logic
here — the views are the single definition shared with nb_pipeline_status."""
from __future__ import annotations

from ..core.config import config
from ..core.db import get_sql


def _day_params(sql, day_id: str | None):
    if day_id:
        return "WHERE day_id = :day", [sql.str_param("day", day_id)]
    return "", []


def batch_status(day_id: str | None = None) -> list[dict]:
    sql = get_sql()
    where, params = _day_params(sql, day_id)
    return sql.execute(
        f"SELECT * FROM {config.fq('v_batch_status')} {where} ORDER BY day_id DESC",
        parameters=params,
    )


def funnel(day_id: str) -> dict | None:
    sql = get_sql()
    rows = sql.execute(
        f"SELECT * FROM {config.fq('v_funnel')} WHERE day_id = :day",
        parameters=[sql.str_param("day", day_id)],
    )
    return rows[0] if rows else None


def stuck_files(day_id: str | None = None) -> list[dict]:
    sql = get_sql()
    where, params = _day_params(sql, day_id)
    return sql.execute(
        f"""SELECT day_id, filename, folder_id, status, sftp_delivery_status,
                   sftp_delivery_error, stuck_reason, error_stage, error_message,
                   retry_count, needs_review, boundary_source, completed_at
            FROM {config.fq('v_stuck_files')} {where}
            ORDER BY day_id DESC, stuck_reason, filename""",
        parameters=params,
    )


def files(day_id: str | None, status: str | None, q: str | None) -> list[dict]:
    sql = get_sql()
    clauses, params = [], []
    if day_id:
        clauses.append("day_id = :day")
        params.append(sql.str_param("day", day_id))
    if status:
        clauses.append("(status = :st OR sftp_delivery_status = :st)")
        params.append(sql.str_param("st", status))
    if q:
        clauses.append("(filename LIKE :q OR folder_id LIKE :q)")
        params.append(sql.str_param("q", f"%{q}%"))
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return sql.execute(
        f"""SELECT day_id, filename, folder_id, status, sftp_delivery_status,
                   n_pages, n_documents, needs_review, boundary_source,
                   error_message, completed_at
            FROM {config.fq('v_file_status')} {where}
            ORDER BY filename LIMIT 500""",
        parameters=params,
    )


def file_detail(day_id: str, filename: str) -> dict:
    sql = get_sql()
    p = [sql.str_param("day", day_id), sql.str_param("f", filename)]
    status = sql.execute(
        f"SELECT * FROM {config.fq('v_file_status')} WHERE day_id = :day AND filename = :f",
        parameters=p,
    )
    events = sql.execute(
        f"""SELECT event_ts, stage, event_type, old_status, new_status,
                   detail, error_message, actor, run_id
            FROM {config.fq('pipeline_events')}
            WHERE day_id = :day AND filename = :f
            ORDER BY event_ts DESC LIMIT 100""",
        parameters=p,
    )
    llm = sql.execute(
        f"""SELECT processing_timestamp, stage, model_used, is_fallback,
                   error_message, parsed_starts,
                   LEFT(raw_response, 2000) AS raw_response
            FROM {config.fq('gcs_llm_responses')}
            WHERE day_id = :day AND filename = :f
            ORDER BY processing_timestamp DESC LIMIT 20""",
        parameters=p,
    )
    return {
        "status": status[0] if status else None,
        "events": events,
        "llm_responses": llm,
    }


def sftp_board(day_id: str | None = None) -> dict:
    sql = get_sql()
    where, params = _day_params(sql, day_id)
    folders = sql.execute(
        f"SELECT * FROM {config.fq('v_sftp_board')} {where} ORDER BY day_id DESC, folder_id",
        parameters=params,
    )
    deferred_where = ("WHERE sftp_delivery_status = 'deferred'"
                      + (" AND day_id = :day" if day_id else ""))
    deferred = sql.execute(
        f"""SELECT day_id, filename, folder_id, sftp_delivery_error
            FROM {config.fq('v_file_status')} {deferred_where}
            ORDER BY filename""",
        parameters=params,
    )
    return {"folders": folders, "deferred": deferred}


def needs_review(day_id: str | None = None) -> list[dict]:
    sql = get_sql()
    where, params = _day_params(sql, day_id)
    return sql.execute(
        f"SELECT * FROM {config.fq('v_needs_review')} {where} ORDER BY day_id DESC, filename",
        parameters=params,
    )


def recent_events(day_id: str | None = None, limit: int = 30) -> list[dict]:
    sql = get_sql()
    where, params = _day_params(sql, day_id)
    return sql.execute(
        f"SELECT * FROM {config.fq('v_events_recent')} {where} LIMIT {int(limit)}",
        parameters=params,
    )


def run_summary(day_id: str | None = None) -> list[dict]:
    sql = get_sql()
    where, params = _day_params(sql, day_id)
    return sql.execute(
        f"""SELECT * FROM {config.fq('v_run_summary')} {where}
            ORDER BY started_at DESC LIMIT 40""",
        parameters=params,
    )


def gate_metrics(day_id: str) -> dict | None:
    """Aggregate GT-vs-model metrics for the batch's annotated sample."""
    sql = get_sql()
    rows = sql.execute(
        f"""SELECT COUNT(*) AS n_evaluated,
                   AVG(CASE WHEN exact_match THEN 1.0 ELSE 0.0 END) AS exact_match_rate,
                   AVG(CASE WHEN multidoc_correct THEN 1.0 ELSE 0.0 END) AS multidoc_rate,
                   AVG(precision) AS avg_precision,
                   AVG(recall) AS avg_recall,
                   AVG(f1) AS avg_f1,
                   AVG(f1_tol) AS avg_f1_tol
            FROM {config.fq('evaluation_results')}
            WHERE day_id = :day AND model_starts IS NOT NULL""",
        parameters=[sql.str_param("day", day_id)],
    )
    return rows[0] if rows else None
