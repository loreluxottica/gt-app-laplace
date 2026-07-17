"""Operational actions on (day_id, filename). Every action:
  1. is fully parameterized (no user data in SQL text),
  2. writes a pipeline_events row with actor = the dashboard user,
so nothing the control tower does is ever silent or unattributed.
"""
from __future__ import annotations

import uuid

from ..core.auth import actor
from ..core.config import config
from ..core.db import get_sql

_LOG = config.fq("processing_log")
_EVENTS = config.fq("pipeline_events")
_SPLIT = config.fq("split_results")
_SIGNALS = config.fq("page_signals")
_SUMMARIES = config.fq("package_summaries")


def _log_event(sql, day_id: str, event_type: str, filename: str | None,
               detail: str | None = None):
    params = [
        sql.str_param("eid", str(uuid.uuid4())),
        sql.str_param("day", day_id),
        sql.str_param("etype", event_type),
        sql.str_param("actor", actor()),
        sql.str_param("detail", detail or ""),
    ]
    fname_sql = ":f"
    if filename is None:
        fname_sql = "NULL"
    else:
        params.append(sql.str_param("f", filename))
    sql.execute(
        f"""INSERT INTO {_EVENTS}
            (event_id, run_id, day_id, stage, event_type, filename, folder_id,
             old_status, new_status, detail, error_message, actor, event_ts)
            VALUES (:eid, NULL, :day, 'dashboard', :etype, {fname_sql}, NULL,
                    NULL, NULL, :detail, NULL, :actor, current_timestamp())""",
        parameters=params,
    )


def retry_parse(day_id: str, filename: str) -> str:
    """error → pending (picked up by the next job_ingest run)."""
    sql = get_sql()
    sql.execute(
        f"""UPDATE {_LOG}
            SET status = 'pending', error_message = NULL, error_stage = NULL,
                retry_count = retry_count + 1
            WHERE day_id = :day AND filename = :f AND status = 'error'""",
        parameters=[sql.str_param("day", day_id), sql.str_param("f", filename)],
    )
    _log_event(sql, day_id, "requeued", filename, "retry parse: error → pending")
    return "requeued for parse"


def retry_split(day_id: str, filename: str) -> str:
    """Hard re-split: drop the file's split artifacts and reset to 'parsed'."""
    sql = get_sql()
    p = [sql.str_param("day", day_id), sql.str_param("f", filename)]
    for table in (_SPLIT, _SIGNALS, _SUMMARIES):
        sql.execute(
            f"DELETE FROM {table} WHERE day_id = :day AND filename = :f",
            parameters=p,
        )
    sql.execute(
        f"""UPDATE {_LOG}
            SET status = 'parsed', error_message = NULL, error_stage = NULL,
                n_documents_found = NULL
            WHERE day_id = :day AND filename = :f AND status IN ('done', 'error')""",
        parameters=p,
    )
    _log_event(sql, day_id, "requeued", filename,
               "hard re-split: artifacts deleted, status → parsed")
    return "requeued for split (artifacts deleted)"


def retry_sftp(day_id: str, filename: str) -> str:
    """failed/deferred → pending (picked up by the next job_deliver run)."""
    sql = get_sql()
    sql.execute(
        f"""UPDATE {_LOG}
            SET sftp_delivery_status = 'pending', sftp_delivery_error = NULL
            WHERE day_id = :day AND filename = :f
              AND sftp_delivery_status IN ('failed', 'deferred')""",
        parameters=[sql.str_param("day", day_id), sql.str_param("f", filename)],
    )
    _log_event(sql, day_id, "requeued", filename, "retry sftp: → pending")
    return "requeued for sftp"


def mark_manual(day_id: str, filename: str) -> str:
    """Terminal state: handled outside the pipeline."""
    sql = get_sql()
    sql.execute(
        f"""UPDATE {_LOG}
            SET status = 'manual'
            WHERE day_id = :day AND filename = :f""",
        parameters=[sql.str_param("day", day_id), sql.str_param("f", filename)],
    )
    _log_event(sql, day_id, "marked_manual", filename, "handled manually")
    return "marked manual"


def approve_review(day_id: str, filename: str) -> str:
    """Approve an unsplit [1]-fallback for delivery as-is."""
    sql = get_sql()
    sql.execute(
        f"""UPDATE {_SPLIT}
            SET needs_review = false
            WHERE day_id = :day AND filename = :f""",
        parameters=[sql.str_param("day", day_id), sql.str_param("f", filename)],
    )
    _log_event(sql, day_id, "review_approved", filename,
               "unsplit delivery approved by reviewer")
    return "review approved"


def reset_deferred(day_id: str, filenames: list[str] | None) -> int:
    """deferred → pending, optionally only a subset. Returns rows targeted."""
    sql = get_sql()
    if filenames:
        for f in filenames:
            sql.execute(
                f"""UPDATE {_LOG}
                    SET sftp_delivery_status = 'pending', sftp_delivery_error = NULL
                    WHERE day_id = :day AND filename = :f
                      AND sftp_delivery_status = 'deferred'""",
                parameters=[sql.str_param("day", day_id), sql.str_param("f", f)],
            )
        n = len(filenames)
    else:
        sql.execute(
            f"""UPDATE {_LOG}
                SET sftp_delivery_status = 'pending', sftp_delivery_error = NULL
                WHERE day_id = :day AND sftp_delivery_status = 'deferred'""",
            parameters=[sql.str_param("day", day_id)],
        )
        n = -1  # all
    _log_event(sql, day_id, "requeued", None,
               f"re-deliver deferred: {n if n >= 0 else 'all'} files → pending")
    return n


ACTIONS = {
    "retry-parse": retry_parse,
    "retry-split": retry_split,
    "retry-sftp": retry_sftp,
    "mark-manual": mark_manual,
    "approve-review": approve_review,
}
