"""Caller identity and the operator gate.

One place decides who is asking and what they may do. Today every caller may
operate the pipeline; flipping that on is a one-line change here plus an
OPERATOR_EMAILS value in app.yaml — no route or template changes.
"""
from __future__ import annotations

from flask import request

from .config import config


def current_user() -> str:
    """The caller's email. Databricks Apps injects it as X-Forwarded-Email;
    locally the header is absent and we fall back to the configured default."""
    header = (request.headers.get(config.USER_HEADER) or "").strip().lower()
    return header or config.DEFAULT_ANNOTATOR


def actor() -> str:
    """Who to record in pipeline_events and in ground-truth JSON."""
    return current_user()


def can_operate() -> bool:
    """May this caller launch jobs and mutate pipeline state?

    Today: everyone. To restrict, set OPERATOR_EMAILS in app.yaml and change
    the body to:

        return (not config.OPERATOR_EMAILS) or current_user() in config.OPERATOR_EMAILS

    Annotation is deliberately NOT gated by this — annotators are the wide
    audience, operators the narrow one.
    """
    return True
