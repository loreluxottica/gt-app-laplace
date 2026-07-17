"""Request-validation helpers shared by both blueprints.

Filenames are PDF stems and day_ids are batch folder names: letters, digits,
_, -, . — reject anything else to keep them safe inside SQL string literals
and volume paths.
"""
from __future__ import annotations

import re

from flask import request

_SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]+$")


def valid_name(name: str) -> bool:
    return bool(name) and bool(_SAFE_NAME.match(name)) and ".." not in name


def day_id_arg() -> str | None:
    """Validated day_id from the query string, or None."""
    day = request.args.get("day_id", "").strip()
    return day if valid_name(day) else None


def optional_day_arg() -> tuple[str | None, bool]:
    """For routes where day_id is optional: (day, ok).

    ok is False only when a day_id was supplied AND is invalid — an absent
    day_id is legitimate and yields (None, True).
    """
    raw = request.args.get("day_id", "").strip()
    if not raw:
        return None, True
    return (raw, True) if valid_name(raw) else (None, False)
