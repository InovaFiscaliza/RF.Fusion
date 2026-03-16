"""
Small shared helpers with no database or transport ownership.

The functions in this module are intentionally narrow and stateless so they can
be reused by workers, maintenance scripts, and database handlers alike.
"""

from __future__ import annotations

import os
import sys
from typing import Optional
from datetime import datetime

# ---------------------------------------------------------------------
# Ensure config import path (same pattern used in legacy / host_context)
# ---------------------------------------------------------------------
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)

CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402


def compose_message(
    task_type: int,
    task_status: int,
    path: Optional[str] = None,
    name: Optional[str] = None,
    *,
    error: Optional[str] = None,
    detail: Optional[str] = None,
    prefix_only: bool = False
) -> str:
    """
    Build a standardized task-history message for audit fields.

    Rules:
    - Messages describe task state transitions deterministically
    - File references are normalized as `file=<path/name>`
    - Extra details and errors are appended only if explicitly provided
    - This function NEVER inspects ErrorHandler directly

    Args:
        task_type (int):
            FILE_TASK_* constant (BACKUP, DISCOVERY, PROCESS)

        task_status (int):
            TASK_* constant (PENDING, RUNNING, DONE, ERROR)

        path (Optional[str]):
            Final file path (only for successful processing)

        name (Optional[str]):
            Final file name (only for successful processing)

        error (Optional[str]):
            Pre-formatted error message (e.g., ErrorHandler.format_error()).
            If provided, it is appended to the message.

        detail (Optional[str]):
            Optional free-form contextual detail appended after the base
            state description and before any explicit error payload.

        prefix_only (bool):
            If True, return only "<Type> <Status>" without details.

    Returns:
        str: Deterministic, audit-friendly message.
    """

    task_type_map = {
        k.FILE_TASK_BACKUP_TYPE: "Backup",
        k.FILE_TASK_DISCOVERY: "Discovery",
        k.FILE_TASK_PROCESS_TYPE: "Processing",
    }
    status_map = {
        k.TASK_PENDING: "Pending",
        k.TASK_DONE: "Done",
        k.TASK_RUNNING: "Running",
        k.TASK_ERROR: "Error",
    }

    type_msg = task_type_map.get(task_type, f"TaskType-{task_type}")
    status_msg = status_map.get(task_status, f"Status-{task_status}")

    prefix = f"{type_msg} {status_msg}"

    if prefix_only:
        return prefix

    parts = [prefix]

    normalized_path = path.strip() if isinstance(path, str) else path
    normalized_name = name.strip() if isinstance(name, str) else name

    if normalized_path and normalized_name:
        parts.append(f"file={normalized_path}/{normalized_name}")
    elif normalized_name:
        parts.append(f"file={normalized_name}")
    elif normalized_path:
        parts.append(f"path={normalized_path}")

    if detail:
        parts.append(detail)

    if error:
        parts.append(error)

    return " | ".join(parts)

def parse_ps_iso(ts: str) -> datetime:
    """
    Parse a PowerShell ISO timestamp into a naive `datetime`.

    PowerShell emits up to 7 fractional digits (ticks, 100ns),
    which Python does not accept. This function:
        • truncates to microseconds (6 digits)
        • removes timezone information
    """
    if "." in ts:
        head, tail = ts.split(".", 1)

        # tail example: "4475322-03:00"
        frac = tail[:6]  # microseconds
        rest = tail[6:]

        # remove timezone
        if "+" in rest:
            rest = rest.split("+", 1)[0]
        elif "-" in rest:
            rest = rest.split("-", 1)[0]

        ts = f"{head}.{frac}"

    else:
        ts = ts.split("+", 1)[0].split("-", 1)[0]

    return datetime.fromisoformat(ts)

def pid_exists(pid: int) -> bool:
    """Return True when a PID exists from the current process perspective."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
