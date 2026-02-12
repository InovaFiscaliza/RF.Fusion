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


# ---------------------------------------------------------------------
# Function tools
# ---------------------------------------------------------------------
def compose_message(
    task_type: int,
    task_status: int,
    path: Optional[str] = None,
    name: Optional[str] = None,
    *,
    error: Optional[str] = None,
    prefix_only: bool = False
) -> str:
    """
    Build a standardized NA_MESSAGE for FILE_TASK_HISTORY.

    Rules:
    - Messages describe task state transitions deterministically
    - Error details are appended only if explicitly provided
    - Path/name are optional and used only when relevant
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

        prefix_only (bool):
            If True, return only "<Type> <Status>" without details.

    Returns:
        str: Deterministic, audit-friendly message.
    """

    # -------------------------------------------------
    # Task type
    # -------------------------------------------------
    if task_type == k.FILE_TASK_BACKUP_TYPE:
        type_msg = "Backup"
    elif task_type == k.FILE_TASK_DISCOVERY:
        type_msg = "Discovery"
    else:
        type_msg = "Processing"

    # -------------------------------------------------
    # Status
    # -------------------------------------------------
    if task_status == k.TASK_PENDING:
        status_msg = "Pending"
    elif task_status == k.TASK_DONE:
        status_msg = "Done"
    elif task_status == k.TASK_RUNNING:
        status_msg = "Running"
    elif task_status == k.TASK_ERROR:
        status_msg = "Error"
    else:
        status_msg = f"Status-{task_status}"

    prefix = f"{type_msg} {status_msg}"

    if prefix_only:
        return prefix

    # -------------------------------------------------
    # Base message (state only)
    # -------------------------------------------------
    if path and name:
        message = f"{prefix} of file {path}/{name}"
    else:
        message = prefix

    # -------------------------------------------------
    # Optional error enrichment
    # -------------------------------------------------
    if error:
        message = f"{message} | {error}"

    return message

def parse_ps_iso(ts: str) -> datetime:
    """
    Parse PowerShell ISO timestamp into naive local datetime.

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


