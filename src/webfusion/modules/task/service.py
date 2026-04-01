"""Task-creation helpers for the WebFusion task builder."""

from __future__ import annotations

import json
from typing import Any

from .builder import build_filter


HOST_TASK_CHECK_TYPE = 1
HOST_TASK_PROCESSING_TYPE = 2
HOST_TASK_UPDATE_STATISTICS_TYPE = 3
HOST_TASK_CHECK_CONNECTION_TYPE = 4
HOST_TASK_BACKLOG_CONTROL_TYPE = 5
HOST_TASK_BACKLOG_ROLLBACK_TYPE = 6

TASK_ERROR = -1
TASK_DONE = 0
TASK_PENDING = 1
TASK_RUNNING = 2
TASK_SUSPENDED = 3

EXPOSED_TASK_TYPES = {
    HOST_TASK_CHECK_TYPE: "Solicitar Backup",
    HOST_TASK_BACKLOG_ROLLBACK_TYPE: "Retirar da Fila de Backup",
}

def _serialize_filter(filter_value: Any) -> str:
    """Return a deterministic JSON representation for HOST_TASK.FILTER."""
    if isinstance(filter_value, str):
        try:
            filter_value = json.loads(filter_value)
        except json.JSONDecodeError:
            return filter_value

    return json.dumps(filter_value, sort_keys=True, ensure_ascii=False)


def _select_candidate_host_tasks(db, host_id, task_type):
    """Load reusable HOST_TASK candidates for the requested exact type."""
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT ID_HOST_TASK, NU_TYPE, NU_STATUS, FILTER
        FROM HOST_TASK
        WHERE FK_HOST = %s
          AND NU_TYPE = %s
        ORDER BY DT_HOST_TASK DESC, ID_HOST_TASK DESC
        """,
        (host_id, task_type),
    )

    return cursor.fetchall() or []


def _find_reusable_singleton_host_task(tasks):
    """Mirror appCataloga's singleton-per-host-type reuse rule."""
    pending = next(
        (task for task in tasks if task.get("NU_STATUS") == TASK_PENDING),
        None,
    )
    if pending:
        return pending

    running = next(
        (task for task in tasks if task.get("NU_STATUS") == TASK_RUNNING),
        None,
    )
    if running:
        return running

    return tasks[0] if tasks else None


def _refresh_host_task(db, task_id, task_type, filter_dict, message):
    """Refresh an existing HOST_TASK row in place."""
    cursor = db.cursor()
    cursor.execute(
        """
        UPDATE HOST_TASK
        SET NU_TYPE = %s,
            NU_STATUS = %s,
            DT_HOST_TASK = NOW(),
            FILTER = %s,
            NA_MESSAGE = %s
        WHERE ID_HOST_TASK = %s
        """,
        (
            task_type,
            TASK_PENDING,
            _serialize_filter(filter_dict),
            f"Refreshed by WebFusion | {message}",
            task_id,
        ),
    )
    db.commit()


def _create_host_task(db, host_id, task_type, filter_dict, message):
    """Insert a new HOST_TASK row."""
    cursor = db.cursor()
    cursor.execute(
        """
        INSERT INTO HOST_TASK
        (FK_HOST, NU_TYPE, DT_HOST_TASK, NU_STATUS, FILTER, NA_MESSAGE)
        VALUES (%s, %s, NOW(), %s, %s, %s)
        """,
        (
            host_id,
            task_type,
            TASK_PENDING,
            _serialize_filter(filter_dict),
            message,
        ),
    )
    db.commit()


def queue_host_task_safe(db, host_id, task_type, filter_dict, message):
    """
    Create or refresh a HOST_TASK without creating duplicate logical work.

    WebFusion now mirrors appCataloga's durable HOST_TASK contract:
        - one reusable row per `FK_HOST + NU_TYPE`
        - PENDING rows are refreshed in place
        - RUNNING rows are preserved
        - terminal rows are reactivated instead of creating infinite inserts
    """

    tasks = _select_candidate_host_tasks(db, host_id, task_type)
    existing = _find_reusable_singleton_host_task(tasks)

    if existing:
        status = existing["NU_STATUS"]
        task_id = existing["ID_HOST_TASK"]

        if status == TASK_RUNNING:
            return "skipped_active"

        _refresh_host_task(
            db=db,
            task_id=task_id,
            task_type=task_type,
            filter_dict=filter_dict,
            message=message,
        )
        return "refreshed"

    _create_host_task(
        db=db,
        host_id=host_id,
        task_type=task_type,
        filter_dict=filter_dict,
        message=message,
    )
    return "created"


def _build_task_action_name(task_type, mode):
    """Return the human-facing action label stored in HOST_TASK.NA_MESSAGE."""
    if task_type == HOST_TASK_CHECK_TYPE:
        return f"Host Check | Backup ({mode})"

    if task_type == HOST_TASK_BACKLOG_ROLLBACK_TYPE:
        return f"Backlog Rollback | Stop ({mode})"

    raise ValueError(f"Unsupported task type for WebFusion: {task_type}")


def create_task(db, hosts, task_type, mode, filter_data):
    """Create or refresh one or more HOST_TASK rows exposed by the UI."""

    if task_type not in EXPOSED_TASK_TYPES:
        raise ValueError("WebFusion only exposes backup and stop tasks.")

    collective = len(hosts) > 1
    queued_count = 0
    skipped_count = 0

    for host_id in hosts:
        filter_dict = build_filter(
            mode=mode,
            start_date=filter_data.get("start_date"),
            end_date=filter_data.get("end_date"),
            last_n_files=filter_data.get("last_n_files"),
            extension=filter_data.get("extension"),
            file_path=filter_data.get("file_path"),
            file_name=filter_data.get("file_name"),
        )
        action_name = _build_task_action_name(task_type, mode)

        scope = "Collective" if collective else "Individual"

        message = f"Created by WebFusion | {action_name} | {scope}"

        result = queue_host_task_safe(
            db=db,
            host_id=host_id,
            task_type=task_type,
            filter_dict=filter_dict,
            message=message
        )

        if result in ("created", "refreshed"):
            queued_count += 1
        else:
            skipped_count += 1

    return {
        "queued_count": queued_count,
        "skipped_count": skipped_count,
    }
