"""Task-creation helpers for the WebFusion task builder.

The route layer handles HTTP and form semantics; this module handles the queue
contract. In practice that means turning normalized form input into durable
`HOST_TASK` rows without creating endless duplicate logical work.
"""

from __future__ import annotations

from typing import Any

from psycopg import Connection

from .builder import build_filter
from modules.tasks.config import (
    HOST_TASK_CHECK_TYPE,
    HOST_TASK_PROCESSING_TYPE,
    HOST_TASK_UPDATE_STATISTICS_TYPE,
    HOST_TASK_CHECK_CONNECTION_TYPE,
    HOST_TASK_BACKLOG_CONTROL_TYPE,
    HOST_TASK_BACKLOG_ROLLBACK_TYPE,
    HOST_TASK_INTERACTIVE_CHECK_TYPE,
    TASK_ERROR,
    TASK_DONE,
    TASK_PENDING,
    TASK_RUNNING,
    EXPOSED_TASK_TYPES,
    STATION_TASK_SUSPENDED as TASK_SUSPENDED,
)
from db.tasks import (
    select_candidate_host_tasks as _select_candidate_host_tasks,
    refresh_host_task as _refresh_host_task,
    create_host_task as _create_host_task,
)


def _find_reusable_singleton_host_task(tasks):
    """Mirror appCataloga's singleton-per-host-type reuse rule.

    WebFusion follows the backend rule of reusing the logical row for one
    `host + task type` pair instead of inserting a fresh row every time.
    """
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


def queue_host_task_safe(
    db: Connection[dict[str, object]],
    host_id: int,
    task_type: int,
    filter_dict: dict[str, object] | str,
    message: str,
) -> str:
    """Create or refresh a HOST_TASK without creating duplicate logical work.

    WebFusion now mirrors appCataloga's durable HOST_TASK contract:
        - one reusable row per `FK_HOST + NU_TYPE`
        - PENDING rows are refreshed in place
        - RUNNING rows are preserved
        - terminal rows are reactivated instead of creating infinite inserts

    Args:
        db (Connection[dict[str, object]]): Caller-owned operational dictionary
            connection.
        host_id (int): Operational station identifier.
        task_type (int): Task type already supported by the worker.
        filter_dict (dict[str, object] | str): Worker filter dictionary or serialized
            JSON.
        message (str): Audit message for the queued task.

    Returns:
        str: One of created, refreshed or skipped_active. Existing running tasks are
            preserved; created/refreshed writes are committed.

    Raises:
        Exception: Invalid task input or a database operation fails.
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


def queue_interactive_connectivity_test(
    db: Connection[dict[str, object]],
    host_id: int,
) -> dict[str, int | bool]:
    """Create or reuse one high-priority manual connectivity test per host.

    Args:
        db (Connection[dict[str, object]]): Caller-owned operational dictionary
            connection.
        host_id (int): Operational station identifier.

    Returns:
        dict[str, int | bool]: Required task_id (int), created (bool) and active (bool),
            identifying the existing or newly committed interactive task.

    Raises:
        Exception: Invalid task input or a database operation fails.
    """
    tasks = _select_candidate_host_tasks(
        db,
        host_id,
        HOST_TASK_INTERACTIVE_CHECK_TYPE,
    )
    existing = _find_reusable_singleton_host_task(tasks)
    message = (
        "Teste de estação solicitado pelo WebFusion. "
        "Aguardando o verificador de conectividade."
    )

    if existing:
        task_id = int(existing["ID_HOST_TASK"])
        if existing["NU_STATUS"] == TASK_RUNNING:
            return {"task_id": task_id, "created": False, "active": True}

        _refresh_host_task(
            db=db,
            task_id=task_id,
            task_type=HOST_TASK_INTERACTIVE_CHECK_TYPE,
            filter_dict={},
            message=message,
        )
        return {"task_id": task_id, "created": False, "active": False}

    task_id = _create_host_task(
        db=db,
        host_id=host_id,
        task_type=HOST_TASK_INTERACTIVE_CHECK_TYPE,
        filter_dict={},
        message=message,
    )
    return {"task_id": int(task_id), "created": True, "active": False}


def _build_task_action_name(task_type, mode):
    """Return the human-facing action label stored in HOST_TASK.NA_MESSAGE."""
    if task_type == HOST_TASK_CHECK_TYPE:
        return f"Host Check | Backup ({mode})"

    if task_type == HOST_TASK_BACKLOG_ROLLBACK_TYPE:
        return f"Backlog Rollback | Stop ({mode})"

    raise ValueError(f"Unsupported task type for WebFusion: {task_type}")


def create_task(
    db: Connection[dict[str, object]],
    hosts: list[int],
    task_type: int,
    mode: str,
    filter_data: dict[str, object],
) -> dict[str, int]:
    """Create or refresh one or more HOST_TASK rows exposed by the UI.

    A single browser submission can fan out into multiple host-specific queue
    updates, especially for collective execution.

    Args:
        db (Connection[dict[str, object]]): Caller-owned operational dictionary
            connection.
        hosts (list[int]): Operational station identifiers selected by the operator.
        task_type (int): Task type already supported by the worker.
        mode (str): Validated worker filter mode.
        filter_data (dict[str, object]): Optional start_date/end_date, last_n_files,
            extension, file_path/file_name, max_total_gb and sort_order values used by
            build_filter.

    Returns:
        dict[str, int]: Required queued_count and skipped_count integers across the
            selected hosts. Each station retains its existing commit boundary.

    Raises:
        Exception: Invalid task input or a database operation fails.
    """

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
            max_total_gb=filter_data.get("max_total_gb"),
            sort_order=filter_data.get("sort_order"),
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
