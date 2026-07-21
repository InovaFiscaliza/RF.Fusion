"""Operational maintenance helpers for manual queue intervention.

This module intentionally stays small and conservative. It exposes only the
state transitions that the current appCataloga runtime already understands, so
operators can recover queue rows without inventing a parallel lifecycle from
WebFusion.
"""

from __future__ import annotations

from typing import Any


QUEUE_HOST_TASK = "host"
QUEUE_FILE_TASK = "file"

ACTION_RESTART = "restart"
ACTION_SUSPEND = "suspend"
ACTION_RECREATE_BACKUP = "recreate_backup"
ACTION_RECREATE_PROCESS = "recreate_process"

TASK_ERROR = -1
TASK_SUSPENDED = -2
TASK_FROZEN = -3
TASK_DONE = 0
TASK_PENDING = 1
TASK_RUNNING = 2

HOST_TASK_CHECK_TYPE = 1
HOST_TASK_PROCESSING_TYPE = 2
HOST_TASK_UPDATE_STATISTICS_TYPE = 3
HOST_TASK_CHECK_CONNECTION_TYPE = 4
HOST_TASK_BACKLOG_CONTROL_TYPE = 5
HOST_TASK_BACKLOG_ROLLBACK_TYPE = 6

FILE_TASK_BACKUP_TYPE = 1
FILE_TASK_PROCESS_TYPE = 2

HOST_DEPENDENT_HOST_TASK_TYPES = {
    HOST_TASK_CHECK_TYPE,
    HOST_TASK_PROCESSING_TYPE,
    HOST_TASK_CHECK_CONNECTION_TYPE,
}
SUSPENDABLE_HOST_TASK_TYPES = set(HOST_DEPENDENT_HOST_TASK_TYPES)
SUSPENDABLE_FILE_TASK_TYPES = {FILE_TASK_BACKUP_TYPE}

QUEUE_OPTIONS = {
    QUEUE_HOST_TASK: "HOST_TASK",
    QUEUE_FILE_TASK: "FILE_TASK",
}

ACTION_OPTIONS = {
    ACTION_RESTART: "Reiniciar",
    ACTION_SUSPEND: "Suspender",
}

HOST_TASK_TYPE_LABELS = {
    HOST_TASK_CHECK_TYPE: "Solicitar backup",
    HOST_TASK_PROCESSING_TYPE: "Descoberta",
    HOST_TASK_UPDATE_STATISTICS_TYPE: "Atualizar estatísticas",
    HOST_TASK_CHECK_CONNECTION_TYPE: "Verificar conexão",
    HOST_TASK_BACKLOG_CONTROL_TYPE: "Promover backlog",
    HOST_TASK_BACKLOG_ROLLBACK_TYPE: "Retirar da fila",
}

FILE_TASK_TYPE_LABELS = {
    FILE_TASK_BACKUP_TYPE: "Backup",
    FILE_TASK_PROCESS_TYPE: "Processamento",
}

TASK_STATUS_LABELS = {
    TASK_ERROR: "Erro",
    TASK_SUSPENDED: "Suspensa",
    TASK_DONE: "Concluída",
    TASK_PENDING: "Pendente",
    TASK_RUNNING: "Em execução",
    TASK_FROZEN: "Frozen",
}

DEFAULT_PAGE_LIMIT = 200
MAX_PAGE_LIMIT = 500
DEFAULT_HISTORY_PAGE_LIMIT = 50
MAX_HISTORY_PAGE_LIMIT = 100

HISTORY_PHASE_BACKUP = "backup"
HISTORY_PHASE_PROCESS = "process"

HISTORY_PHASE_OPTIONS = {
    HISTORY_PHASE_BACKUP: "Recriar backup",
    HISTORY_PHASE_PROCESS: "Recriar processamento",
}

HISTORY_DATE_FIELDS = {
    "DT_DISCOVERED": "h.DT_DISCOVERED",
    "DT_BACKUP": "h.DT_BACKUP",
    "DT_PROCESSED": "h.DT_PROCESSED",
}


def _normalize_queue_kind(raw_value: str | None) -> str:
    """Keep queue selection inside the two maintenance tables."""
    normalized = str(raw_value or QUEUE_HOST_TASK).strip().lower()
    if normalized in QUEUE_OPTIONS:
        return normalized
    return QUEUE_HOST_TASK


def _normalize_action(raw_value: str | None) -> str:
    """Keep action selection limited to safe queue transitions."""
    normalized = str(raw_value or ACTION_RESTART).strip().lower()
    if normalized in ACTION_OPTIONS:
        return normalized
    return ACTION_RESTART


def _normalize_task_type(raw_value: str | None) -> int | None:
    """Parse an optional task-type filter."""
    if raw_value in (None, "", "all"):
        return None

    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def _normalize_status(raw_value: str | None) -> int | None:
    """Parse an optional queue-status filter."""
    if raw_value in (None, "", "all"):
        return None

    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def _normalize_limit(raw_value: str | None) -> int:
    """Clamp the maintenance page limit to a safe visible window."""
    try:
        parsed = int(raw_value or DEFAULT_PAGE_LIMIT)
    except (TypeError, ValueError):
        return DEFAULT_PAGE_LIMIT

    return max(20, min(parsed, MAX_PAGE_LIMIT))


def _normalize_history_limit(raw_value: str | None) -> int:
    """Clamp history queries to a smaller window than the regular queue page."""
    try:
        parsed = int(raw_value or DEFAULT_HISTORY_PAGE_LIMIT)
    except (TypeError, ValueError):
        return DEFAULT_HISTORY_PAGE_LIMIT

    return max(20, min(parsed, MAX_HISTORY_PAGE_LIMIT))


def build_filters(args: dict[str, Any] | Any) -> dict[str, Any]:
    """Normalize query/form filters shared by GET and POST handlers."""
    getter = args.get if hasattr(args, "get") else dict(args).get
    return {
        "queue_kind": _normalize_queue_kind(getter("queue_kind")),
        "task_type": _normalize_task_type(getter("task_type")),
        "status": _normalize_status(getter("status")),
        "search": str(getter("search") or "").strip(),
        "limit": _normalize_limit(getter("limit")),
    }


def _normalize_history_phase(raw_value: str | None) -> str:
    """Keep history recreation limited to backup or processing phases."""
    normalized = str(raw_value or HISTORY_PHASE_PROCESS).strip().lower()
    if normalized in HISTORY_PHASE_OPTIONS:
        return normalized
    return HISTORY_PHASE_PROCESS


def build_history_filters(args: dict[str, Any] | Any) -> dict[str, Any]:
    """Normalize the filters used by the history recreation panel."""
    getter = args.get if hasattr(args, "get") else dict(args).get
    return {
        "phase": _normalize_history_phase(getter("history_phase")),
        "host_name": str(getter("history_host_name") or "").strip(),
        "host_file_name": str(getter("history_host_file_name") or "").strip(),
        "server_file_name": str(getter("history_server_file_name") or "").strip(),
        "message": str(getter("history_message") or "").strip(),
        "date_field": str(getter("history_date_field") or "").strip().upper(),
        "date_from": str(getter("history_date_from") or "").strip(),
        "date_to": str(getter("history_date_to") or "").strip(),
        "limit": _normalize_history_limit(getter("history_limit")),
    }


def history_filters_are_actionable(filters: dict[str, Any]) -> bool:
    """Require at least one anchored history filter before hitting the DB.

    FILE_TASK_HISTORY is large enough that opening the query with only phase
    and limit still risks a wide scan. The maintenance UI therefore requires
    one concrete narrowing input before it loads recreation candidates.
    """
    return any(
        [
            bool(filters.get("host_name")),
            bool(filters.get("host_file_name")),
            bool(filters.get("server_file_name")),
            bool(filters.get("message")),
            bool(filters.get("date_from")),
            bool(filters.get("date_to")),
        ]
    )


def _build_like_value(search: str) -> str:
    """Wrap one plain-text search token for SQL LIKE matching."""
    return f"%{search}%"


def _build_file_task_message(*, task_type: int, detail: str, path: str | None, name: str | None) -> str:
    """Compose a human-readable audit message without importing appCataloga helpers."""
    task_label = FILE_TASK_TYPE_LABELS.get(int(task_type), str(task_type))
    location = ""
    if path or name:
        location = f" | {path or ''}/{name or ''}".replace("//", "/")
    return f"WebFusion maintenance | {task_label} | Pendente{location} | {detail}"


def _apply_common_filters(
    sql_parts: list[str],
    params: list[Any],
    *,
    alias: str,
    search: str,
    task_type: int | None,
    status: int | None,
    search_columns: tuple[str, ...],
) -> None:
    """Append shared search, type, and status filters to one query."""
    if task_type is not None:
        sql_parts.append(f"{alias}.NU_TYPE = %s")
        params.append(int(task_type))

    if status is not None:
        sql_parts.append(f"{alias}.NU_STATUS = %s")
        params.append(int(status))

    if not search:
        return

    like_value = _build_like_value(search)
    clauses = [f"{column} LIKE %s" for column in search_columns]
    sql_parts.append("(" + " OR ".join(clauses) + ")")
    params.extend([like_value] * len(search_columns))


def list_host_tasks(db, filters: dict[str, Any]) -> list[dict[str, Any]]:
    """Return one filtered maintenance table for HOST_TASK rows."""
    sql_parts = ["1 = 1"]
    params: list[Any] = []
    _apply_common_filters(
        sql_parts,
        params,
        alias="ht",
        search=filters["search"],
        task_type=filters["task_type"],
        status=filters["status"],
        search_columns=("h.NA_HOST_NAME", "ht.NA_MESSAGE"),
    )

    cursor = db.cursor()
    cursor.execute(
        f"""
        SELECT
            ht.ID_HOST_TASK,
            ht.FK_HOST,
            ht.NU_TYPE,
            ht.NU_STATUS,
            ht.NU_PID,
            ht.DT_HOST_TASK,
            ht.NA_MESSAGE,
            h.NA_HOST_NAME,
            h.IS_OFFLINE,
            h.IS_BUSY
        FROM HOST_TASK ht
        JOIN HOST h
          ON h.ID_HOST = ht.FK_HOST
        WHERE {" AND ".join(sql_parts)}
        ORDER BY ht.DT_HOST_TASK DESC, ht.ID_HOST_TASK DESC
        LIMIT %s
        """,
        tuple(params + [filters["limit"]]),
    )
    rows = cursor.fetchall() or []
    for row in rows:
        row["TYPE_LABEL"] = HOST_TASK_TYPE_LABELS.get(row["NU_TYPE"], str(row["NU_TYPE"]))
        row["STATUS_LABEL"] = TASK_STATUS_LABELS.get(row["NU_STATUS"], str(row["NU_STATUS"]))
        row["QUEUE_KIND"] = QUEUE_HOST_TASK
    return rows


def list_file_tasks(db, filters: dict[str, Any]) -> list[dict[str, Any]]:
    """Return one filtered maintenance table for FILE_TASK rows."""
    sql_parts = ["1 = 1"]
    params: list[Any] = []
    _apply_common_filters(
        sql_parts,
        params,
        alias="ft",
        search=filters["search"],
        task_type=filters["task_type"],
        status=filters["status"],
        search_columns=(
            "h.NA_HOST_NAME",
            "ft.NA_HOST_FILE_NAME",
            "ft.NA_SERVER_FILE_NAME",
            "ft.NA_MESSAGE",
        ),
    )

    cursor = db.cursor()
    cursor.execute(
        f"""
        SELECT
            ft.ID_FILE_TASK,
            ft.FK_HOST,
            ft.NU_TYPE,
            ft.NU_STATUS,
            ft.NU_PID,
            ft.DT_FILE_TASK,
            ft.NA_MESSAGE,
            ft.NA_HOST_FILE_PATH,
            ft.NA_HOST_FILE_NAME,
            ft.NA_SERVER_FILE_PATH,
            ft.NA_SERVER_FILE_NAME,
            h.NA_HOST_NAME,
            h.IS_OFFLINE,
            h.IS_BUSY
        FROM FILE_TASK ft
        JOIN HOST h
          ON h.ID_HOST = ft.FK_HOST
        WHERE {" AND ".join(sql_parts)}
        ORDER BY ft.DT_FILE_TASK DESC, ft.ID_FILE_TASK DESC
        LIMIT %s
        """,
        tuple(params + [filters["limit"]]),
    )
    rows = cursor.fetchall() or []
    for row in rows:
        row["TYPE_LABEL"] = FILE_TASK_TYPE_LABELS.get(row["NU_TYPE"], str(row["NU_TYPE"]))
        row["STATUS_LABEL"] = TASK_STATUS_LABELS.get(row["NU_STATUS"], str(row["NU_STATUS"]))
        row["QUEUE_KIND"] = QUEUE_FILE_TASK
    return rows


def list_tasks(db, filters: dict[str, Any]) -> list[dict[str, Any]]:
    """Route one maintenance table request to the chosen queue."""
    queue_kind = filters["queue_kind"]
    if queue_kind == QUEUE_FILE_TASK:
        return list_file_tasks(db, filters)
    return list_host_tasks(db, filters)


def list_file_history_recreate_candidates(db, filters: dict[str, Any]) -> list[dict[str, Any]]:
    """Return conservative FILE_TASK_HISTORY rows eligible for manual recreation."""
    phase = filters["phase"]
    sql_parts = [
        "t.ID_FILE_TASK IS NULL",
    ]
    params: list[Any] = []
    history_index_hint = ""

    if phase == HISTORY_PHASE_BACKUP:
        history_index_hint = "USE INDEX (idx_fth_backup_error_group)"
        sql_parts.extend(
            [
                "h.NU_STATUS_BACKUP IN (%s, %s, %s)",
                "h.NA_HOST_FILE_PATH IS NOT NULL",
                "TRIM(h.NA_HOST_FILE_PATH) <> ''",
                "h.NA_HOST_FILE_NAME IS NOT NULL",
                "TRIM(h.NA_HOST_FILE_NAME) <> ''",
            ]
        )
        params.extend([TASK_ERROR, TASK_SUSPENDED, TASK_FROZEN])
    else:
        history_index_hint = "USE INDEX (idx_fth_processing_error_group)"
        sql_parts.extend(
            [
                "h.NU_STATUS_BACKUP = %s",
                "h.NU_STATUS_PROCESSING IN (%s, %s, %s)",
                "h.NA_SERVER_FILE_PATH IS NOT NULL",
                "TRIM(h.NA_SERVER_FILE_PATH) <> ''",
                "h.NA_SERVER_FILE_NAME IS NOT NULL",
                "TRIM(h.NA_SERVER_FILE_NAME) <> ''",
            ]
        )
        params.extend([TASK_DONE, TASK_ERROR, TASK_SUSPENDED, TASK_FROZEN])

    if filters["host_name"]:
        sql_parts.append("host.NA_HOST_NAME LIKE %s")
        params.append(_build_like_value(filters["host_name"]))

    if filters["host_file_name"]:
        sql_parts.append("h.NA_HOST_FILE_NAME LIKE %s")
        params.append(_build_like_value(filters["host_file_name"]))

    if filters["server_file_name"]:
        sql_parts.append("h.NA_SERVER_FILE_NAME LIKE %s")
        params.append(_build_like_value(filters["server_file_name"]))

    if filters["message"]:
        sql_parts.append("h.NA_MESSAGE LIKE %s")
        params.append(_build_like_value(filters["message"]))

    date_field = filters["date_field"]
    if date_field in HISTORY_DATE_FIELDS:
        sql_field = HISTORY_DATE_FIELDS[date_field]
        if filters["date_from"]:
            sql_parts.append(f"{sql_field} >= %s")
            params.append(filters["date_from"])
        if filters["date_to"]:
            sql_parts.append(f"{sql_field} < %s")
            params.append(filters["date_to"])

    cursor = db.cursor()
    cursor.execute(
        f"""
        SELECT
            h.ID_HISTORY,
            h.FK_HOST,
            host.NA_HOST_NAME,
            host.IS_OFFLINE,
            h.NA_HOST_FILE_NAME,
            h.NA_SERVER_FILE_NAME,
            h.DT_BACKUP,
            h.DT_PROCESSED,
            h.NU_STATUS_BACKUP,
            h.NU_STATUS_PROCESSING,
            h.NA_MESSAGE,
            t.ID_FILE_TASK
        FROM FILE_TASK_HISTORY h {history_index_hint}
        JOIN HOST host
          ON host.ID_HOST = h.FK_HOST
        LEFT JOIN FILE_TASK t USE INDEX (idx_file_task_identity)
          ON t.FK_HOST = h.FK_HOST
         AND t.NA_HOST_FILE_PATH = h.NA_HOST_FILE_PATH
         AND t.NA_HOST_FILE_NAME = h.NA_HOST_FILE_NAME
        WHERE {" AND ".join(sql_parts)}
        ORDER BY h.ID_HISTORY DESC
        LIMIT %s
        """,
        tuple(params + [filters["limit"]]),
    )
    rows = cursor.fetchall() or []
    for row in rows:
        row["BACKUP_STATUS_LABEL"] = TASK_STATUS_LABELS.get(
            row["NU_STATUS_BACKUP"], str(row["NU_STATUS_BACKUP"])
        )
        row["PROCESS_STATUS_LABEL"] = TASK_STATUS_LABELS.get(
            row["NU_STATUS_PROCESSING"], str(row["NU_STATUS_PROCESSING"])
        )
    return rows


def _load_host_tasks_for_action(db, task_ids: list[int]) -> list[dict[str, Any]]:
    """Load the HOST_TASK rows targeted by one bulk action."""
    if not task_ids:
        return []

    placeholders = ", ".join(["%s"] * len(task_ids))
    cursor = db.cursor()
    cursor.execute(
        f"""
        SELECT
            ht.ID_HOST_TASK,
            ht.FK_HOST,
            ht.NU_TYPE,
            ht.NU_STATUS,
            ht.NU_PID,
            h.NA_HOST_NAME,
            h.IS_OFFLINE,
            h.IS_BUSY
        FROM HOST_TASK ht
        JOIN HOST h
          ON h.ID_HOST = ht.FK_HOST
        WHERE ht.ID_HOST_TASK IN ({placeholders})
        """,
        tuple(task_ids),
    )
    return cursor.fetchall() or []


def _load_file_tasks_for_action(db, task_ids: list[int]) -> list[dict[str, Any]]:
    """Load the FILE_TASK rows targeted by one bulk action."""
    if not task_ids:
        return []

    placeholders = ", ".join(["%s"] * len(task_ids))
    cursor = db.cursor()
    cursor.execute(
        f"""
        SELECT
            ft.ID_FILE_TASK,
            ft.FK_HOST,
            ft.NU_TYPE,
            ft.NU_STATUS,
            ft.NU_PID,
            ft.NA_HOST_FILE_PATH,
            ft.NA_HOST_FILE_NAME,
            ft.NA_SERVER_FILE_PATH,
            ft.NA_SERVER_FILE_NAME,
            h.NA_HOST_NAME,
            h.IS_OFFLINE,
            h.IS_BUSY
        FROM FILE_TASK ft
        JOIN HOST h
          ON h.ID_HOST = ft.FK_HOST
        WHERE ft.ID_FILE_TASK IN ({placeholders})
        """,
        tuple(task_ids),
    )
    return cursor.fetchall() or []


def _publish_summary_scope(db, host_id: int, reason: str) -> None:
    """Publish one dirty host scope without risking the committed queue action."""
    cursor = db.cursor()
    cursor.execute(
        """
        REPLACE INTO RFFUSION_SUMMARY.SUMMARY_OUTBOX
            (NA_SCOPE_TYPE, NA_SCOPE_VALUE, NA_SOURCE_HANDLER, NA_REASON)
        VALUES (%s, %s, %s, %s)
        """,
        ("host", str(host_id), "webfusion_maintenance", reason),
    )
    db.commit()


def _status_message(prefix: str, action_label: str) -> str:
    """Build the audit message persisted by manual maintenance actions."""
    return f"WebFusion maintenance | {prefix} | {action_label}"


def _validate_host_task_action(row: dict[str, Any], action: str) -> str | None:
    """Return the blocking reason for one HOST_TASK action, if any."""
    task_type = int(row["NU_TYPE"])
    is_offline = bool(row.get("IS_OFFLINE"))

    if action == ACTION_RESTART and is_offline and task_type in HOST_DEPENDENT_HOST_TASK_TYPES:
        return "host_offline"

    if action == ACTION_SUSPEND and task_type not in SUSPENDABLE_HOST_TASK_TYPES:
        return "unsupported_suspend_type"

    return None


def _validate_file_task_action(row: dict[str, Any], action: str) -> str | None:
    """Return the blocking reason for one FILE_TASK action, if any."""
    task_type = int(row["NU_TYPE"])
    is_offline = bool(row.get("IS_OFFLINE"))

    if action == ACTION_RESTART and task_type == FILE_TASK_BACKUP_TYPE and is_offline:
        return "host_offline"

    if action == ACTION_SUSPEND and task_type not in SUSPENDABLE_FILE_TASK_TYPES:
        return "unsupported_suspend_type"

    return None


def _apply_host_task_action(db, row: dict[str, Any], action: str) -> None:
    """Persist one safe HOST_TASK status change."""
    status = TASK_PENDING if action == ACTION_RESTART else TASK_SUSPENDED
    action_label = ACTION_OPTIONS[action]
    message = _status_message("HOST_TASK", action_label)

    cursor = db.cursor()
    cursor.execute(
        """
        UPDATE HOST_TASK
        SET NU_STATUS = %s,
            NU_PID = NULL,
            DT_HOST_TASK = NOW(),
            NA_MESSAGE = %s
        WHERE ID_HOST_TASK = %s
        """,
        (
            status,
            message,
            int(row["ID_HOST_TASK"]),
        ),
    )
    if int(cursor.rowcount or 0) != 1:
        raise RuntimeError(
            f"HOST_TASK update affected {cursor.rowcount} rows "
            f"(expected 1 for task_id={row['ID_HOST_TASK']})"
        )
    db.commit()

    try:
        _publish_summary_scope(
            db,
            int(row["FK_HOST"]),
            reason=f"maintenance_host_task_{action}",
        )
    except Exception:
        db.rollback()


def _history_phase_field(task_type: int) -> str:
    """Return the FILE_TASK_HISTORY phase column owned by one FILE_TASK type."""
    if int(task_type) == FILE_TASK_BACKUP_TYPE:
        return "NU_STATUS_BACKUP"
    return "NU_STATUS_PROCESSING"


def _load_history_rows_for_recreation(db, history_ids: list[int]) -> list[dict[str, Any]]:
    """Load the FILE_TASK_HISTORY rows targeted by one recreation action."""
    if not history_ids:
        return []

    placeholders = ", ".join(["%s"] * len(history_ids))
    cursor = db.cursor()
    cursor.execute(
        f"""
        SELECT
            h.ID_HISTORY,
            h.FK_HOST,
            host.NA_HOST_NAME,
            host.IS_OFFLINE,
            h.NA_HOST_FILE_PATH,
            h.NA_HOST_FILE_NAME,
            h.NA_EXTENSION_HOST,
            h.VL_FILE_SIZE_KB_HOST,
            h.DT_FILE_CREATED_HOST,
            h.DT_FILE_MODIFIED_HOST,
            h.NA_SERVER_FILE_PATH,
            h.NA_SERVER_FILE_NAME,
            h.NA_EXTENSION_SERVER,
            h.VL_FILE_SIZE_KB_SERVER,
            h.DT_FILE_CREATED_SERVER,
            h.DT_FILE_MODIFIED_SERVER,
            h.DT_BACKUP,
            h.DT_PROCESSED,
            h.NU_STATUS_BACKUP,
            h.NU_STATUS_PROCESSING,
            h.NA_MESSAGE,
            t.ID_FILE_TASK
        FROM FILE_TASK_HISTORY h
        JOIN HOST host
          ON host.ID_HOST = h.FK_HOST
        LEFT JOIN FILE_TASK t
          ON t.FK_HOST = h.FK_HOST
         AND t.NA_HOST_FILE_PATH = h.NA_HOST_FILE_PATH
         AND t.NA_HOST_FILE_NAME = h.NA_HOST_FILE_NAME
        WHERE h.ID_HISTORY IN ({placeholders})
        """,
        tuple(history_ids),
    )
    return cursor.fetchall() or []


def _validate_history_recreation(row: dict[str, Any], action: str) -> str | None:
    """Return the blocking reason for one FILE_TASK recreation request."""
    if row.get("ID_FILE_TASK") is not None:
        return "live_file_task_exists"

    if action == ACTION_RECREATE_BACKUP:
        if bool(row.get("IS_OFFLINE")):
            return "host_offline"
        if row.get("NU_STATUS_BACKUP") not in {TASK_ERROR, TASK_SUSPENDED, TASK_FROZEN}:
            return "unsupported_history_status"
        if not row.get("NA_HOST_FILE_PATH") or not row.get("NA_HOST_FILE_NAME"):
            return "missing_host_identity"
        return None

    if row.get("NU_STATUS_BACKUP") != TASK_DONE:
        return "backup_not_done"
    if row.get("NU_STATUS_PROCESSING") not in {TASK_ERROR, TASK_SUSPENDED, TASK_FROZEN}:
        return "unsupported_history_status"
    if not row.get("NA_SERVER_FILE_PATH") or not row.get("NA_SERVER_FILE_NAME"):
        return "missing_server_identity"
    return None


def _insert_recreated_file_task(cursor, row: dict[str, Any], *, task_type: int, message: str) -> None:
    """Insert one recreated FILE_TASK row using history metadata as source."""
    server_path = row["NA_SERVER_FILE_PATH"] if task_type == FILE_TASK_PROCESS_TYPE else None
    server_name = row["NA_SERVER_FILE_NAME"] if task_type == FILE_TASK_PROCESS_TYPE else None
    server_extension = row["NA_EXTENSION_SERVER"] if task_type == FILE_TASK_PROCESS_TYPE else None
    server_size = row["VL_FILE_SIZE_KB_SERVER"] if task_type == FILE_TASK_PROCESS_TYPE else None
    server_created = row["DT_FILE_CREATED_SERVER"] if task_type == FILE_TASK_PROCESS_TYPE else None
    server_modified = row["DT_FILE_MODIFIED_SERVER"] if task_type == FILE_TASK_PROCESS_TYPE else None

    cursor.execute(
        """
        INSERT INTO FILE_TASK (
            FK_HOST,
            NA_HOST_FILE_PATH,
            NA_HOST_FILE_NAME,
            NA_EXTENSION_HOST,
            VL_FILE_SIZE_KB_HOST,
            DT_FILE_CREATED_HOST,
            DT_FILE_MODIFIED_HOST,
            NU_PID,
            NU_TYPE,
            NU_STATUS,
            DT_FILE_TASK,
            NA_SERVER_FILE_PATH,
            NA_SERVER_FILE_NAME,
            NA_EXTENSION_SERVER,
            VL_FILE_SIZE_KB_SERVER,
            DT_FILE_CREATED_SERVER,
            DT_FILE_MODIFIED_SERVER,
            NA_MESSAGE
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            int(row["FK_HOST"]),
            row["NA_HOST_FILE_PATH"],
            row["NA_HOST_FILE_NAME"],
            row["NA_EXTENSION_HOST"],
            row["VL_FILE_SIZE_KB_HOST"],
            row["DT_FILE_CREATED_HOST"],
            row["DT_FILE_MODIFIED_HOST"],
            int(task_type),
            TASK_PENDING,
            server_path,
            server_name,
            server_extension,
            server_size,
            server_created,
            server_modified,
            message,
        ),
    )
    if int(cursor.rowcount or 0) != 1:
        raise RuntimeError(
            f"FILE_TASK recreation affected {cursor.rowcount} rows "
            f"(expected 1 for history_id={row['ID_HISTORY']})"
        )


def _apply_history_recreate_backup(db, row: dict[str, Any]) -> None:
    """Recreate one BACKUP/PENDING FILE_TASK and rewind history to pre-backup."""
    message = _build_file_task_message(
        task_type=FILE_TASK_BACKUP_TYPE,
        detail="recreated from FILE_TASK_HISTORY",
        path=row.get("NA_HOST_FILE_PATH"),
        name=row.get("NA_HOST_FILE_NAME"),
    )
    cursor = db.cursor()

    try:
        _apply_history_recreate_backup_with_cursor(cursor, row, message=message)
        db.commit()
    except Exception:
        db.rollback()
        raise

    try:
        _publish_summary_scope(
            db,
            int(row["FK_HOST"]),
            reason="maintenance_recreate_backup_from_history",
        )
    except Exception:
        db.rollback()


def _apply_history_recreate_process(db, row: dict[str, Any]) -> None:
    """Recreate one PROCESS/PENDING FILE_TASK and rewind history processing only."""
    message = _build_file_task_message(
        task_type=FILE_TASK_PROCESS_TYPE,
        detail="recreated from FILE_TASK_HISTORY",
        path=row.get("NA_SERVER_FILE_PATH"),
        name=row.get("NA_SERVER_FILE_NAME"),
    )
    cursor = db.cursor()
    backup_at = row.get("DT_BACKUP")

    try:
        _apply_history_recreate_process_with_cursor(
            cursor,
            row,
            message=message,
            backup_at=backup_at,
        )
        db.commit()
    except Exception:
        db.rollback()
        raise

    try:
        _publish_summary_scope(
            db,
            int(row["FK_HOST"]),
            reason="maintenance_recreate_process_from_history",
        )
    except Exception:
        db.rollback()


def _apply_file_task_action(db, row: dict[str, Any], action: str) -> None:
    """Persist one atomic FILE_TASK + FILE_TASK_HISTORY status change."""
    status = TASK_PENDING if action == ACTION_RESTART else TASK_SUSPENDED
    action_label = ACTION_OPTIONS[action]
    message = _status_message("FILE_TASK", action_label)
    history_phase = _history_phase_field(int(row["NU_TYPE"]))

    cursor = db.cursor()

    try:
        cursor.execute(
            """
            UPDATE FILE_TASK
            SET NU_STATUS = %s,
                NU_PID = NULL,
                DT_FILE_TASK = NOW(),
                NA_MESSAGE = %s
            WHERE ID_FILE_TASK = %s
            """,
            (
                status,
                message,
                int(row["ID_FILE_TASK"]),
            ),
        )
        if int(cursor.rowcount or 0) != 1:
            raise RuntimeError(
                f"FILE_TASK update affected {cursor.rowcount} rows "
                f"(expected 1 for task_id={row['ID_FILE_TASK']})"
            )

        cursor.execute(
            f"""
            UPDATE FILE_TASK_HISTORY
            SET {history_phase} = %s,
                NA_MESSAGE = %s
            WHERE FK_HOST = %s
              AND NA_HOST_FILE_PATH = %s
              AND NA_HOST_FILE_NAME = %s
            """,
            (
                status,
                message,
                int(row["FK_HOST"]),
                row["NA_HOST_FILE_PATH"],
                row["NA_HOST_FILE_NAME"],
            ),
        )
        if int(cursor.rowcount or 0) != 1:
            raise RuntimeError(
                f"FILE_TASK_HISTORY update affected {cursor.rowcount} rows "
                f"(expected 1 for host={row['FK_HOST']}, "
                f"path={row['NA_HOST_FILE_PATH']}, name={row['NA_HOST_FILE_NAME']})"
            )

        db.commit()
    except Exception:
        db.rollback()
        raise

    try:
        _publish_summary_scope(
            db,
            int(row["FK_HOST"]),
            reason=f"maintenance_file_task_{action}",
        )
    except Exception:
        db.rollback()


def _build_action_summary(
    *,
    queue_kind: str,
    action: str,
    selected_count: int,
    updated_count: int,
    blocked_rows: list[dict[str, Any]],
    missing_ids: list[int],
) -> dict[str, Any]:
    """Return one UI-friendly summary for the executed maintenance action."""
    return {
        "queue_kind": queue_kind,
        "queue_label": QUEUE_OPTIONS[queue_kind],
        "action": action,
        "action_label": ACTION_OPTIONS[action],
        "selected_count": selected_count,
        "updated_count": updated_count,
        "blocked_count": len(blocked_rows),
        "missing_count": len(missing_ids),
        "blocked_rows": blocked_rows,
        "missing_ids": missing_ids,
    }


def apply_bulk_action(db, queue_kind: str, task_ids: list[int], action: str) -> dict[str, Any]:
    """Apply one safe maintenance action to the selected queue rows."""
    queue_kind = _normalize_queue_kind(queue_kind)
    action = _normalize_action(action)
    unique_ids = sorted({int(task_id) for task_id in task_ids})

    if queue_kind == QUEUE_FILE_TASK:
        rows = _load_file_tasks_for_action(db, unique_ids)
        row_id_key = "ID_FILE_TASK"
        validator = _validate_file_task_action
        applier = _apply_file_task_action
    else:
        rows = _load_host_tasks_for_action(db, unique_ids)
        row_id_key = "ID_HOST_TASK"
        validator = _validate_host_task_action
        applier = _apply_host_task_action

    row_ids = {int(row[row_id_key]) for row in rows}
    missing_ids = [task_id for task_id in unique_ids if task_id not in row_ids]
    blocked_rows = []
    updated_count = 0

    for row in rows:
        blocked_reason = validator(row, action)
        if blocked_reason:
            blocked_rows.append(
                {
                    "task_id": int(row[row_id_key]),
                    "host_name": row["NA_HOST_NAME"],
                    "task_type": (
                        HOST_TASK_TYPE_LABELS.get(row["NU_TYPE"], str(row["NU_TYPE"]))
                        if queue_kind == QUEUE_HOST_TASK
                        else FILE_TASK_TYPE_LABELS.get(row["NU_TYPE"], str(row["NU_TYPE"]))
                    ),
                    "reason": blocked_reason,
                }
            )
            continue

        applier(db, row, action)
        updated_count += 1

    return _build_action_summary(
        queue_kind=queue_kind,
        action=action,
        selected_count=len(unique_ids),
        updated_count=updated_count,
        blocked_rows=blocked_rows,
        missing_ids=missing_ids,
    )


def parse_selected_ids(form_data: Any) -> list[int]:
    """Read the selected checkbox ids from a Flask-style form object."""
    values = []
    if hasattr(form_data, "getlist"):
        values = form_data.getlist("selected_ids")
    else:
        raw = form_data.get("selected_ids", [])
        values = raw if isinstance(raw, list) else [raw]

    parsed = []
    for value in values:
        try:
            parsed.append(int(value))
        except (TypeError, ValueError):
            continue
    return parsed


def parse_selected_history_ids(form_data: Any) -> list[int]:
    """Read the selected FILE_TASK_HISTORY ids from a Flask-style form object."""
    values = []
    if hasattr(form_data, "getlist"):
        values = form_data.getlist("selected_history_ids")
    else:
        raw = form_data.get("selected_history_ids", [])
        values = raw if isinstance(raw, list) else [raw]

    parsed = []
    for value in values:
        try:
            parsed.append(int(value))
        except (TypeError, ValueError):
            continue
    return parsed


def _apply_history_recreate_backup_with_cursor(cursor, row: dict[str, Any], *, message: str) -> None:
    """Apply one backup recreation inside an existing transaction."""
    _insert_recreated_file_task(
        cursor,
        row,
        task_type=FILE_TASK_BACKUP_TYPE,
        message=message,
    )
    cursor.execute(
        """
        UPDATE FILE_TASK_HISTORY
        SET DT_BACKUP = NULL,
            DT_PROCESSED = NULL,
            NU_STATUS_BACKUP = %s,
            NU_STATUS_PROCESSING = %s,
            NA_SERVER_FILE_PATH = NULL,
            NA_SERVER_FILE_NAME = NULL,
            NA_EXTENSION_SERVER = NULL,
            VL_FILE_SIZE_KB_SERVER = NULL,
            DT_FILE_CREATED_SERVER = NULL,
            DT_FILE_MODIFIED_SERVER = NULL,
            NA_MESSAGE = %s
        WHERE ID_HISTORY = %s
        """,
        (
            TASK_PENDING,
            TASK_PENDING,
            message,
            int(row["ID_HISTORY"]),
        ),
    )
    if int(cursor.rowcount or 0) != 1:
        raise RuntimeError(
            f"FILE_TASK_HISTORY backup recreation affected {cursor.rowcount} rows "
            f"(expected 1 for history_id={row['ID_HISTORY']})"
        )


def _apply_history_recreate_process_with_cursor(
    cursor,
    row: dict[str, Any],
    *,
    message: str,
    backup_at: Any,
) -> None:
    """Apply one processing recreation inside an existing transaction."""
    _insert_recreated_file_task(
        cursor,
        row,
        task_type=FILE_TASK_PROCESS_TYPE,
        message=message,
    )
    cursor.execute(
        """
        UPDATE FILE_TASK_HISTORY
        SET DT_BACKUP = %s,
            DT_PROCESSED = NULL,
            NU_STATUS_BACKUP = %s,
            NU_STATUS_PROCESSING = %s,
            NA_MESSAGE = %s
        WHERE ID_HISTORY = %s
        """,
        (
            backup_at,
            TASK_DONE,
            TASK_PENDING,
            message,
            int(row["ID_HISTORY"]),
        ),
    )
    if int(cursor.rowcount or 0) != 1:
        raise RuntimeError(
            f"FILE_TASK_HISTORY processing recreation affected {cursor.rowcount} rows "
            f"(expected 1 for history_id={row['ID_HISTORY']})"
        )


def apply_history_recreate_action(db, history_ids: list[int], action: str) -> dict[str, Any]:
    """Recreate FILE_TASK rows from FILE_TASK_HISTORY using conservative rules."""
    unique_ids = sorted({int(history_id) for history_id in history_ids})
    rows = _load_history_rows_for_recreation(db, unique_ids)
    row_ids = {int(row["ID_HISTORY"]) for row in rows}
    missing_ids = [history_id for history_id in unique_ids if history_id not in row_ids]
    blocked_rows = []
    updated_count = 0

    applier = (
        _apply_history_recreate_backup_with_cursor
        if action == ACTION_RECREATE_BACKUP
        else _apply_history_recreate_process_with_cursor
    )
    updated_hosts: set[int] = set()
    cursor = db.cursor()

    try:
        cursor.execute("START TRANSACTION")

        for row in rows:
            blocked_reason = _validate_history_recreation(row, action)
            if blocked_reason:
                blocked_rows.append(
                    {
                        "task_id": int(row["ID_HISTORY"]),
                        "host_name": row["NA_HOST_NAME"],
                        "task_type": (
                            FILE_TASK_TYPE_LABELS[FILE_TASK_BACKUP_TYPE]
                            if action == ACTION_RECREATE_BACKUP
                            else FILE_TASK_TYPE_LABELS[FILE_TASK_PROCESS_TYPE]
                        ),
                        "reason": blocked_reason,
                    }
                )
                continue

            if action == ACTION_RECREATE_BACKUP:
                message = _build_file_task_message(
                    task_type=FILE_TASK_BACKUP_TYPE,
                    detail="recreated from FILE_TASK_HISTORY",
                    path=row.get("NA_HOST_FILE_PATH"),
                    name=row.get("NA_HOST_FILE_NAME"),
                )
                applier(cursor, row, message=message)
            else:
                message = _build_file_task_message(
                    task_type=FILE_TASK_PROCESS_TYPE,
                    detail="recreated from FILE_TASK_HISTORY",
                    path=row.get("NA_SERVER_FILE_PATH"),
                    name=row.get("NA_SERVER_FILE_NAME"),
                )
                applier(
                    cursor,
                    row,
                    message=message,
                    backup_at=row.get("DT_BACKUP"),
                )
            updated_hosts.add(int(row["FK_HOST"]))
            updated_count += 1

        db.commit()
    except Exception:
        db.rollback()
        raise

    for host_id in sorted(updated_hosts):
        try:
            _publish_summary_scope(
                db,
                host_id,
                reason=(
                    "maintenance_recreate_backup_from_history"
                    if action == ACTION_RECREATE_BACKUP
                    else "maintenance_recreate_process_from_history"
                ),
            )
        except Exception:
            db.rollback()

    return {
        "action": action,
        "action_label": (
            "Recriar backup"
            if action == ACTION_RECREATE_BACKUP
            else "Recriar processamento"
        ),
        "selected_count": len(unique_ids),
        "updated_count": updated_count,
        "blocked_count": len(blocked_rows),
        "missing_count": len(missing_ids),
        "blocked_rows": blocked_rows,
        "missing_ids": missing_ids,
    }


def format_block_reason(reason: str) -> str:
    """Translate one internal block reason into a PT-BR operator message."""
    if reason == "host_offline":
        return "Host offline: a tarefa depende de conectividade."
    if reason == "unsupported_suspend_type":
        return "Suspensão manual não é suportada para este tipo."
    if reason == "live_file_task_exists":
        return "Já existe FILE_TASK ativa para este arquivo."
    if reason == "unsupported_history_status":
        return "O status atual do histórico não é elegível para recriação automática."
    if reason == "missing_host_identity":
        return "O histórico não possui identidade suficiente do arquivo no host."
    if reason == "missing_server_identity":
        return "O histórico não possui metadados suficientes do arquivo no servidor."
    if reason == "backup_not_done":
        return "O processamento só pode ser recriado após backup concluído."
    return "Ação bloqueada por regra operacional."
