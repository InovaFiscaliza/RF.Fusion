"""Operational maintenance helpers for manual queue intervention.

This module intentionally stays small and conservative. It exposes only the
state transitions that the current appCataloga runtime already understands, so
operators can recover queue rows without inventing a parallel lifecycle from
WebFusion.
"""

from __future__ import annotations

from datetime import date
from typing import Any


QUEUE_HOST_TASK = "host"
QUEUE_FILE_TASK = "file"

ACTION_RESTART = "restart"
ACTION_SUSPEND = "suspend"
ACTION_MOVE_TO_BACKUP = "move_to_backup"
ACTION_REDO_BACKUP = "redo_backup"
ACTION_REPROCESS = "reprocess"

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
FILE_TASK_DISCOVERY_TYPE = 3

HOST_DEPENDENT_HOST_TASK_TYPES = {
    HOST_TASK_CHECK_TYPE,
    HOST_TASK_PROCESSING_TYPE,
    HOST_TASK_CHECK_CONNECTION_TYPE,
}
SUSPENDABLE_HOST_TASK_TYPES = set(HOST_DEPENDENT_HOST_TASK_TYPES)
SUSPENDABLE_FILE_TASK_TYPES = {FILE_TASK_BACKUP_TYPE}

QUEUE_OPTIONS = {
    QUEUE_HOST_TASK: "Tarefas do Host",
    QUEUE_FILE_TASK: "Fila de Arquivos",
}

ACTION_OPTIONS = {
    ACTION_RESTART: "Reiniciar",
    ACTION_SUSPEND: "Suspender",
}

FILE_TASK_ACTION_OPTIONS = {
    ACTION_RESTART: "Reiniciar etapa atual",
    ACTION_SUSPEND: "Suspender",
    ACTION_MOVE_TO_BACKUP: "Mover para backup",
    ACTION_REDO_BACKUP: "Refazer backup",
    ACTION_REPROCESS: "Reprocessar",
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
    FILE_TASK_DISCOVERY_TYPE: "Descoberta",
}

TASK_STATUS_LABELS = {
    TASK_ERROR: "Erro",
    TASK_SUSPENDED: "Suspensa",
    TASK_DONE: "Concluída",
    TASK_PENDING: "Pendente",
    TASK_RUNNING: "Em execução",
    TASK_FROZEN: "Congelada",
}

DEFAULT_PAGE_LIMIT = 200
MAX_PAGE_LIMIT = 500
DEFAULT_HISTORY_PAGE_LIMIT = 50
MAX_HISTORY_PAGE_LIMIT = 100

HISTORY_TARGET_BACKUP = "backup"
HISTORY_TARGET_PROCESS = "process"

HISTORY_TARGET_STAGE_OPTIONS = {
    HISTORY_TARGET_BACKUP: "Backup",
    HISTORY_TARGET_PROCESS: "Processamento",
}

HISTORY_TARGET_STATUS_OPTIONS = {
    TASK_PENDING: "Aguardando",
    TASK_SUSPENDED: "Suspensa",
    TASK_FROZEN: "Congelada",
}

HISTORY_DATE_FIELDS = {
    "DT_FILE_CREATED_HOST": "h.DT_FILE_CREATED_HOST",
    "DT_DISCOVERED": "h.DT_DISCOVERED",
    "DT_BACKUP": "h.DT_BACKUP",
    "DT_PROCESSED": "h.DT_PROCESSED",
}

FILE_TASK_DATE_FIELDS = {
    "DT_FILE_TASK": "ft.DT_FILE_TASK",
    "DT_FILE_CREATED_HOST": "ft.DT_FILE_CREATED_HOST",
}

HISTORY_PHASE_STATUS_FIELDS = {
    "discovery_status": "h.NU_STATUS_DISCOVERY",
    "backup_status": "h.NU_STATUS_BACKUP",
    "processing_status": "h.NU_STATUS_PROCESSING",
}


def _normalize_queue_kind(raw_value: str | None) -> str:
    """Keep queue selection inside the two maintenance tables."""
    normalized = str(raw_value or QUEUE_HOST_TASK).strip().lower()
    if normalized in QUEUE_OPTIONS:
        return normalized
    return QUEUE_HOST_TASK


def _require_supported_queue_kind(raw_value: str | None) -> str:
    """Reject a submitted queue kind that does not map to a known table."""
    queue_kind = str(raw_value or "").strip().lower()
    if queue_kind not in QUEUE_OPTIONS:
        raise ValueError(f"Unsupported queue kind: {queue_kind or '<empty>'}")
    return queue_kind


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


def _normalize_host_id(raw_value: str | None) -> int | None:
    """Parse one optional host selector value."""
    if raw_value in (None, "", "all"):
        return None

    try:
        host_id = int(raw_value)
    except (TypeError, ValueError):
        return None

    return host_id if host_id > 0 else None


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
        "host_id": _normalize_host_id(getter("host_id")),
        "task_type": _normalize_task_type(getter("task_type")),
        "status": _normalize_status(getter("status")),
        "search": str(getter("search") or "").strip(),
        "limit": _normalize_limit(getter("limit")),
    }


def build_file_task_filters(args: dict[str, Any] | Any) -> dict[str, Any]:
    """Normalize FILE_TASK filters, including the optional date range."""
    getter = args.get if hasattr(args, "get") else dict(args).get
    filters = build_filters(args)
    filters.update(
        {
            "host_file_name": str(getter("host_file_name") or "").strip(),
            "date_field": str(getter("date_field") or "").strip().upper(),
            "date_from": str(getter("date_from") or "").strip(),
            "date_to": str(getter("date_to") or "").strip(),
        }
    )
    return filters


def _require_supported_action(
    raw_value: str | None,
    *,
    supported_actions: set[str] | dict[str, str],
    action_group: str,
) -> str:
    """Validate a submitted action before it can mutate an operational queue."""
    action = str(raw_value or "").strip().lower()
    if action not in supported_actions:
        raise ValueError(f"Unsupported {action_group} action: {action or '<empty>'}")
    return action


def build_history_filters(args: dict[str, Any] | Any) -> dict[str, Any]:
    """Normalize the filters used by the history action panel."""
    getter = args.get if hasattr(args, "get") else dict(args).get
    return {
        "host_id": _normalize_host_id(getter("history_host_id")),
        "host_file_name": str(getter("history_host_file_name") or "").strip(),
        "server_file_name": str(getter("history_server_file_name") or "").strip(),
        "message": str(getter("history_message") or "").strip(),
        "date_field": str(getter("history_date_field") or "").strip().upper(),
        "date_from": str(getter("history_date_from") or "").strip(),
        "date_to": str(getter("history_date_to") or "").strip(),
        "discovery_status": _normalize_status(getter("history_discovery_status")),
        "backup_status": _normalize_status(getter("history_backup_status")),
        "processing_status": _normalize_status(getter("history_processing_status")),
        "limit": _normalize_history_limit(getter("history_limit")),
    }


def _require_history_target_stage(raw_value: str | None) -> str:
    """Validate the stage selected for one history action."""
    target_stage = str(raw_value or "").strip().lower()
    if target_stage not in HISTORY_TARGET_STAGE_OPTIONS:
        raise ValueError("Selecione uma etapa de destino válida.")
    return target_stage


def _require_history_target_status(raw_value: str | None) -> int:
    """Validate the initial status selected for one history action."""
    try:
        target_status = int(raw_value)
    except (TypeError, ValueError) as error:
        raise ValueError("Selecione uma situação inicial válida.") from error

    if target_status not in HISTORY_TARGET_STATUS_OPTIONS:
        raise ValueError("Selecione uma situação inicial válida.")
    return target_status


def history_filters_are_actionable(filters: dict[str, Any]) -> bool:
    """Require at least one anchored history filter before hitting the DB.

    FILE_TASK_HISTORY is large enough that opening the query with only a limit
    still risks a wide scan. The maintenance UI therefore requires
    one concrete narrowing input before it loads history candidates.
    """
    has_identity_filter = any(
        [
            bool(filters.get("host_id")),
            bool(filters.get("host_file_name")),
            bool(filters.get("server_file_name")),
        ]
    )
    return has_identity_filter


def validate_history_filters(filters: dict[str, Any]) -> None:
    """Reject history searches that would ignore filters or scan on message text."""
    _validate_date_filters(filters, date_fields=HISTORY_DATE_FIELDS)

    if not history_filters_are_actionable(filters):
        raise ValueError(
            "Selecione um host ou informe o nome completo de um arquivo para consultar o histórico. "
            "Data e mensagem apenas refinam esses filtros."
        )


def validate_file_task_filters(filters: dict[str, Any]) -> None:
    """Reject invalid FILE_TASK date ranges before running the queue query."""
    _validate_date_filters(filters, date_fields=FILE_TASK_DATE_FIELDS)


def _validate_date_filters(
    filters: dict[str, Any],
    *,
    date_fields: dict[str, str],
) -> None:
    """Validate one optional inclusive/exclusive date interval."""
    date_field = filters.get("date_field")
    date_from = filters.get("date_from")
    date_to = filters.get("date_to")

    if (date_from or date_to) and date_field not in date_fields:
        raise ValueError("Selecione o campo de data antes de informar uma faixa de datas.")

    parsed_date_from = None
    parsed_date_to = None
    try:
        if date_from:
            parsed_date_from = date.fromisoformat(str(date_from))
        if date_to:
            parsed_date_to = date.fromisoformat(str(date_to))
    except ValueError as error:
        raise ValueError("Informe datas no formato AAAA-MM-DD.") from error

    if parsed_date_from and parsed_date_to and parsed_date_from >= parsed_date_to:
        raise ValueError("A data final exclusiva deve ser posterior a data inicial.")


def _build_like_value(search: str) -> str:
    """Wrap one plain-text search token for SQL LIKE matching."""
    return f"%{search}%"


def _build_file_task_message(*, task_type: int, detail: str, path: str | None, name: str | None) -> str:
    """Compose a human-readable audit message without importing appCataloga helpers."""
    task_label = FILE_TASK_TYPE_LABELS.get(int(task_type), str(task_type))
    location = ""
    if path or name:
        location = f" | {path or ''}/{name or ''}".replace("//", "/")
    return f"Manutenção WebFusion | {task_label} | Pendente{location} | {detail}"


def _apply_common_filters(
    sql_parts: list[str],
    params: list[Any],
    *,
    alias: str,
    host_id: int | None,
    search: str,
    task_type: int | None,
    status: int | None,
    search_columns: tuple[str, ...],
) -> None:
    """Append shared search, type, and status filters to one query."""
    if host_id is not None:
        sql_parts.append(f"{alias}.FK_HOST = %s")
        params.append(int(host_id))

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


def list_maintenance_hosts(db) -> list[dict[str, Any]]:
    """Return the small host catalog used by maintenance selectors."""
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT
            ID_HOST,
            NA_HOST_NAME,
            IS_OFFLINE
        FROM HOST
        ORDER BY NA_HOST_NAME ASC, ID_HOST ASC
        """
    )
    return cursor.fetchall() or []


def list_file_task_hosts(db) -> list[dict[str, Any]]:
    """Return only hosts that currently have at least one file task."""
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT
            h.ID_HOST,
            h.NA_HOST_NAME,
            h.IS_OFFLINE
        FROM HOST h
        WHERE EXISTS (
            SELECT 1
            FROM FILE_TASK ft USE INDEX (FK_FILE_TASK_HOST)
            WHERE ft.FK_HOST = h.ID_HOST
        )
        ORDER BY h.NA_HOST_NAME ASC, h.ID_HOST ASC
        """
    )
    return cursor.fetchall() or []


def list_host_tasks(db, filters: dict[str, Any]) -> list[dict[str, Any]]:
    """Return one filtered maintenance table for HOST_TASK rows."""
    sql_parts = ["1 = 1"]
    params: list[Any] = []
    _apply_common_filters(
        sql_parts,
        params,
        alias="ht",
        host_id=filters["host_id"],
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
    validate_file_task_filters(filters)
    sql_parts = ["1 = 1"]
    params: list[Any] = []
    _apply_common_filters(
        sql_parts,
        params,
        alias="ft",
        host_id=filters["host_id"],
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

    if filters.get("host_file_name"):
        sql_parts.append("ft.NA_HOST_FILE_NAME = %s")
        params.append(filters["host_file_name"])

    date_field = filters.get("date_field")
    if date_field in FILE_TASK_DATE_FIELDS:
        sql_field = FILE_TASK_DATE_FIELDS[date_field]
        if filters.get("date_from"):
            sql_parts.append(f"{sql_field} >= %s")
            params.append(filters["date_from"])
        if filters.get("date_to"):
            sql_parts.append(f"{sql_field} < %s")
            params.append(filters["date_to"])

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
            h.IS_BUSY,
            fh.ID_HISTORY,
            fh.NA_SERVER_FILE_PATH AS HISTORY_SERVER_FILE_PATH,
            fh.NA_SERVER_FILE_NAME AS HISTORY_SERVER_FILE_NAME
        FROM FILE_TASK ft
        JOIN HOST h
          ON h.ID_HOST = ft.FK_HOST
        LEFT JOIN FILE_TASK_HISTORY fh
          ON fh.FK_HOST = ft.FK_HOST
         AND fh.NA_HOST_FILE_PATH = ft.NA_HOST_FILE_PATH
         AND fh.NA_HOST_FILE_NAME = ft.NA_HOST_FILE_NAME
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


def list_file_history(db, filters: dict[str, Any]) -> list[dict[str, Any]]:
    """Return an anchored view of the durable file history."""
    validate_history_filters(filters)
    sql_parts = ["1 = 1"]
    params: list[Any] = []

    if filters["host_id"] is not None:
        sql_parts.append("h.FK_HOST = %s")
        params.append(int(filters["host_id"]))

    if filters["host_file_name"]:
        sql_parts.append("h.NA_HOST_FILE_NAME = %s")
        params.append(filters["host_file_name"])

    if filters["server_file_name"]:
        sql_parts.append("h.NA_SERVER_FILE_NAME = %s")
        params.append(filters["server_file_name"])

    if filters["message"]:
        sql_parts.append("h.NA_MESSAGE LIKE %s")
        params.append(_build_like_value(filters["message"]))

    for filter_name, sql_field in HISTORY_PHASE_STATUS_FIELDS.items():
        status = filters.get(filter_name)
        if status is not None:
            sql_parts.append(f"{sql_field} = %s")
            params.append(int(status))

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
            h.NU_STATUS_DISCOVERY,
            h.NU_STATUS_BACKUP,
            h.NU_STATUS_PROCESSING,
            h.NA_HOST_FILE_NAME,
            h.NA_SERVER_FILE_PATH,
            h.NA_SERVER_FILE_NAME,
            h.DT_DISCOVERED,
            h.DT_BACKUP,
            h.DT_PROCESSED,
            h.NA_MESSAGE,
            t.ID_FILE_TASK,
            t.NU_TYPE AS ACTIVE_TASK_TYPE,
            t.NU_STATUS AS ACTIVE_TASK_STATUS
        FROM FILE_TASK_HISTORY h
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
        row["DISCOVERY_STATUS_LABEL"] = TASK_STATUS_LABELS.get(
            row["NU_STATUS_DISCOVERY"], str(row["NU_STATUS_DISCOVERY"])
        )
        row["BACKUP_STATUS_LABEL"] = TASK_STATUS_LABELS.get(
            row["NU_STATUS_BACKUP"], str(row["NU_STATUS_BACKUP"])
        )
        row["PROCESS_STATUS_LABEL"] = TASK_STATUS_LABELS.get(
            row["NU_STATUS_PROCESSING"], str(row["NU_STATUS_PROCESSING"])
        )
        if row["ID_FILE_TASK"] is None:
            row["ACTIVE_TASK_LABEL"] = "Nenhuma"
        else:
            task_type = FILE_TASK_TYPE_LABELS.get(
                row["ACTIVE_TASK_TYPE"], str(row["ACTIVE_TASK_TYPE"])
            )
            task_status = TASK_STATUS_LABELS.get(
                row["ACTIVE_TASK_STATUS"], str(row["ACTIVE_TASK_STATUS"])
            )
            row["ACTIVE_TASK_LABEL"] = f"{task_type}: {task_status}"
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
            fh.ID_HISTORY,
            fh.NA_SERVER_FILE_PATH AS HISTORY_SERVER_FILE_PATH,
            fh.NA_SERVER_FILE_NAME AS HISTORY_SERVER_FILE_NAME,
            fh.DT_BACKUP AS HISTORY_DT_BACKUP,
            h.NA_HOST_NAME,
            h.IS_OFFLINE,
            h.IS_BUSY
        FROM FILE_TASK ft
        JOIN HOST h
          ON h.ID_HOST = ft.FK_HOST
        LEFT JOIN FILE_TASK_HISTORY fh
          ON fh.FK_HOST = ft.FK_HOST
         AND fh.NA_HOST_FILE_PATH = ft.NA_HOST_FILE_PATH
         AND fh.NA_HOST_FILE_NAME = ft.NA_HOST_FILE_NAME
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
    return f"Manutenção WebFusion | {prefix} | {action_label}"


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

    if row.get("ID_HISTORY") is None:
        return "missing_history"

    if action in {ACTION_MOVE_TO_BACKUP, ACTION_REDO_BACKUP, ACTION_REPROCESS}:
        if int(row["NU_STATUS"]) == TASK_RUNNING:
            return "task_running"

        if action == ACTION_MOVE_TO_BACKUP and task_type != FILE_TASK_DISCOVERY_TYPE:
            return "move_to_backup_requires_discovery"

        if action in {ACTION_MOVE_TO_BACKUP, ACTION_REDO_BACKUP} and is_offline:
            return "host_offline"

        if action == ACTION_REPROCESS and (
            not row.get("HISTORY_SERVER_FILE_PATH")
            or not row.get("HISTORY_SERVER_FILE_NAME")
        ):
            return "missing_server_identity"

        return None

    if action == ACTION_RESTART and task_type == FILE_TASK_BACKUP_TYPE and is_offline:
        return "host_offline"

    if action == ACTION_SUSPEND and task_type not in SUSPENDABLE_FILE_TASK_TYPES:
        return "unsupported_suspend_type"

    return None


def _validate_file_task_target(
    row: dict[str, Any],
    *,
    target_stage: str,
    target_status: int,
) -> str | None:
    """Return the blocking reason for one FILE_TASK destination request."""
    if row.get("ID_HISTORY") is None:
        return "missing_history"

    if int(row["NU_STATUS"]) == TASK_RUNNING:
        return "task_running"

    if target_stage == HISTORY_TARGET_BACKUP:
        if target_status == TASK_PENDING and bool(row.get("IS_OFFLINE")):
            return "host_offline"
        return None

    if target_stage == HISTORY_TARGET_PROCESS:
        if not row.get("HISTORY_SERVER_FILE_PATH") or not row.get(
            "HISTORY_SERVER_FILE_NAME"
        ):
            return "missing_server_identity"
        return None

    raise ValueError(f"Unsupported file task target stage: {target_stage}")


def _apply_host_task_action(db, row: dict[str, Any], action: str) -> None:
    """Persist one safe HOST_TASK status change."""
    status = TASK_PENDING if action == ACTION_RESTART else TASK_SUSPENDED
    action_label = ACTION_OPTIONS[action]
    message = _status_message("Tarefas do Host", action_label)

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


def _load_history_rows_for_action(db, history_ids: list[int]) -> list[dict[str, Any]]:
    """Load the FILE_TASK_HISTORY rows targeted by one manual action."""
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
            h.NU_STATUS_DISCOVERY,
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


def _validate_history_action(
    row: dict[str, Any],
    *,
    target_stage: str,
    target_status: int,
) -> str | None:
    """Return the blocking reason for one history operation request."""
    if row.get("ID_FILE_TASK") is not None:
        return "live_file_task_exists"

    if target_stage == HISTORY_TARGET_BACKUP:
        if row.get("NU_STATUS_DISCOVERY") != TASK_DONE:
            return "discovery_not_done"
        if row.get("NU_STATUS_BACKUP") == TASK_RUNNING:
            return "task_running"
        if target_status == TASK_PENDING and bool(row.get("IS_OFFLINE")):
            return "host_offline"
        if not row.get("NA_HOST_FILE_PATH") or not row.get("NA_HOST_FILE_NAME"):
            return "missing_host_identity"
        return None

    if target_stage == HISTORY_TARGET_PROCESS:
        if row.get("NU_STATUS_BACKUP") != TASK_DONE:
            return "backup_not_done"
        if row.get("NU_STATUS_PROCESSING") == TASK_RUNNING:
            return "task_running"
        if not row.get("NA_SERVER_FILE_PATH") or not row.get("NA_SERVER_FILE_NAME"):
            return "missing_server_identity"
        return None

    raise ValueError(f"Unsupported history target stage: {target_stage}")


def _insert_history_file_task(
    cursor,
    row: dict[str, Any],
    *,
    task_type: int,
    task_status: int,
    message: str,
) -> None:
    """Insert one FILE_TASK row using durable history metadata as source."""
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
            task_status,
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


def _assert_single_row(cursor, *, table: str, task_id: int) -> None:
    """Raise when a maintenance transition lost its optimistic status check."""
    if int(cursor.rowcount or 0) != 1:
        raise RuntimeError(
            f"{table} update affected {cursor.rowcount} rows "
            f"(expected 1 for task_id={task_id})"
        )


def _publish_file_task_action(db, row: dict[str, Any], action: str) -> None:
    """Publish the committed maintenance mutation without reopening its transaction."""
    try:
        _publish_summary_scope(
            db,
            int(row["FK_HOST"]),
            reason=f"maintenance_file_task_{action}",
        )
    except Exception:
        db.rollback()


def _apply_file_task_backup_transition(
    db,
    row: dict[str, Any],
    *,
    task_status: int,
    message: str,
    publish_reason: str,
) -> None:
    """Reset one non-running queue row to BACKUP with the selected status."""
    cursor = db.cursor()

    try:
        cursor.execute("START TRANSACTION")
        cursor.execute(
            """
            UPDATE FILE_TASK
            SET NU_TYPE = %s,
                NU_STATUS = %s,
                NU_PID = NULL,
                DT_FILE_TASK = NOW(),
                NA_SERVER_FILE_PATH = NULL,
                NA_SERVER_FILE_NAME = NULL,
                NA_EXTENSION_SERVER = NULL,
                VL_FILE_SIZE_KB_SERVER = NULL,
                DT_FILE_CREATED_SERVER = NULL,
                DT_FILE_MODIFIED_SERVER = NULL,
                NA_MESSAGE = %s,
                NA_ERROR_CODE = NULL,
                NA_ERROR_DETAIL = NULL,
                NU_ERROR_CLASSIFIER_VERSION = NULL
            WHERE ID_FILE_TASK = %s
              AND NU_STATUS = %s
            """,
            (
                FILE_TASK_BACKUP_TYPE,
                task_status,
                message,
                int(row["ID_FILE_TASK"]),
                int(row["NU_STATUS"]),
            ),
        )
        _assert_single_row(cursor, table="FILE_TASK", task_id=int(row["ID_FILE_TASK"]))

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
                IS_PAYLOAD_DELETED = 0,
                DT_PAYLOAD_DELETED = NULL,
                NA_MESSAGE = %s,
                NA_ERROR_CODE = NULL,
                NA_ERROR_DETAIL = NULL,
                NU_ERROR_CLASSIFIER_VERSION = NULL
            WHERE ID_HISTORY = %s
            """,
            (
                task_status,
                TASK_PENDING,
                message,
                int(row["ID_HISTORY"]),
            ),
        )
        _assert_single_row(cursor, table="FILE_TASK_HISTORY", task_id=int(row["ID_FILE_TASK"]))
        db.commit()
    except Exception:
        db.rollback()
        raise

    _publish_file_task_action(db, row, publish_reason)


def _apply_file_task_process_transition(
    db,
    row: dict[str, Any],
    *,
    task_status: int,
    message: str,
    publish_reason: str,
) -> None:
    """Reset one non-running queue row to PROCESS with durable server metadata."""
    cursor = db.cursor()

    try:
        cursor.execute("START TRANSACTION")
        cursor.execute(
            """
            UPDATE FILE_TASK
            SET NU_TYPE = %s,
                NU_STATUS = %s,
                NU_PID = NULL,
                DT_FILE_TASK = NOW(),
                NA_SERVER_FILE_PATH = %s,
                NA_SERVER_FILE_NAME = %s,
                NA_MESSAGE = %s,
                NA_ERROR_CODE = NULL,
                NA_ERROR_DETAIL = NULL,
                NU_ERROR_CLASSIFIER_VERSION = NULL
            WHERE ID_FILE_TASK = %s
              AND NU_STATUS = %s
            """,
            (
                FILE_TASK_PROCESS_TYPE,
                task_status,
                row["HISTORY_SERVER_FILE_PATH"],
                row["HISTORY_SERVER_FILE_NAME"],
                message,
                int(row["ID_FILE_TASK"]),
                int(row["NU_STATUS"]),
            ),
        )
        _assert_single_row(cursor, table="FILE_TASK", task_id=int(row["ID_FILE_TASK"]))

        cursor.execute(
            """
            UPDATE FILE_TASK_HISTORY
            SET DT_BACKUP = COALESCE(DT_BACKUP, NOW()),
                DT_PROCESSED = NULL,
                NU_STATUS_BACKUP = %s,
                NU_STATUS_PROCESSING = %s,
                NA_MESSAGE = %s,
                NA_ERROR_CODE = NULL,
                NA_ERROR_DETAIL = NULL,
                NU_ERROR_CLASSIFIER_VERSION = NULL
            WHERE ID_HISTORY = %s
            """,
            (
                TASK_DONE,
                task_status,
                message,
                int(row["ID_HISTORY"]),
            ),
        )
        _assert_single_row(cursor, table="FILE_TASK_HISTORY", task_id=int(row["ID_FILE_TASK"]))
        db.commit()
    except Exception:
        db.rollback()
        raise

    _publish_file_task_action(db, row, publish_reason)


def _apply_file_task_backup_action(db, row: dict[str, Any], action: str) -> None:
    """Keep the legacy backup action mapped to BACKUP/PENDING."""
    _apply_file_task_backup_transition(
        db,
        row,
        task_status=TASK_PENDING,
        message=_status_message("Fila de Arquivos", FILE_TASK_ACTION_OPTIONS[action]),
        publish_reason=action,
    )


def _apply_file_task_reprocess_action(db, row: dict[str, Any]) -> None:
    """Keep the legacy reprocess action mapped to PROCESS/PENDING."""
    _apply_file_task_process_transition(
        db,
        row,
        task_status=TASK_PENDING,
        message=_status_message(
            "Fila de Arquivos", FILE_TASK_ACTION_OPTIONS[ACTION_REPROCESS]
        ),
        publish_reason=ACTION_REPROCESS,
    )


def _apply_file_task_action(db, row: dict[str, Any], action: str) -> None:
    """Persist one atomic FILE_TASK + FILE_TASK_HISTORY maintenance action."""
    if action in {ACTION_MOVE_TO_BACKUP, ACTION_REDO_BACKUP}:
        _apply_file_task_backup_action(db, row, action)
        return
    if action == ACTION_REPROCESS:
        _apply_file_task_reprocess_action(db, row)
        return

    status = TASK_PENDING if action == ACTION_RESTART else TASK_SUSPENDED
    message = _status_message("Fila de Arquivos", FILE_TASK_ACTION_OPTIONS[action])
    history_phase = _history_phase_field(int(row["NU_TYPE"]))
    cursor = db.cursor()

    try:
        cursor.execute("START TRANSACTION")
        cursor.execute(
            """
            UPDATE FILE_TASK
            SET NU_STATUS = %s,
                NU_PID = NULL,
                DT_FILE_TASK = NOW(),
                NA_MESSAGE = %s
            WHERE ID_FILE_TASK = %s
              AND NU_STATUS = %s
            """,
            (
                status,
                message,
                int(row["ID_FILE_TASK"]),
                int(row["NU_STATUS"]),
            ),
        )
        _assert_single_row(cursor, table="FILE_TASK", task_id=int(row["ID_FILE_TASK"]))

        cursor.execute(
            f"""
            UPDATE FILE_TASK_HISTORY
            SET {history_phase} = %s,
                NA_MESSAGE = %s
            WHERE ID_HISTORY = %s
            """,
            (
                status,
                message,
                int(row["ID_HISTORY"]),
            ),
        )
        _assert_single_row(cursor, table="FILE_TASK_HISTORY", task_id=int(row["ID_FILE_TASK"]))
        db.commit()
    except Exception:
        db.rollback()
        raise

    _publish_file_task_action(db, row, action)


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
    action_options = FILE_TASK_ACTION_OPTIONS if queue_kind == QUEUE_FILE_TASK else ACTION_OPTIONS
    return {
        "queue_kind": queue_kind,
        "queue_label": QUEUE_OPTIONS[queue_kind],
        "action": action,
        "action_label": action_options[action],
        "selected_count": selected_count,
        "updated_count": updated_count,
        "blocked_count": len(blocked_rows),
        "missing_count": len(missing_ids),
        "blocked_rows": blocked_rows,
        "missing_ids": missing_ids,
    }


def apply_bulk_action(db, queue_kind: str, task_ids: list[int], action: str) -> dict[str, Any]:
    """Apply one safe maintenance action to the selected queue rows."""
    queue_kind = _require_supported_queue_kind(queue_kind)
    action_options = FILE_TASK_ACTION_OPTIONS if queue_kind == QUEUE_FILE_TASK else ACTION_OPTIONS
    action = _require_supported_action(
        action,
        supported_actions=action_options,
        action_group="queue",
    )
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


def apply_file_task_target_action(
    db,
    task_ids: list[int],
    *,
    target_stage: str,
    target_status: str | int,
) -> dict[str, Any]:
    """Move selected FILE_TASK rows to one safe stage and status combination."""
    target_stage = _require_history_target_stage(target_stage)
    target_status = _require_history_target_status(str(target_status))
    unique_ids = sorted({int(task_id) for task_id in task_ids})
    rows = _load_file_tasks_for_action(db, unique_ids)
    row_ids = {int(row["ID_FILE_TASK"]) for row in rows}
    missing_ids = [task_id for task_id in unique_ids if task_id not in row_ids]
    blocked_rows = []
    updated_count = 0
    action_label = _history_action_label(target_stage, target_status)
    publish_reason = f"{target_stage}_{target_status}"

    for row in rows:
        blocked_reason = _validate_file_task_target(
            row,
            target_stage=target_stage,
            target_status=target_status,
        )
        if blocked_reason:
            task_type = (
                FILE_TASK_BACKUP_TYPE
                if target_stage == HISTORY_TARGET_BACKUP
                else FILE_TASK_PROCESS_TYPE
            )
            blocked_rows.append(
                {
                    "task_id": int(row["ID_FILE_TASK"]),
                    "host_name": row["NA_HOST_NAME"],
                    "task_type": FILE_TASK_TYPE_LABELS[task_type],
                    "reason": blocked_reason,
                }
            )
            continue

        message = _status_message("Fila de Arquivos", action_label)
        if target_stage == HISTORY_TARGET_BACKUP:
            _apply_file_task_backup_transition(
                db,
                row,
                task_status=target_status,
                message=message,
                publish_reason=publish_reason,
            )
        else:
            _apply_file_task_process_transition(
                db,
                row,
                task_status=target_status,
                message=message,
                publish_reason=publish_reason,
            )
        updated_count += 1

    return {
        "queue_kind": QUEUE_FILE_TASK,
        "queue_label": QUEUE_OPTIONS[QUEUE_FILE_TASK],
        "target_stage": target_stage,
        "target_status": target_status,
        "action_label": action_label,
        "selected_count": len(unique_ids),
        "updated_count": updated_count,
        "blocked_count": len(blocked_rows),
        "missing_count": len(missing_ids),
        "blocked_rows": blocked_rows,
        "missing_ids": missing_ids,
    }


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


def _apply_history_backup_with_cursor(
    cursor,
    row: dict[str, Any],
    *,
    task_status: int,
    message: str,
) -> None:
    """Create one backup task and reset history to the selected status."""
    _insert_history_file_task(
        cursor,
        row,
        task_type=FILE_TASK_BACKUP_TYPE,
        task_status=task_status,
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
            task_status,
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


def _apply_history_process_with_cursor(
    cursor,
    row: dict[str, Any],
    *,
    task_status: int,
    message: str,
    backup_at: Any,
) -> None:
    """Create one processing task and reset history to the selected status."""
    _insert_history_file_task(
        cursor,
        row,
        task_type=FILE_TASK_PROCESS_TYPE,
        task_status=task_status,
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
            task_status,
            message,
            int(row["ID_HISTORY"]),
        ),
    )
    if int(cursor.rowcount or 0) != 1:
        raise RuntimeError(
            f"FILE_TASK_HISTORY processing recreation affected {cursor.rowcount} rows "
            f"(expected 1 for history_id={row['ID_HISTORY']})"
        )


def _history_action_label(target_stage: str, target_status: int) -> str:
    """Build one concise summary label for a history queue action."""
    return (
        f"{HISTORY_TARGET_STAGE_OPTIONS[target_stage]}: "
        f"{HISTORY_TARGET_STATUS_OPTIONS[target_status]}"
    )


def apply_history_action(
    db,
    history_ids: list[int],
    *,
    target_stage: str,
    target_status: str | int,
) -> dict[str, Any]:
    """Create selected queue tasks from durable history with one target state."""
    target_stage = _require_history_target_stage(target_stage)
    target_status = _require_history_target_status(str(target_status))
    unique_ids = sorted({int(history_id) for history_id in history_ids})
    rows = _load_history_rows_for_action(db, unique_ids)
    row_ids = {int(row["ID_HISTORY"]) for row in rows}
    missing_ids = [history_id for history_id in unique_ids if history_id not in row_ids]
    blocked_rows = []
    updated_count = 0

    applier = (
        _apply_history_backup_with_cursor
        if target_stage == HISTORY_TARGET_BACKUP
        else _apply_history_process_with_cursor
    )
    updated_hosts: set[int] = set()
    cursor = db.cursor()

    try:
        cursor.execute("START TRANSACTION")

        for row in rows:
            blocked_reason = _validate_history_action(
                row,
                target_stage=target_stage,
                target_status=target_status,
            )
            if blocked_reason:
                blocked_rows.append(
                    {
                        "task_id": int(row["ID_HISTORY"]),
                        "host_name": row["NA_HOST_NAME"],
                        "task_type": (
                            FILE_TASK_TYPE_LABELS[FILE_TASK_BACKUP_TYPE]
                            if target_stage == HISTORY_TARGET_BACKUP
                            else FILE_TASK_TYPE_LABELS[FILE_TASK_PROCESS_TYPE]
                        ),
                        "reason": blocked_reason,
                    }
                )
                continue

            if target_stage == HISTORY_TARGET_BACKUP:
                message = _build_file_task_message(
                    task_type=FILE_TASK_BACKUP_TYPE,
                    detail="created from FILE_TASK_HISTORY action",
                    path=row.get("NA_HOST_FILE_PATH"),
                    name=row.get("NA_HOST_FILE_NAME"),
                )
                applier(
                    cursor,
                    row,
                    task_status=target_status,
                    message=message,
                )
            else:
                message = _build_file_task_message(
                    task_type=FILE_TASK_PROCESS_TYPE,
                    detail="created from FILE_TASK_HISTORY action",
                    path=row.get("NA_SERVER_FILE_PATH"),
                    name=row.get("NA_SERVER_FILE_NAME"),
                )
                applier(
                    cursor,
                    row,
                    task_status=target_status,
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
                    f"maintenance_history_{target_stage}_{target_status}"
                ),
            )
        except Exception:
            db.rollback()

    return {
        "target_stage": target_stage,
        "target_status": target_status,
        "action_label": _history_action_label(target_stage, target_status),
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
        return "Já existe uma tarefa ativa para este arquivo."
    if reason == "missing_host_identity":
        return "O histórico não possui identidade suficiente do arquivo no host."
    if reason == "missing_server_identity":
        return "O histórico não possui metadados suficientes do arquivo no servidor."
    if reason == "backup_not_done":
        return "O processamento só pode ser preparado após backup concluído."
    if reason == "discovery_not_done":
        return "O backup só pode ser preparado após a descoberta ser concluída."
    if reason == "task_running":
        return "A task está em execução e não pode ser reconfigurada manualmente."
    if reason == "missing_history":
        return "Não existe histórico correspondente para manter a transição consistente."
    if reason == "move_to_backup_requires_discovery":
        return "Mover para backup é destinado a itens ainda na etapa de descoberta."
    return "Ação bloqueada por regra operacional."
