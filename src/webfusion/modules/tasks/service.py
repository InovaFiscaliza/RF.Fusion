"""Apply bounded, auditable manual queue maintenance.

This module accepts only task states already understood by appCataloga. FILE_TASK
and FILE_TASK_HISTORY changes are committed together, preserving the same
cross-table view seen by workers and summary metrics. A summary outbox message is
published only after the queue mutation succeeds, so a refresh failure cannot
undo the operator action.

Routes own HTTP input and presentation. This service owns validation and
the compact action summary returned to the interface.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from psycopg import Connection

from modules.tasks.config import (
    QUEUE_HOST_TASK,
    QUEUE_FILE_TASK,
    ACTION_RESTART,
    ACTION_SUSPEND,
    ACTION_MOVE_TO_BACKUP,
    ACTION_REDO_BACKUP,
    ACTION_REPROCESS,
    TASK_ERROR,
    TASK_SUSPENDED,
    TASK_FROZEN,
    TASK_DONE,
    TASK_PENDING,
    TASK_RUNNING,
    HOST_TASK_CHECK_TYPE,
    HOST_TASK_PROCESSING_TYPE,
    HOST_TASK_UPDATE_STATISTICS_TYPE,
    HOST_TASK_CHECK_CONNECTION_TYPE,
    HOST_TASK_BACKLOG_CONTROL_TYPE,
    HOST_TASK_BACKLOG_ROLLBACK_TYPE,
    HOST_TASK_INTERACTIVE_CHECK_TYPE,
    FILE_TASK_BACKUP_TYPE,
    FILE_TASK_PROCESS_TYPE,
    FILE_TASK_DISCOVERY_TYPE,
    HOST_DEPENDENT_HOST_TASK_TYPES,
    SUSPENDABLE_HOST_TASK_TYPES,
    SUSPENDABLE_FILE_TASK_TYPES,
    QUEUE_OPTIONS,
    ACTION_OPTIONS,
    FILE_TASK_ACTION_OPTIONS,
    HOST_TASK_TYPE_LABELS,
    FILE_TASK_TYPE_LABELS,
    TASK_STATUS_LABELS,
    DEFAULT_PAGE_LIMIT,
    MAX_PAGE_LIMIT,
    DEFAULT_HISTORY_PAGE_LIMIT,
    HISTORY_PAGE_LIMIT_OPTIONS,
    BRAZIL_TIMEZONE,
    HISTORY_TARGET_BACKUP,
    HISTORY_TARGET_PROCESS,
    HISTORY_TARGET_STAGE_OPTIONS,
    HISTORY_TARGET_STATUS_OPTIONS,
)
from db.config import FILE_TASK_DATE_FIELDS, HISTORY_DATE_FIELDS
from db.tasks import (
    list_maintenance_hosts,
    list_file_task_hosts,
    load_host_tasks_for_action as _load_host_tasks_for_action,
    load_file_tasks_for_action as _load_file_tasks_for_action,
    publish_summary_scope as _publish_summary_scope,
    apply_history_backup_with_cursor as _apply_history_backup_with_cursor,
    apply_history_process_with_cursor as _apply_history_process_with_cursor,
    history_phase_field as _history_phase_field,
    load_host_tasks,
    load_file_tasks,
    load_file_history,
    update_host_task_status,
    reset_file_task_backup,
    reset_file_task_process,
    update_file_task_status,
    load_history_rows_for_action as _load_history_rows_for_action,
)


def _format_brazil_datetime(value: datetime | str | None) -> str:
    """Format database UTC timestamps for the Brazilian operator interface."""
    if value is None:
        return "-"

    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return value

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    return value.astimezone(BRAZIL_TIMEZONE).strftime("%d/%m/%Y %H:%M:%S BRT")


def _add_display_datetime(row: dict[str, Any], field: str) -> None:
    """Add one localized display field without changing its database value."""
    row[f"{field}_DISPLAY"] = _format_brazil_datetime(row.get(field))


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
    """Parse an optional numeric task type; blank input means no type filter."""
    if raw_value in (None, "", "all"):
        return None

    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def _normalize_status(raw_value: str | None) -> int | None:
    """Parse an optional numeric status; blank input means no status filter."""
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
    """Keep history queries inside the explicit safe visible windows."""
    try:
        parsed = int(raw_value or DEFAULT_HISTORY_PAGE_LIMIT)
    except (TypeError, ValueError):
        return DEFAULT_HISTORY_PAGE_LIMIT

    if parsed in HISTORY_PAGE_LIMIT_OPTIONS:
        return parsed
    return DEFAULT_HISTORY_PAGE_LIMIT


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
    """Allow a bounded recent view, but keep message search anchored."""
    has_identity_filter = any(
        [
            bool(filters.get("host_id")),
            bool(filters.get("host_file_name")),
            bool(filters.get("server_file_name")),
        ]
    )
    if filters.get("message"):
        return has_identity_filter
    return filters.get("limit") in HISTORY_PAGE_LIMIT_OPTIONS


def validate_history_filters(filters: dict[str, Any]) -> None:
    """Validate bounded history searches before querying the durable table."""
    _validate_date_filters(filters, date_fields=HISTORY_DATE_FIELDS)

    if not history_filters_are_actionable(filters):
        raise ValueError(
            "Informe um host ou o nome completo de um arquivo antes de refinar por mensagem."
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


def _build_file_task_message(*, task_type: int, detail: str, path: str | None, name: str | None) -> str:
    """Compose a human-readable audit message without importing appCataloga helpers."""
    task_label = FILE_TASK_TYPE_LABELS.get(int(task_type), str(task_type))
    location = ""
    if path or name:
        location = f" | {path or ''}/{name or ''}".replace("//", "/")
    return f"Manutenção WebFusion | {task_label} | Pendente{location} | {detail}"


def list_host_tasks(db: Connection[dict[str, object]], filters: dict[str, Any]) -> list[dict[str, Any]]:
    """Return one filtered maintenance table for HOST_TASK rows."""
    rows = load_host_tasks(db=db, filters=filters)

    for row in rows:
        row["TYPE_LABEL"] = HOST_TASK_TYPE_LABELS.get(row["NU_TYPE"], str(row["NU_TYPE"]))
        row["STATUS_LABEL"] = TASK_STATUS_LABELS.get(row["NU_STATUS"], str(row["NU_STATUS"]))
        row["QUEUE_KIND"] = QUEUE_HOST_TASK
        _add_display_datetime(row, "DT_HOST_TASK")
    return rows


def list_file_tasks(db: Connection[dict[str, object]], filters: dict[str, Any]) -> list[dict[str, Any]]:
    """Return one filtered maintenance table for FILE_TASK rows."""
    validate_file_task_filters(filters)
    rows = load_file_tasks(db=db, filters=filters)

    for row in rows:
        row["TYPE_LABEL"] = FILE_TASK_TYPE_LABELS.get(row["NU_TYPE"], str(row["NU_TYPE"]))
        row["STATUS_LABEL"] = TASK_STATUS_LABELS.get(row["NU_STATUS"], str(row["NU_STATUS"]))
        row["QUEUE_KIND"] = QUEUE_FILE_TASK
        _add_display_datetime(row, "DT_FILE_TASK")
    return rows


def list_file_history(db: Connection[dict[str, object]], filters: dict[str, Any]) -> list[dict[str, Any]]:
    """Return a bounded view of the durable file history."""
    validate_history_filters(filters)
    rows = load_file_history(db=db, filters=filters)

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
        _add_display_datetime(row, "DT_DISCOVERED")
        _add_display_datetime(row, "DT_BACKUP")
        _add_display_datetime(row, "DT_PROCESSED")
    return rows


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


def _apply_host_task_action(db: Connection[dict[str, object]], row: dict[str, Any], action: str) -> None:
    """Persist one safe HOST_TASK status change."""
    status = TASK_PENDING if action == ACTION_RESTART else TASK_SUSPENDED
    action_label = ACTION_OPTIONS[action]
    message = _status_message("Tarefas do Host", action_label)

    update_host_task_status(db=db, row=row, status=status, message=message)

    try:
        _publish_summary_scope(
            db,
            int(row["FK_HOST"]),
            reason=f"maintenance_host_task_{action}",
        )
    except Exception:
        db.rollback()


def _validate_history_action(
    row: dict[str, Any],
    *,
    target_stage: str,
    target_status: int,
) -> str | None:
    """Return the blocking reason for one history operation request."""
    if row.get("ID_FILE_TASK") is not None and row.get("ACTIVE_TASK_STATUS") != TASK_FROZEN:
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


def _publish_file_task_action(db: Connection[dict[str, object]], row: dict[str, Any], action: str) -> None:
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
    db: Connection[dict[str, object]],
    row: dict[str, Any],
    *,
    task_status: int,
    message: str,
    publish_reason: str,
) -> None:
    """Reset one non-running queue row to BACKUP with the selected status.

    The live task and durable history are one operational transition, so both
    updates share a transaction and an optimistic status check.
    """
    reset_file_task_backup(db=db, row=row, task_status=task_status, message=message)

    _publish_file_task_action(db, row, publish_reason)


def _apply_file_task_process_transition(
    db: Connection[dict[str, object]],
    row: dict[str, Any],
    *,
    task_status: int,
    message: str,
    publish_reason: str,
) -> None:
    """Reset one non-running queue row to PROCESS with durable server metadata.

    The server identity comes from history. Both records change together so the
    processor and summary reader observe the same processing state.
    """
    reset_file_task_process(db=db, row=row, task_status=task_status, message=message)

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


def _apply_file_task_action(db: Connection[dict[str, object]], row: dict[str, Any], action: str) -> None:
    """Persist one atomic FILE_TASK and FILE_TASK_HISTORY maintenance action.

    Stage-changing actions use their dedicated reset paths. Restart and suspend
    keep the current stage but still update both records atomically.
    """
    if action in {ACTION_MOVE_TO_BACKUP, ACTION_REDO_BACKUP}:
        _apply_file_task_backup_action(db, row, action)
        return
    if action == ACTION_REPROCESS:
        _apply_file_task_reprocess_action(db, row)
        return

    status = TASK_PENDING if action == ACTION_RESTART else TASK_SUSPENDED
    message = _status_message("Fila de Arquivos", FILE_TASK_ACTION_OPTIONS[action])
    history_phase = _history_phase_field(int(row["NU_TYPE"]))
    update_file_task_status(db=db, row=row, status=status, message=message, history_phase=history_phase)

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


def _history_action_label(target_stage: str, target_status: int) -> str:
    """Build one concise summary label for a history queue action."""
    return (
        f"{HISTORY_TARGET_STAGE_OPTIONS[target_stage]}: "
        f"{HISTORY_TARGET_STATUS_OPTIONS[target_status]}"
    )


def apply_history_action(
    db: Connection,
    history_ids: list[int],
    *,
    target_stage: str,
    target_status: str | int,
) -> dict[str, Any]:
    """Create or adjust frozen queue tasks from history with one target state.

    One transaction covers the selected history rows. Rows blocked by a domain
    rule are reported but never partially recreated.
    """
    target_stage = _require_history_target_stage(target_stage)
    target_status = _require_history_target_status(str(target_status))
    unique_ids = sorted({int(history_id) for history_id in history_ids})
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
        # Queue creation and history reset must either both persist or neither does.
        rows = _load_history_rows_for_action(db, unique_ids)
        row_ids = {int(row["ID_HISTORY"]) for row in rows}
        missing_ids = [history_id for history_id in unique_ids if history_id not in row_ids]

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

            detail = (
                "frozen task adjusted from FILE_TASK_HISTORY action"
                if row.get("ID_FILE_TASK") is not None
                else "created from FILE_TASK_HISTORY action"
            )
            if target_stage == HISTORY_TARGET_BACKUP:
                message = _build_file_task_message(
                    task_type=FILE_TASK_BACKUP_TYPE,
                    detail=detail,
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
                    detail=detail,
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
