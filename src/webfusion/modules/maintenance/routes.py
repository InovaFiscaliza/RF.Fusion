"""Routes for the manual queue-maintenance page.

This module is intentionally conservative. It exposes only a small set of
operator actions that map to existing queue states already handled by
appCataloga, so the web UI does not become a parallel workflow engine.
"""

from __future__ import annotations

from flask import Blueprint, Response, jsonify, render_template, request

from db import get_connection_bpdata as get_connection
from modules.maintenance.service import (
    ACTION_OPTIONS,
    FILE_TASK_TYPE_LABELS,
    HISTORY_TARGET_STAGE_OPTIONS,
    HISTORY_TARGET_STATUS_OPTIONS,
    HOST_TASK_TYPE_LABELS,
    QUEUE_FILE_TASK,
    QUEUE_HOST_TASK,
    TASK_STATUS_LABELS,
    apply_bulk_action,
    apply_file_task_target_action,
    apply_history_action,
    build_file_task_filters,
    build_filters,
    build_history_filters,
    format_block_reason,
    list_file_history,
    list_file_tasks,
    list_file_task_hosts,
    list_host_tasks,
    list_maintenance_hosts,
    parse_selected_ids,
    parse_selected_history_ids,
    validate_file_task_filters,
    validate_history_filters,
)
from modules.server.usage_metrics import record_page_view


maintenance_bp = Blueprint("maintenance", __name__, url_prefix="/maintenance")

MAINTENANCE_AUTH_USERNAME = "admin"
MAINTENANCE_AUTH_PASSWORD = "admin"
MAINTENANCE_AUTH_REALM = "RF.Fusion Maintenance"


def _maintenance_auth_failed():
    """Trigger the browser basic-auth challenge for maintenance access."""
    return Response(
        "Authentication required.",
        401,
        {"WWW-Authenticate": f'Basic realm="{MAINTENANCE_AUTH_REALM}"'},
    )


def _has_valid_maintenance_credentials():
    """Validate the bootstrap credentials protecting manual queue actions."""
    auth = request.authorization
    if not auth:
        return False

    return (
        str(auth.username or "") == MAINTENANCE_AUTH_USERNAME
        and str(auth.password or "") == MAINTENANCE_AUTH_PASSWORD
    )


@maintenance_bp.before_request
def require_maintenance_auth():
    """Protect the maintenance page behind the same simple auth style as tasks."""
    if not _has_valid_maintenance_credentials():
        return _maintenance_auth_failed()


def _build_queue_filters(source: dict, *, prefix: str, queue_kind: str) -> dict:
    """Normalize one panel's prefixed filter values."""
    return build_filters(
        {
            "queue_kind": queue_kind,
            "host_id": source.get(f"{prefix}_host_id"),
            "task_type": source.get(f"{prefix}_task_type"),
            "status": source.get(f"{prefix}_status"),
            "search": source.get(f"{prefix}_search"),
            "limit": source.get(f"{prefix}_limit"),
        }
    )


def _build_file_task_filters(source: dict) -> dict:
    """Normalize the file queue filters, including an optional date range."""
    return build_file_task_filters(
        {
            "queue_kind": QUEUE_FILE_TASK,
            "host_id": source.get("file_task_host_id"),
            "task_type": source.get("file_task_task_type"),
            "status": source.get("file_task_status"),
            "search": source.get("file_task_search"),
            "host_file_name": source.get("file_task_host_file_name"),
            "date_field": source.get("file_task_date_field"),
            "date_from": source.get("file_task_date_from"),
            "date_to": source.get("file_task_date_to"),
            "limit": source.get("file_task_limit"),
        }
    )


def _blocked_rows(summary: dict | None) -> list[dict]:
    """Add human-readable explanations to action summaries."""
    if not summary:
        return []

    return [
        {
            **row,
            "reason_label": format_block_reason(row["reason"]),
        }
        for row in summary["blocked_rows"]
    ]


def _empty_action_summary(*, queue_kind: str, action: str) -> dict:
    """Build the same action summary shape when nothing was selected."""
    action_options = ACTION_OPTIONS
    queue_label = (
        "Fila de Arquivos" if queue_kind == QUEUE_FILE_TASK else "Tarefas do Host"
    )
    return {
        "queue_kind": queue_kind,
        "queue_label": queue_label,
        "action": action,
        "action_label": action_options.get(action, "Ação"),
        "selected_count": 0,
        "updated_count": 0,
        "blocked_count": 0,
        "missing_count": 0,
        "blocked_rows": [],
        "missing_ids": [],
    }


def _empty_history_action_summary(*, target_stage: str, target_status: int) -> dict:
    """Build the history action summary when no row was selected."""
    return {
        "target_stage": target_stage,
        "target_status": target_status,
        "action_label": (
            f"{HISTORY_TARGET_STAGE_OPTIONS[target_stage]}: "
            f"{HISTORY_TARGET_STATUS_OPTIONS[target_status]}"
        ),
        "selected_count": 0,
        "updated_count": 0,
        "blocked_count": 0,
        "missing_count": 0,
        "blocked_rows": [],
        "missing_ids": [],
    }


def _build_template_context(
    *,
    hosts: list[dict],
    host_task_filters: dict,
    host_task_rows: list[dict],
    host_task_loaded: bool,
    host_task_query_message: str | None,
    host_task_action_error: str | None,
    host_task_action_summary: dict | None,
    file_task_filters: dict,
    file_task_rows: list[dict],
    file_task_loaded: bool,
    file_task_query_message: str | None,
    file_task_action_error: str | None,
    file_task_action_summary: dict | None,
    history_filters: dict,
    history_rows: list[dict],
    history_loaded: bool,
    history_query_message: str | None,
    history_action_summary: dict | None,
) -> dict:
    """Compose the full template context shared by GET and POST responses."""
    return {
        "hosts": hosts,
        "host_task_filters": host_task_filters,
        "host_task_rows": host_task_rows,
        "host_task_loaded": host_task_loaded,
        "host_task_query_message": host_task_query_message,
        "host_task_action_error": host_task_action_error,
        "host_task_action_summary": host_task_action_summary,
        "host_task_blocked_rows": _blocked_rows(host_task_action_summary),
        "file_task_filters": file_task_filters,
        "file_task_rows": file_task_rows,
        "file_task_loaded": file_task_loaded,
        "file_task_query_message": file_task_query_message,
        "file_task_action_error": file_task_action_error,
        "file_task_action_summary": file_task_action_summary,
        "file_task_blocked_rows": _blocked_rows(file_task_action_summary),
        "history_filters": history_filters,
        "history_rows": history_rows,
        "history_loaded": history_loaded,
        "history_query_message": history_query_message,
        "history_action_summary": history_action_summary,
        "history_blocked_rows": _blocked_rows(history_action_summary),
        "action_options": ACTION_OPTIONS,
        "history_target_stage_options": HISTORY_TARGET_STAGE_OPTIONS,
        "history_target_status_options": HISTORY_TARGET_STATUS_OPTIONS,
        "host_task_type_labels": HOST_TASK_TYPE_LABELS,
        "file_task_type_labels": FILE_TASK_TYPE_LABELS,
        "task_status_labels": TASK_STATUS_LABELS,
    }


@maintenance_bp.route("/", methods=["GET", "POST"])
def maintenance_dashboard():
    """Render and process the manual queue-maintenance page."""
    host_task_action_summary = None
    file_task_action_summary = None
    history_action_summary = None
    host_task_query_message = None
    file_task_query_message = None
    history_query_message = None
    host_task_action_error = None
    file_task_action_error = None
    source_data = request.args if request.method == "GET" else request.form
    host_task_filters = _build_queue_filters(
        source_data,
        prefix="host_task",
        queue_kind=QUEUE_HOST_TASK,
    )
    file_task_filters = _build_file_task_filters(source_data)
    history_filters = build_history_filters(source_data)
    host_task_loaded = (
        request.method == "GET" and request.args.get("host_task_load") == "1"
    )
    file_task_loaded = (
        request.method == "GET" and request.args.get("file_task_load") == "1"
    )
    history_loaded = request.method == "GET" and request.args.get("history_load") == "1"

    db = get_connection()

    try:
        if request.method == "POST":
            host_task_loaded = False
            file_task_loaded = False
            history_loaded = False
            action = request.form.get("action")
            form_scope = request.form.get("maintenance_form", "")

            if form_scope == "history_actions":
                selected_history_ids = parse_selected_history_ids(request.form)
                target_stage = str(request.form.get("history_target_stage") or "").strip()
                target_status = request.form.get("history_target_status")
                try:
                    parsed_target_status = int(target_status)
                except (TypeError, ValueError):
                    parsed_target_status = None

                if target_stage not in HISTORY_TARGET_STAGE_OPTIONS:
                    history_query_message = "Selecione uma etapa de destino válida."
                elif parsed_target_status not in HISTORY_TARGET_STATUS_OPTIONS:
                    history_query_message = "Selecione uma situação inicial válida."
                elif selected_history_ids:
                    history_action_summary = apply_history_action(
                        db,
                        history_ids=selected_history_ids,
                        target_stage=target_stage,
                        target_status=parsed_target_status,
                    )
                else:
                    history_action_summary = _empty_history_action_summary(
                        target_stage=target_stage,
                        target_status=parsed_target_status,
                    )
                if history_action_summary:
                    history_query_message = (
                        "Ação concluída. O histórico não foi recarregado automaticamente "
                        "para evitar uma nova consulta pesada."
                    )
            elif form_scope == "file_task_targets":
                selected_ids = parse_selected_ids(request.form)
                target_stage = str(request.form.get("file_task_target_stage") or "").strip()
                target_status = request.form.get("file_task_target_status")
                try:
                    parsed_target_status = int(target_status)
                except (TypeError, ValueError):
                    parsed_target_status = None

                if target_stage not in HISTORY_TARGET_STAGE_OPTIONS:
                    file_task_action_error = "Selecione uma etapa de destino válida."
                elif parsed_target_status not in HISTORY_TARGET_STATUS_OPTIONS:
                    file_task_action_error = "Selecione uma situação inicial válida."
                elif selected_ids:
                    file_task_action_summary = apply_file_task_target_action(
                        db,
                        task_ids=selected_ids,
                        target_stage=target_stage,
                        target_status=parsed_target_status,
                    )
                else:
                    file_task_action_summary = _empty_history_action_summary(
                        target_stage=target_stage,
                        target_status=parsed_target_status,
                    )

                if file_task_action_summary:
                    file_task_query_message = (
                        "Ação concluída. A fila de arquivos não foi recarregada "
                        "automaticamente para evitar uma nova consulta operacional."
                    )
            elif form_scope == "host_task_actions":
                selected_ids = parse_selected_ids(request.form)
                action_summary = None
                queue_kind = QUEUE_HOST_TASK
                action_options = ACTION_OPTIONS
                panel_name = "tarefas do host"

                if action not in action_options:
                    error_message = (
                        f"Ação inválida para {panel_name}. Nenhuma tarefa foi alterada."
                    )
                    if queue_kind == QUEUE_HOST_TASK:
                        host_task_action_error = error_message
                    else:
                        file_task_action_error = error_message
                elif selected_ids:
                    action_summary = apply_bulk_action(
                        db,
                        queue_kind=queue_kind,
                        task_ids=selected_ids,
                        action=action,
                    )
                else:
                    action_summary = _empty_action_summary(
                        queue_kind=queue_kind,
                        action=action,
                    )

                host_task_action_summary = action_summary
                if action_summary:
                    host_task_query_message = (
                        "Ação concluída. As tarefas do host não foram recarregadas "
                        "automaticamente para evitar uma nova consulta operacional."
                    )
            else:
                host_task_action_error = "Formulário de manutenção inválido. Nenhuma tarefa foi alterada."

        hosts = list_maintenance_hosts(db)
        host_task_rows = list_host_tasks(db, host_task_filters) if host_task_loaded else []
        if file_task_loaded:
            try:
                validate_file_task_filters(file_task_filters)
            except ValueError as error:
                file_task_rows = []
                file_task_loaded = False
                file_task_query_message = str(error)
            else:
                file_task_rows = list_file_tasks(db, file_task_filters)
        else:
            file_task_rows = []
        if history_loaded:
            try:
                validate_history_filters(history_filters)
            except ValueError as error:
                history_rows = []
                history_loaded = False
                history_query_message = str(error)
            else:
                history_rows = list_file_history(db, history_filters)
        else:
            history_rows = []
    finally:
        db.close()

    record_page_view()
    return render_template(
        "maintenance/maintenance.html",
        **_build_template_context(
            hosts=hosts,
            host_task_filters=host_task_filters,
            host_task_rows=host_task_rows,
            host_task_loaded=host_task_loaded,
            host_task_query_message=host_task_query_message,
            host_task_action_error=host_task_action_error,
            host_task_action_summary=host_task_action_summary,
            file_task_filters=file_task_filters,
            file_task_rows=file_task_rows,
            file_task_loaded=file_task_loaded,
            file_task_query_message=file_task_query_message,
            file_task_action_error=file_task_action_error,
            file_task_action_summary=file_task_action_summary,
            history_filters=history_filters,
            history_rows=history_rows,
            history_loaded=history_loaded,
            history_query_message=history_query_message,
            history_action_summary=history_action_summary,
        ),
    )


@maintenance_bp.route("/file-task-hosts", methods=["GET"])
def file_task_hosts():
    """Return the optional host subset that currently has file queue rows."""
    db = get_connection()
    try:
        return jsonify({"hosts": list_file_task_hosts(db)})
    finally:
        db.close()
