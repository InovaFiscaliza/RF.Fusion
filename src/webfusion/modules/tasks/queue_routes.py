"""Render and protect the manual queue-maintenance interface.

The routes normalize HTTP input and show the resulting summaries. They delegate
all validation and state transitions to ``modules.tasks.service`` so the
web UI cannot become a second queue workflow engine. Every mutation remains
behind the maintenance basic-auth check.
"""

from __future__ import annotations

from typing import Any

from flask import jsonify, redirect, render_template, request, url_for

from db import get_connection_bpdata as get_connection
from modules.tasks.blueprints import tasks_api_bp, tasks_bp
from modules.tasks.service import (
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
        "Fila de Arquivos" if queue_kind == QUEUE_FILE_TASK else "Tarefas de Estação"
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


WORKFLOW_STATIONS = "stations"
WORKFLOW_FILES = "files"
WORKFLOW_HISTORY = "history"

WORKFLOW_PAGE_CONTENT = {
    WORKFLOW_STATIONS: {
        "eyebrow": "Estações",
        "title": "Gerenciar tarefas de estação",
        "description": "Consulte, reinicie ou suspenda solicitações enviadas para as estações.",
    },
    WORKFLOW_FILES: {
        "eyebrow": "Arquivos em trânsito",
        "title": "Gerenciar fila de arquivos",
        "description": "Revise as etapas de backup, descoberta e processamento dos arquivos.",
    },
    WORKFLOW_HISTORY: {
        "eyebrow": "Histórico",
        "title": "Recuperar a partir do histórico",
        "description": "Prepare novos backups ou processamentos a partir de registros anteriores.",
    },
}


@tasks_bp.route("/stations", methods=["GET", "POST"])
def station_tasks() -> Any:
    """Render and process station task operations.

    Returns:
        Any: Rendered station task page or a redirect after a successful action.
    """
    return _task_operations(WORKFLOW_STATIONS)


@tasks_bp.route("/files", methods=["GET", "POST"])
def file_tasks() -> Any:
    """Render and process file queue operations.

    Returns:
        Any: Rendered file queue page or a redirect after a successful action.
    """
    return _task_operations(WORKFLOW_FILES)


@tasks_bp.route("/history", methods=["GET", "POST"])
def task_history() -> Any:
    """Render and process history recovery operations.

    Returns:
        Any: Rendered history recovery page or a redirect after a successful action.
    """
    return _task_operations(WORKFLOW_HISTORY)


def _task_operations(workflow: str) -> Any:
    """Render and process one task operation workflow.

    Args:
        workflow: Selected workflow identifier. Type: str.

    Returns:
        Any: Rendered workflow page or a redirect after a successful action.
    """
    host_task_action_summary = None
    file_task_action_summary = None
    history_action_summary = None
    host_task_query_message = None
    file_task_query_message = None
    history_query_message = None
    host_task_action_error = None
    file_task_action_error = None
    follow_host_id = None
    source_data = request.args if request.method == "GET" else request.form
    host_task_filters = _build_queue_filters(
        source_data,
        prefix="host_task",
        queue_kind=QUEUE_HOST_TASK,
    )
    file_task_filters = _build_file_task_filters(source_data)
    history_filters = build_history_filters(source_data)
    host_task_loaded = workflow == WORKFLOW_STATIONS and (
        request.method == "GET" and request.args.get("host_task_load") == "1"
    )
    file_task_loaded = workflow == WORKFLOW_FILES and (
        request.method == "GET" and request.args.get("file_task_load") == "1"
    )
    history_loaded = (
        workflow == WORKFLOW_HISTORY
        and request.method == "GET"
        and request.args.get("history_load") == "1"
    )

    db = get_connection()

    try:
        if request.method == "POST":
            host_task_loaded = False
            file_task_loaded = False
            history_loaded = False
            action = request.form.get("action")
            form_scope = request.form.get("maintenance_form", "")
            try:
                requested_host_id = int(request.form.get("follow_host_id") or 0)
            except (TypeError, ValueError):
                requested_host_id = 0
            if requested_host_id > 0:
                follow_host_id = requested_host_id

            if form_scope == "history_actions" and workflow == WORKFLOW_HISTORY:
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
            elif form_scope == "file_task_targets" and workflow == WORKFLOW_FILES:
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
            elif form_scope == "host_task_actions" and workflow == WORKFLOW_STATIONS:
                selected_ids = parse_selected_ids(request.form)
                action_summary = None
                queue_kind = QUEUE_HOST_TASK
                action_options = ACTION_OPTIONS
                panel_name = "tarefas de estação"

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
                        "Ação concluída. As tarefas de estação não foram recarregadas "
                        "automaticamente para evitar uma nova consulta operacional."
                    )
            else:
                error_message = "Formulário de tarefa inválido. Nenhuma tarefa foi alterada."
                if workflow == WORKFLOW_HISTORY:
                    history_query_message = error_message
                elif workflow == WORKFLOW_FILES:
                    file_task_action_error = error_message
                else:
                    host_task_action_error = error_message

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
    action_summary = (
        history_action_summary
        or file_task_action_summary
        or host_task_action_summary
    )
    if follow_host_id and action_summary:
        return redirect(url_for("host.host", host_id=follow_host_id))

    return render_template(
        "tasks/queues.html",
        workflow=workflow,
        page_content=WORKFLOW_PAGE_CONTENT[workflow],
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


@tasks_api_bp.route("/file-task-hosts", methods=["GET"])
def file_task_hosts():
    """Return the optional host subset that currently has file queue rows."""
    db = get_connection()
    try:
        return jsonify({"hosts": list_file_task_hosts(db)})
    finally:
        db.close()
