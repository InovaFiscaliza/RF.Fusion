"""Routes for the manual queue-maintenance page.

This module is intentionally conservative. It exposes only a small set of
operator actions that map to existing queue states already handled by
appCataloga, so the web UI does not become a parallel workflow engine.
"""

from __future__ import annotations

from flask import Blueprint, Response, render_template, request

from db import get_connection_bpdata as get_connection
from modules.maintenance.service import (
    ACTION_OPTIONS,
    ACTION_RECREATE_BACKUP,
    ACTION_RECREATE_PROCESS,
    FILE_TASK_TYPE_LABELS,
    HISTORY_PHASE_OPTIONS,
    HOST_TASK_TYPE_LABELS,
    QUEUE_FILE_TASK,
    QUEUE_HOST_TASK,
    TASK_STATUS_LABELS,
    apply_bulk_action,
    apply_history_recreate_action,
    build_filters,
    build_history_filters,
    format_block_reason,
    history_filters_are_actionable,
    list_file_history_recreate_candidates,
    list_tasks,
    parse_selected_ids,
    parse_selected_history_ids,
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


def _build_template_context(
    filters: dict,
    rows: list[dict],
    history_filters: dict,
    history_rows: list[dict],
    history_loaded: bool = False,
    history_query_message: str | None = None,
    action_summary=None,
    history_action_summary=None,
) -> dict:
    """Compose the full template context shared by GET and POST responses."""
    blocked_rows = []
    if action_summary:
        blocked_rows = [
            {
                **row,
                "reason_label": format_block_reason(row["reason"]),
            }
            for row in action_summary["blocked_rows"]
        ]

    history_blocked_rows = []
    if history_action_summary:
        history_blocked_rows = [
            {
                **row,
                "reason_label": format_block_reason(row["reason"]),
            }
            for row in history_action_summary["blocked_rows"]
        ]

    return {
        "filters": filters,
        "rows": rows,
        "history_filters": history_filters,
        "history_rows": history_rows,
        "history_loaded": history_loaded,
        "history_query_message": history_query_message,
        "action_summary": action_summary,
        "history_action_summary": history_action_summary,
        "blocked_rows": blocked_rows,
        "history_blocked_rows": history_blocked_rows,
        "queue_host_task": QUEUE_HOST_TASK,
        "queue_file_task": QUEUE_FILE_TASK,
        "action_options": ACTION_OPTIONS,
        "history_phase_options": HISTORY_PHASE_OPTIONS,
        "action_recreate_backup": ACTION_RECREATE_BACKUP,
        "action_recreate_process": ACTION_RECREATE_PROCESS,
        "host_task_type_labels": HOST_TASK_TYPE_LABELS,
        "file_task_type_labels": FILE_TASK_TYPE_LABELS,
        "task_status_labels": TASK_STATUS_LABELS,
    }


@maintenance_bp.route("/", methods=["GET", "POST"])
def maintenance_dashboard():
    """Render and process the manual queue-maintenance page."""
    action_summary = None
    history_action_summary = None
    history_query_message = None
    source_data = request.args if request.method == "GET" else request.form
    filters = build_filters(source_data)
    history_filters = build_history_filters(source_data)
    history_loaded = False

    db = get_connection()

    try:
        if request.method == "POST":
            action = request.form.get("action")
            form_scope = request.form.get("maintenance_form", "queue_tasks")

            if form_scope == "history_recreate":
                history_loaded = True
                selected_history_ids = parse_selected_history_ids(request.form)
                if selected_history_ids:
                    history_action_summary = apply_history_recreate_action(
                        db,
                        history_ids=selected_history_ids,
                        action=action,
                    )
                else:
                    history_action_summary = {
                        "action": action,
                        "action_label": (
                            "Recriar backup"
                            if action == ACTION_RECREATE_BACKUP
                            else "Recriar processamento"
                        ),
                        "selected_count": 0,
                        "updated_count": 0,
                        "blocked_count": 0,
                        "missing_count": 0,
                        "blocked_rows": [],
                        "missing_ids": [],
                    }
            else:
                selected_ids = parse_selected_ids(request.form)

                if selected_ids:
                    action_summary = apply_bulk_action(
                        db,
                        queue_kind=filters["queue_kind"],
                        task_ids=selected_ids,
                        action=action,
                    )
                else:
                    action_summary = {
                        "queue_kind": filters["queue_kind"],
                        "queue_label": "HOST_TASK" if filters["queue_kind"] == QUEUE_HOST_TASK else "FILE_TASK",
                        "action": action,
                        "action_label": ACTION_OPTIONS.get(action, "Ação"),
                        "selected_count": 0,
                        "updated_count": 0,
                        "blocked_count": 0,
                        "missing_count": 0,
                        "blocked_rows": [],
                        "missing_ids": [],
                    }
        else:
            history_loaded = request.args.get("history_load") == "1"

        rows = list_tasks(db, filters)
        if history_loaded:
            if history_filters_are_actionable(history_filters):
                history_rows = list_file_history_recreate_candidates(db, history_filters)
            else:
                history_rows = []
                history_query_message = (
                    "Informe ao menos um filtro para consultar o histórico: "
                    "host, arquivo host, arquivo server, mensagem ou faixa de data."
                )
        else:
            history_rows = []

        if request.method == "POST" and history_action_summary:
            history_rows = []
            history_query_message = (
                "Recriação concluída. A tabela de histórico não foi recarregada automaticamente "
                "para evitar uma nova consulta pesada."
            )
            history_loaded = False
    finally:
        db.close()

    record_page_view()
    return render_template(
        "maintenance/maintenance.html",
        **_build_template_context(
            filters,
            rows,
            history_filters,
            history_rows,
            history_loaded=history_loaded,
            history_query_message=history_query_message,
            action_summary=action_summary,
            history_action_summary=history_action_summary,
        ),
    )
