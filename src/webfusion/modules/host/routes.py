"""Routes for the station-focused host page.

This module follows the common WebFusion pattern:

- render the page shell once with the host selector
- defer heavier diagnostics to focused JSON endpoints

That keeps the first render lightweight while still allowing rich drill-down
once the operator expands the host details.
"""

from flask import Blueprint, Response, current_app, jsonify, render_template, request
from db import get_connection_bpdata
from modules.host.service import (
    get_all_hosts,
    get_host_backup_error_overview,
    get_host_location_history_overview,
    get_host_processing_error_overview,
    get_host_statistics,
)
from modules.server.usage_metrics import record_page_view
from modules.task.service import (
    HOST_TASK_INTERACTIVE_CHECK_TYPE,
    TASK_DONE,
    TASK_ERROR,
    TASK_PENDING,
    TASK_RUNNING,
    queue_interactive_connectivity_test,
)

host_bp = Blueprint("host", __name__)

HOST_OPERATION_AUTH_USERNAME = "admin"
HOST_OPERATION_AUTH_PASSWORD = "admin"
HOST_OPERATION_AUTH_REALM = "RF.Fusion Task"

_CONNECTIVITY_TEST_STATUS_LABELS = {
    TASK_ERROR: "Falha",
    TASK_DONE: "Concluído",
    TASK_PENDING: "Aguardando",
    TASK_RUNNING: "Em execução",
}


def _connectivity_test_stage(status: int, message: str) -> str:
    """Return the last real test stage reported by the task worker."""
    normalized_message = message.lower()
    if normalized_message.startswith("icmp:") or "icmp" in normalized_message:
        return "icmp"
    if normalized_message.startswith("ssh:") or "ssh" in normalized_message:
        return "ssh"
    if normalized_message.startswith("atualizando"):
        return "persist"
    if status in {TASK_DONE, TASK_ERROR}:
        return "persist"
    return "queue"


def _host_operation_auth_failed() -> Response:
    """Trigger the existing lightweight operation-auth challenge."""
    return Response(
        "Authentication required.",
        401,
        {"WWW-Authenticate": f'Basic realm="{HOST_OPERATION_AUTH_REALM}"'},
    )


def _has_valid_host_operation_credentials() -> bool:
    """Validate the lightweight credentials used by write-oriented screens."""
    auth = request.authorization
    return bool(
        auth
        and str(auth.username or "") == HOST_OPERATION_AUTH_USERNAME
        and str(auth.password or "") == HOST_OPERATION_AUTH_PASSWORD
    )


def _serialize_connectivity_test_row(row: dict) -> dict:
    """Build the compact polling payload consumed by the station-test dialog."""
    status = int(row["NU_STATUS"])
    updated_at = row.get("DT_HOST_TASK")
    message = row.get("NA_MESSAGE") or "Aguardando atualização do teste."
    return {
        "task_id": int(row["ID_HOST_TASK"]),
        "host_id": int(row["FK_HOST"]),
        "host_name": row.get("NA_HOST_NAME") or "Estação",
        "status": status,
        "status_label": _CONNECTIVITY_TEST_STATUS_LABELS.get(status, "Em andamento"),
        "message": message,
        "stage": _connectivity_test_stage(status, message),
        "updated_at": updated_at.isoformat(sep=" ") if updated_at else None,
        "is_terminal": status in {TASK_DONE, TASK_ERROR},
    }


def _read_connectivity_test_row(cursor, host_id: int, task_id: int) -> dict | None:
    """Read one interactive task without loading the broader host queue."""
    cursor.execute(
        """
        SELECT
            HT.ID_HOST_TASK,
            HT.FK_HOST,
            HT.NU_STATUS,
            HT.NA_MESSAGE,
            HT.DT_HOST_TASK,
            H.NA_HOST_NAME
        FROM HOST_TASK HT
        JOIN HOST H ON H.ID_HOST = HT.FK_HOST
        WHERE HT.ID_HOST_TASK = %s
          AND HT.FK_HOST = %s
          AND HT.NU_TYPE = %s
        LIMIT 1
        """,
        (task_id, host_id, HOST_TASK_INTERACTIVE_CHECK_TYPE),
    )
    return cursor.fetchone()


@host_bp.route("/host", methods=["GET"])
def host():
    """Render the host page with an optional station detail panel.

    The left-hand selector/list is always available. When ``host_id`` is
    provided, the page also loads the historical summaries for that station.
    """

    host_id = request.args.get("host_id")
    search = request.args.get("search") or None
    online_only = request.args.get("online_only") == "1"

    hosts = get_all_hosts(online_only=online_only, search=search)
    stats = None

    if host_id:
        stats = get_host_statistics(host_id)

    record_page_view()
    return render_template(
        "host/host.html",
        hosts=hosts,
        stats=stats,
        selected_host=host_id,
        online_only=online_only,
        search=search,
    )


@host_bp.route("/api/host/<int:host_id>/processing-errors", methods=["GET"])
def host_processing_errors(host_id):
    """Return grouped processing errors for one host on demand."""

    try:
        return jsonify(get_host_processing_error_overview(host_id))
    except Exception:
        current_app.logger.exception(
            "failed_to_build_host_processing_errors host_id=%s",
            host_id,
        )
        return jsonify(
            {
                "rows": [],
                "error_group_count": 0,
                "error_total_occurrences": 0,
            }
        )


@host_bp.route("/api/host/<int:host_id>/backup-errors", methods=["GET"])
def host_backup_errors(host_id):
    """Return grouped backup errors for one host on demand."""

    try:
        return jsonify(get_host_backup_error_overview(host_id))
    except Exception:
        current_app.logger.exception(
            "failed_to_build_host_backup_errors host_id=%s",
            host_id,
        )
        return jsonify(
            {
                "rows": [],
                "error_group_count": 0,
                "error_total_occurrences": 0,
            }
        )


@host_bp.route("/api/host/<int:host_id>/locations", methods=["GET"])
def host_locations(host_id):
    """Return reconciled locality history for one host on demand."""

    try:
        return jsonify(get_host_location_history_overview(host_id))
    except Exception:
        current_app.logger.exception(
            "failed_to_build_host_locations host_id=%s",
            host_id,
        )
        return jsonify(
            {
                "equipment_matches": [],
                "location_history": [],
            }
        )


@host_bp.route("/api/host/<int:host_id>/connectivity-test", methods=["POST"])
def start_connectivity_test(host_id):
    """Queue a high-priority connectivity test without performing network I/O."""
    if not _has_valid_host_operation_credentials():
        return _host_operation_auth_failed()

    connection = None
    try:
        connection = get_connection_bpdata()
        cursor = connection.cursor()
        cursor.execute(
            "SELECT ID_HOST FROM HOST WHERE ID_HOST = %s LIMIT 1",
            (host_id,),
        )
        if not cursor.fetchone():
            return jsonify({"error": "Estação não encontrada no catálogo operacional."}), 404

        queued = queue_interactive_connectivity_test(connection, host_id)
        task_row = _read_connectivity_test_row(
            cursor,
            host_id,
            queued["task_id"],
        )
        if not task_row:
            raise RuntimeError("A tarefa de teste não pôde ser consultada após o enfileiramento.")

        payload = _serialize_connectivity_test_row(task_row)
        payload.update(
            created=queued["created"],
            active=queued["active"],
        )
        return jsonify(payload), 202
    except Exception:
        current_app.logger.exception(
            "failed_to_queue_connectivity_test host_id=%s",
            host_id,
        )
        return jsonify({"error": "Não foi possível iniciar o teste da estação."}), 500
    finally:
        if connection is not None:
            connection.close()


@host_bp.route(
    "/api/host/<int:host_id>/connectivity-test/<int:task_id>",
    methods=["GET"],
)
def connectivity_test_status(host_id, task_id):
    """Return one tiny task-status payload for the interactive dialog poller."""
    if not _has_valid_host_operation_credentials():
        return _host_operation_auth_failed()

    connection = None
    try:
        connection = get_connection_bpdata()
        task_row = _read_connectivity_test_row(
            connection.cursor(),
            host_id,
            task_id,
        )
        if not task_row:
            return jsonify({"error": "Teste de estação não encontrado."}), 404
        return jsonify(_serialize_connectivity_test_row(task_row))
    except Exception:
        current_app.logger.exception(
            "failed_to_read_connectivity_test host_id=%s task_id=%s",
            host_id,
            task_id,
        )
        return jsonify({"error": "Não foi possível acompanhar o teste da estação."}), 500
    finally:
        if connection is not None:
            connection.close()
