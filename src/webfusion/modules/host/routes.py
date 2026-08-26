"""Routes for the station-focused host page.

This module follows the common WebFusion pattern:

- render the page shell once with the host selector
- defer heavier diagnostics to focused JSON endpoints
- expose the appCataloga-compatible monitoring snapshot without rendering HTML

That keeps the first render lightweight while still allowing rich drill-down
once the operator expands the host details. JSON reads do not record page views,
because monitoring polls are not operator navigation.
"""

import re
from datetime import date, datetime
from decimal import Decimal

from flask import Blueprint, Response, current_app, jsonify, render_template, request
from db import get_connection_bpdata
from modules.host.service import (
    FILE_TASK_DISCOVERY_TYPE,
    FILE_TASK_PROCESS_TYPE,
    get_all_hosts,
    get_host_activity_detail,
    get_host_backup_error_overview,
    get_host_current_activity,
    get_host_location_history_overview,
    get_host_operational_metrics_snapshot,
    get_host_processing_error_overview,
    get_processed_file_spectrum_metadata,
    get_host_statistics,
)
from modules.server.usage_metrics import record_page_view
from modules.task.service import (
    HOST_TASK_BACKLOG_CONTROL_TYPE,
    HOST_TASK_BACKLOG_ROLLBACK_TYPE,
    HOST_TASK_CHECK_CONNECTION_TYPE,
    HOST_TASK_CHECK_TYPE,
    HOST_TASK_INTERACTIVE_CHECK_TYPE,
    HOST_TASK_PROCESSING_TYPE,
    HOST_TASK_UPDATE_STATISTICS_TYPE,
    TASK_DONE,
    TASK_ERROR,
    TASK_PENDING,
    TASK_RUNNING,
    TASK_SUSPENDED,
    queue_interactive_connectivity_test,
)

host_bp = Blueprint("host", __name__)

HOST_OPERATION_AUTH_USERNAME = "admin"
HOST_OPERATION_AUTH_PASSWORD = "admin"
HOST_OPERATION_AUTH_REALM = "RF.Fusion Task"
HOST_METRICS_SUCCESS_MESSAGE = "Host operational metrics read successfully"
BYTES_PER_KILOBYTE = 1024
_TRANSFER_PROGRESS_PATTERN = re.compile(
    r"(?:^|\|)\s*transfer=(?P<transferred>\d+)/(?P<total>\d+)\s+bytes(?:\s*\||$)"
)

_CONNECTIVITY_TEST_STATUS_LABELS = {
    TASK_ERROR: "Falha",
    TASK_DONE: "Concluído",
    TASK_PENDING: "Aguardando",
    TASK_RUNNING: "Em execução",
}

_HOST_ACTIVITY_STATUS_LABELS = {
    TASK_ERROR: "Falha",
    TASK_DONE: "Concluído",
    TASK_PENDING: "Aguardando",
    TASK_RUNNING: "Em execução",
    TASK_SUSPENDED: "Suspensa",
}

_HOST_ACTIVITY_TYPE_LABELS = {
    HOST_TASK_CHECK_TYPE: "Preparação de backup",
    HOST_TASK_PROCESSING_TYPE: "Discovery de arquivos",
    HOST_TASK_UPDATE_STATISTICS_TYPE: "Atualização de estatísticas",
    HOST_TASK_CHECK_CONNECTION_TYPE: "Checagem de conectividade",
    HOST_TASK_BACKLOG_CONTROL_TYPE: "Promoção para backup",
    HOST_TASK_BACKLOG_ROLLBACK_TYPE: "Retorno da fila de backup",
    HOST_TASK_INTERACTIVE_CHECK_TYPE: "Teste interativo de conectividade",
}


def _extract_connectivity_step_detail(
    message: str,
    marker: str,
    next_marker: str | None = None,
) -> str | None:
    """Extract one operator-facing detail from the worker's stage markers."""
    start = message.find(marker)
    if start < 0:
        return None

    detail_start = start + len(marker)
    detail_end = message.find(next_marker, detail_start) if next_marker else len(message)
    if detail_end < 0:
        detail_end = len(message)

    detail = message[detail_start:detail_end].strip()
    return detail or None


def _connectivity_test_step_details(message: str) -> dict[str, str]:
    """Expose ICMP and SSH reasons separately for the station-test dialog."""
    details: dict[str, str] = {}
    icmp_detail = _extract_connectivity_step_detail(message, "ICMP:", "SSH:")
    ssh_detail = _extract_connectivity_step_detail(message, "SSH:")

    if icmp_detail:
        details["icmp"] = icmp_detail
    if ssh_detail:
        details["ssh"] = ssh_detail
    return details


def _connectivity_test_stage(status: int, message: str) -> str:
    """Return the last real test stage reported by the task worker."""
    normalized_message = message.lower()
    if status == TASK_ERROR:
        icmp_detail = _extract_connectivity_step_detail(message, "ICMP:", "SSH:")
        ssh_detail = _extract_connectivity_step_detail(message, "SSH:")
        if icmp_detail and "não respondeu" in icmp_detail.lower():
            return "icmp"
        if ssh_detail and "não executado" not in ssh_detail.lower():
            return "ssh"
        if icmp_detail:
            return "icmp"
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
        "step_details": _connectivity_test_step_details(message),
        "updated_at": updated_at.isoformat(sep=" ") if updated_at else None,
        "is_terminal": status in {TASK_DONE, TASK_ERROR},
    }


def _format_activity_timestamp(value) -> str | None:
    """Keep the worker timestamp readable without changing its timezone semantics."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value)


def _activity_transfer_progress(activity: dict) -> dict | None:
    """Extract stable transfer progress from the running backup message."""
    if activity.get("source") != "backup-file":
        return None
    if int(activity.get("task_type") or 0) != 1:
        return None
    if activity.get("backup_completed"):
        return None
    if int(activity.get("status") or TASK_PENDING) != TASK_RUNNING:
        return None

    message = str(activity.get("message") or "")
    match = _TRANSFER_PROGRESS_PATTERN.search(message)
    if match:
        transferred_bytes = int(match.group("transferred"))
        total_bytes = int(match.group("total"))
    else:
        try:
            total_bytes = int(float(activity.get("file_size_kb") or 0) * BYTES_PER_KILOBYTE)
        except (TypeError, ValueError):
            total_bytes = 0
        transferred_bytes = 0

    if total_bytes <= 0:
        return None

    transferred_bytes = min(max(0, transferred_bytes), total_bytes)
    return {
        "transferred_bytes": transferred_bytes,
        "total_bytes": total_bytes,
        "percentage": round((transferred_bytes / total_bytes) * 100, 1),
        "is_determinate": match is not None,
    }


def _serialize_host_activity(activity: dict) -> dict:
    """Build the stable polling payload for the current host-work dialog."""
    status = int(activity["status"])
    source = activity["source"]
    is_backup = source == "backup-file"
    is_file_activity = source == "file-task"
    task_type = int(activity.get("task_type") or 0)
    backup_completed = bool(activity.get("backup_completed"))

    if task_type == FILE_TASK_PROCESS_TYPE:
        title = "Processamento de arquivo"
    elif is_backup:
        title = (
            "Arquivo em download"
            if status == TASK_RUNNING
            else "Próximo arquivo de backup"
        )
    elif is_file_activity:
        if task_type == FILE_TASK_DISCOVERY_TYPE:
            title = "Discovery de arquivo"
        else:
            title = "Arquivo em execução"
    else:
        title = _HOST_ACTIVITY_TYPE_LABELS.get(
            int(activity.get("task_type") or 0),
            "Tarefa operacional da estação",
        )

    return {
        "source": source,
        "task_id": int(activity["task_id"]),
        "host_id": int(activity["host_id"]),
        "title": title,
        "status": status,
        "status_label": _HOST_ACTIVITY_STATUS_LABELS.get(status, "Encerrada"),
        "message": activity.get("message") or "Aguardando atualização do worker.",
        "updated_at": _format_activity_timestamp(activity.get("updated_at")),
        "file_name": activity.get("file_name") if is_backup or is_file_activity else None,
        "file_path": activity.get("file_path") if is_backup or is_file_activity else None,
        "server_file_name": activity.get("server_file_name"),
        "transfer_progress": _activity_transfer_progress(activity),
        "is_processing": task_type == FILE_TASK_PROCESS_TYPE,
        "is_terminal": status not in {TASK_PENDING, TASK_RUNNING},
    }


def _serialize_host_metric_value(value):
    """Match the appCataloga metrics gateway JSON scalar conversion."""

    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, date):
        return int(datetime.combine(value, datetime.min.time()).timestamp())
    if isinstance(value, Decimal):
        return float(value)
    return value


def _build_host_zabbix_metrics_payload(snapshot: dict) -> dict:
    """Build the JSON envelope consumed by the existing Zabbix collector."""

    return {
        "status": 1,
        "message": HOST_METRICS_SUCCESS_MESSAGE,
        "metrics": {
            field: _serialize_host_metric_value(value)
            for field, value in snapshot.items()
        },
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


@host_bp.route("/api/host/<int:host_id>/zabbix_metrics", methods=["GET"])
def host_zabbix_metrics(host_id: int):
    """Expose one materialized host snapshot in the appCataloga format."""

    try:
        snapshot = get_host_operational_metrics_snapshot(host_id)
    except Exception:
        current_app.logger.exception(
            "failed_to_read_host_operational_metrics host_id=%s",
            host_id,
        )
        return jsonify(
            {
                "status": 0,
                "message": "Failed to read host operational metrics",
                "metrics": {},
            }
        ), 503

    if not snapshot:
        return jsonify(
            {
                "status": 0,
                "message": "Host operational metrics are not available",
                "metrics": {},
            }
        ), 404

    return jsonify(_build_host_zabbix_metrics_payload(snapshot))


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


@host_bp.route("/api/host/<int:host_id>/activity", methods=["GET"])
def host_current_activity(host_id: int):
    """Return the one task that currently represents work on a station."""
    try:
        activity = get_host_current_activity(host_id)
        return jsonify(
            {
                "activity": _serialize_host_activity(activity) if activity else None,
            }
        )
    except Exception:
        current_app.logger.exception(
            "failed_to_read_host_current_activity host_id=%s",
            host_id,
        )
        return jsonify({"error": "Não foi possível consultar a atividade atual da estação."}), 503


@host_bp.route(
    "/api/host/<int:host_id>/activity/<string:source>/<int:task_id>",
    methods=["GET"],
)
def host_activity_detail(host_id: int, source: str, task_id: int):
    """Keep a chosen task visible until its worker publishes a terminal result."""
    try:
        activity = get_host_activity_detail(
            host_id,
            source,
            task_id,
            request.args.get("file_path"),
            request.args.get("file_name"),
        )
        if not activity:
            return jsonify({"error": "Atividade não encontrada para esta estação."}), 404
        return jsonify(_serialize_host_activity(activity))
    except Exception:
        current_app.logger.exception(
            "failed_to_read_host_activity host_id=%s source=%s task_id=%s",
            host_id,
            source,
            task_id,
        )
        return jsonify({"error": "Não foi possível acompanhar a atividade da estação."}), 503


def _serialize_processed_spectrum_metadata(metadata: dict) -> dict:
    """Convert database values into the compact processed-file JSON contract."""
    def format_datetime(value) -> str | None:
        return _format_activity_timestamp(value) if value else None

    def as_int(value) -> int:
        return int(value or 0)

    def as_float(value) -> float | None:
        return float(value) if value is not None else None

    spectra = [
        {
            "spectrum_id": as_int(row.get("ID_SPECTRUM")),
            "frequency_start": as_float(row.get("FREQUENCY_START")),
            "frequency_end": as_float(row.get("FREQUENCY_END")),
            "description": row.get("DESCRIPTION") or None,
            "time_start": format_datetime(row.get("TIME_START")),
            "time_end": format_datetime(row.get("TIME_END")),
            "locality": row.get("LOCALITY") or None,
            "equipment": row.get("EQUIPMENT") or None,
        }
        for row in metadata.get("SPECTRA", [])
    ]

    return {
        "file_id": as_int(metadata.get("ID_FILE")),
        "file_name": metadata.get("NA_FILE") or None,
        "spectrum_count": as_int(metadata.get("SPECTRUM_COUNT")),
        "time_start": format_datetime(metadata.get("TIME_START")),
        "time_end": format_datetime(metadata.get("TIME_END")),
        "frequency_start": as_float(metadata.get("FREQUENCY_START")),
        "frequency_end": as_float(metadata.get("FREQUENCY_END")),
        "site_count": as_int(metadata.get("SITE_COUNT")),
        "equipment_count": as_int(metadata.get("EQUIPMENT_COUNT")),
        "spectra": spectra,
        "spectra_truncated": as_int(metadata.get("SPECTRUM_COUNT")) > len(spectra),
    }


@host_bp.route("/api/host/<int:host_id>/processed-spectrum-metadata", methods=["GET"])
def processed_file_spectrum_metadata(host_id: int):
    """Return RFDATA spectral metadata for a completed host file."""
    server_file_name = str(request.args.get("file_name") or "").strip()
    if not server_file_name:
        return jsonify({"error": "Arquivo processado não informado."}), 400

    try:
        metadata = get_processed_file_spectrum_metadata(server_file_name)
        return jsonify(
            {
                "metadata": _serialize_processed_spectrum_metadata(metadata)
                if metadata
                else None,
            }
        )
    except Exception:
        current_app.logger.exception(
            "failed_to_read_processed_spectrum_metadata host_id=%s file_name=%s",
            host_id,
            server_file_name,
        )
        return jsonify({"error": "Não foi possível consultar os metadados espectrais."}), 503


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
