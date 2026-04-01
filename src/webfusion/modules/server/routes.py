"""Routes for the server-wide operational dashboard."""

from datetime import datetime

from flask import Blueprint, current_app, jsonify, render_template, request

from modules.host.service import (
    get_hosts,
    get_server_backup_error_overview,
    get_server_overview,
    get_server_processing_error_overview,
    get_server_summary_metrics,
)


server_bp = Blueprint("server", __name__)


def _serialize_host_rows(rows):
    """Normalize host-table rows for JSON transport to the browser."""

    serialized = []

    for row in rows:
        clean_row = dict(row)

        for key, value in list(clean_row.items()):
            if isinstance(value, datetime):
                clean_row[key] = value.strftime("%Y-%m-%d %H:%M:%S")

        serialized.append(clean_row)

    return serialized


@server_bp.route("/server", methods=["GET"])
def server():
    """Render the global server dashboard.

    The dashboard itself stays global on purpose. Page filters affect only the
    lower station table so operators can narrow navigation without masking the
    real server totals.
    """

    search = request.args.get("search") or None
    online_only = request.args.get("online_only", "0") == "1"
    server_overview = get_server_overview(online_only=online_only, search=search)

    return render_template(
        "server/server.html",
        server_overview=server_overview,
        online_only=online_only,
        search=search,
    )


@server_bp.route("/api/server/processing-errors", methods=["GET"])
def server_processing_errors():
    """Return grouped processing diagnostics only when the panel is expanded."""

    try:
        return jsonify(get_server_processing_error_overview())
    except Exception:
        current_app.logger.exception("failed_to_build_server_processing_errors")
        return jsonify(
            {
                "rows": [],
                "error_group_count": 0,
                "error_total_occurrences": 0,
            }
        )


@server_bp.route("/api/server/backup-errors", methods=["GET"])
def server_backup_errors():
    """Return grouped backup diagnostics only when the panel is expanded."""

    try:
        return jsonify(get_server_backup_error_overview())
    except Exception:
        current_app.logger.exception("failed_to_build_server_backup_errors")
        return jsonify(
            {
                "rows": [],
                "error_group_count": 0,
                "error_total_occurrences": 0,
            }
        )


@server_bp.route("/api/server/summary-metrics", methods=["GET"])
def server_summary_metrics():
    """Return the heavy global server summary metrics on demand."""

    try:
        return jsonify(get_server_summary_metrics())
    except Exception:
        current_app.logger.exception("failed_to_build_server_summary_metrics")
        return jsonify(
            {
                "CURRENT_MONTH_LABEL": None,
                "BACKUP_DONE_THIS_MONTH": 0,
                "BACKUP_DONE_GB_THIS_MONTH": 0,
                "DISCOVERED_FILES_TOTAL": 0,
                "BACKUP_PENDING_FILES_TOTAL": 0,
                "BACKUP_ERROR_FILES_TOTAL": 0,
                "PROCESSING_PENDING_FILES_TOTAL": 0,
                "PROCESSING_DONE_FILES_TOTAL": 0,
                "FACT_SPECTRUM_TOTAL": 0,
                "PROCESSING_ERROR_FILES_TOTAL": 0,
                "BACKUP_PENDING_GB_TOTAL": 0,
            }
        )


@server_bp.route("/api/server/hosts", methods=["GET"])
def server_hosts():
    """Return the filtered station table only when the panel is expanded."""

    search = request.args.get("search") or None
    online_only = request.args.get("online_only", "0") == "1"

    try:
        rows = get_hosts(search=search, online_only=online_only)
        return jsonify(
            {
                "rows": _serialize_host_rows(rows),
                "count": len(rows),
            }
        )
    except Exception:
        current_app.logger.exception(
            "failed_to_build_server_hosts_table search=%s online_only=%s",
            search,
            online_only,
        )
        return jsonify(
            {
                "rows": [],
                "count": 0,
            }
        )
