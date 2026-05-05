"""Routes for the server-wide operational dashboard.

The server page mixes one server-rendered dashboard shell with several lazy
JSON endpoints. The initial route stays fast, while the heavier panels are only
computed when the operator actually opens them.
"""

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
    """Normalize host-table rows for JSON transport to the browser.

    Jinja can render Python datetimes directly on the initial page load, but
    browser-side rendering of lazy table data needs plain JSON-friendly values.
    """

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
    server_summary_metrics_payload = None

    try:
        server_summary_metrics_payload = get_server_summary_metrics()
    except Exception:
        current_app.logger.exception("failed_to_build_server_summary_metrics_initial")

    return render_template(
        "server/server.html",
        server_overview=server_overview,
        server_summary_metrics=server_summary_metrics_payload,
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
        return (
            jsonify(
                {
                    "rows": [],
                    "error": "failed_to_build_server_processing_errors",
                }
            ),
            503,
        )


@server_bp.route("/api/server/backup-errors", methods=["GET"])
def server_backup_errors():
    """Return grouped backup diagnostics only when the panel is expanded."""

    try:
        return jsonify(get_server_backup_error_overview())
    except Exception:
        current_app.logger.exception("failed_to_build_server_backup_errors")
        return (
            jsonify(
                {
                    "rows": [],
                    "error": "failed_to_build_server_backup_errors",
                }
            ),
            503,
        )


@server_bp.route("/api/server/summary-metrics", methods=["GET"])
def server_summary_metrics():
    """Return the heavy global server summary metrics on demand."""

    try:
        return jsonify(get_server_summary_metrics())
    except Exception:
        current_app.logger.exception("failed_to_build_server_summary_metrics")
        return (
            jsonify(
                {
                    "error": "failed_to_build_server_summary_metrics",
                }
            ),
            503,
        )


@server_bp.route("/api/server/hosts", methods=["GET"])
def server_hosts():
    """Return the filtered station table only when the panel is expanded.

    This endpoint exists so navigation filters can affect only the host table,
    without changing the meaning of the server-wide summary cards.
    """

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
