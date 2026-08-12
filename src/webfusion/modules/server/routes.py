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
from modules.server.usage_metrics import (
    get_usage_metrics_snapshot,
    record_download_action,
    record_page_view,
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


def _build_zabbix_metrics_payload() -> dict[str, object]:
    """Build the flat monitoring payload without rendering the dashboard."""

    server_overview = get_server_overview()
    summary_metrics = get_server_summary_metrics()
    usage_metrics = get_usage_metrics_snapshot()
    usage_totals = usage_metrics["totals"]
    usage_current_month = usage_metrics["current_month_totals"]
    memory = server_overview["SERVER_MEMORY"]
    reposfi = server_overview["REPOSFI_USAGE"]
    appanalise = server_overview["APP_ANALISE_STATUS"]

    payload = {
        "status": "ok",
        "reference_month": summary_metrics["CURRENT_MONTH_LABEL"],
        "host_total": int(server_overview["TOTAL_HOSTS"]),
        "host_online": int(server_overview["ONLINE_HOSTS"]),
        "host_offline": int(server_overview["OFFLINE_HOSTS"]),
        "host_busy": int(server_overview["BUSY_HOSTS"]),
        "memory_total_bytes": int(memory["total_bytes"]),
        "memory_used_bytes": int(memory["used_bytes"]),
        "memory_available_bytes": int(memory["available_bytes"]),
        "memory_used_percent": float(memory["use_percent"]),
        "reposfi_mounted": int(bool(reposfi["mounted"])),
        "reposfi_total_bytes": int(reposfi.get("total_bytes") or 0),
        "reposfi_used_bytes": int(reposfi.get("used_bytes") or 0),
        "reposfi_free_bytes": int(reposfi.get("free_bytes") or 0),
        "reposfi_used_percent": float(reposfi.get("use_percent") or 0),
        "appanalise_online": int(bool(appanalise["online"])),
        "appanalise_latency_ms": float(appanalise.get("latency_ms") or 0),
    }

    payload.update(
        {
            key.lower(): value
            for key, value in summary_metrics.items()
            if key != "CURRENT_MONTH_LABEL"
        }
    )
    payload.update(
        {
            f"webfusion_{name}_total": int(value)
            for name, value in usage_totals.items()
        }
    )
    payload.update(
        {
            f"webfusion_{name}_current_month": int(value)
            for name, value in usage_current_month.items()
        }
    )
    return payload


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

    record_page_view()
    usage_metrics = get_usage_metrics_snapshot()

    try:
        server_summary_metrics_payload = get_server_summary_metrics()
    except Exception:
        current_app.logger.exception("failed_to_build_server_summary_metrics_initial")

    return render_template(
        "server/server.html",
        server_overview=server_overview,
        server_summary_metrics=server_summary_metrics_payload,
        usage_metrics=usage_metrics,
        online_only=online_only,
        search=search,
    )


@server_bp.route("/server/zabbix_metrics", methods=["GET"])
def server_zabbix_metrics() -> object:
    """Return the `/server` indicators as one flat JSON payload for Zabbix."""

    try:
        return jsonify(_build_zabbix_metrics_payload())
    except Exception:
        current_app.logger.exception("failed_to_build_server_zabbix_metrics")
        return jsonify({"status": "error"}), 503


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


@server_bp.route("/api/server/usage-metrics", methods=["GET"])
def server_usage_metrics():
    """Return persisted WebFusion usage counters and breakdowns on demand."""

    try:
        return jsonify(get_usage_metrics_snapshot())
    except Exception:
        current_app.logger.exception("failed_to_build_server_usage_metrics")
        return (
            jsonify(
                {
                    "error": "failed_to_build_server_usage_metrics",
                }
            ),
            503,
        )


@server_bp.route("/api/server/usage-metrics/download-action", methods=["POST"])
def server_download_action_metric():
    """Count one UI-triggered repository download action.

    The front-end records the click intent before navigation starts. This is a
    safer operational signal than touching the NGINX download path and is
    deliberately defined as "action initiated", not "transfer completed".
    """

    try:
        current_value = record_download_action()
        return jsonify(
            {
                "ok": True,
                "download_action_count": current_value,
            }
        ), 202
    except Exception:
        current_app.logger.exception("failed_to_record_download_action_metric")
        return jsonify({"ok": False}), 503


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
