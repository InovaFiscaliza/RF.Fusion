"""Routes for the station-focused host page.

This module follows the common WebFusion pattern:

- render the page shell once with the host selector
- defer heavier diagnostics to focused JSON endpoints

That keeps the first render lightweight while still allowing rich drill-down
once the operator expands the host details.
"""

from flask import Blueprint, current_app, jsonify, render_template, request
from modules.host.service import (
    get_all_hosts,
    get_host_backup_error_overview,
    get_host_location_history_overview,
    get_host_processing_error_overview,
    get_host_statistics,
)

host_bp = Blueprint("host", __name__)


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
