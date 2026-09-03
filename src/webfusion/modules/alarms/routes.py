"""Render the protected operational alarms page."""

from __future__ import annotations

from flask import Blueprint, current_app, render_template

from modules.alarms.service import list_alarms
from modules.server.usage_metrics import record_page_view
from modules.zabbix_configuration.service import ZabbixApiError


alarms_bp = Blueprint("alarms", __name__, url_prefix="/alarms")


@alarms_bp.route("/", methods=["GET"])
def alarms_dashboard():
    """Render current Zabbix problems tagged for appCataloga."""
    alarms = []
    error_message = None
    try:
        alarms = list_alarms()
    except ZabbixApiError:
        current_app.logger.exception("appcataloga_alarms_unavailable")
        error_message = "Não foi possível consultar os alarmes no Zabbix."

    record_page_view()
    return render_template(
        "alarms/alarms.html",
        alarms=alarms,
        error_message=error_message,
    )