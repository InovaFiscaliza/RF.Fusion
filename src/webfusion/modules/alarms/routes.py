"""Render the public read-only operational alarms page."""

from __future__ import annotations

from flask import Blueprint, current_app, render_template

from modules.alarms.service import get_zabbix_problems_url, list_alarms
from modules.server.usage_metrics import record_page_view
from modules.configuration.service import ZabbixApiError


alarms_bp = Blueprint("alarms", __name__, url_prefix="/alarms")


@alarms_bp.route("/", methods=["GET"])
def alarms_dashboard() -> str:
    """Render current Zabbix problems tagged for appCataloga.

    Args:
        None.

    Returns:
        str: Rendered alarms dashboard HTML with the current problem list and
        the general Zabbix Monitoring Problems URL.
    """
    alarms = []
    error_message = None
    zabbix_problems_url = None
    try:
        zabbix_problems_url = get_zabbix_problems_url()
        alarms = list_alarms()
    except ZabbixApiError:
        current_app.logger.exception("appcataloga_alarms_unavailable")
        error_message = "Não foi possível consultar os alarmes no Zabbix."

    record_page_view()
    return render_template(
        "alarms/alarms.html",
        alarms=alarms,
        error_message=error_message,
        zabbix_problems_url=zabbix_problems_url,
    )