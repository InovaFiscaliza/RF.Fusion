"""Routes for the RF.Fusion station configuration page."""

from __future__ import annotations

from flask import Blueprint, Response, current_app, redirect, render_template, request, url_for

from modules.server.usage_metrics import record_page_view
from modules.zabbix_configuration.service import (
    ACTION_RESTORE,
    ACTION_SAVE,
    TARGET_KIND_HOST,
    TARGET_KIND_TEMPLATE,
    ZabbixApiError,
    ZabbixConfigurationError,
    apply_macro_change,
    get_catalog,
    get_configuration,
)


zabbix_configuration_bp = Blueprint(
    "zabbix_configuration",
    __name__,
    url_prefix="/host-configuration",
)

ZABBIX_CONFIGURATION_AUTH_USERNAME = "admin"
ZABBIX_CONFIGURATION_AUTH_PASSWORD = "admin"
ZABBIX_CONFIGURATION_AUTH_REALM = "RF.Fusion Host Configuration"


def _zabbix_configuration_auth_failed() -> Response:
    """Trigger the browser basic-auth challenge for station configuration."""

    return Response(
        "Authentication required.",
        401,
        {"WWW-Authenticate": f'Basic realm="{ZABBIX_CONFIGURATION_AUTH_REALM}"'},
    )


def _has_valid_zabbix_configuration_credentials() -> bool:
    """Validate the bootstrap credentials for Zabbix macro changes."""

    auth = request.authorization
    if not auth:
        return False

    return (
        str(auth.username or "") == ZABBIX_CONFIGURATION_AUTH_USERNAME
        and str(auth.password or "") == ZABBIX_CONFIGURATION_AUTH_PASSWORD
    )


@zabbix_configuration_bp.before_request
def require_zabbix_configuration_auth() -> Response | None:
    """Protect configuration reads and writes with the shared module auth."""

    if not _has_valid_zabbix_configuration_credentials():
        return _zabbix_configuration_auth_failed()

    return None


@zabbix_configuration_bp.route("/", methods=["GET"])
def configuration_dashboard():
    """Render the selective Zabbix host/template configuration console."""
    target_kind = str(request.args.get("target_kind") or "").strip().lower()
    target_id = str(request.args.get("target_id") or "").strip()
    error_message = _notice_error_message(request.args.get("error"))
    success_message = _notice_success_message(request.args.get("notice"))
    catalog = {"hosts": [], "templates": []}
    configuration = None

    try:
        catalog = get_catalog()
        if target_kind and target_id:
            configuration = get_configuration(target_kind, target_id)
    except (ZabbixApiError, ZabbixConfigurationError) as error:
        error_message = str(error)
        current_app.logger.warning("zabbix_configuration_unavailable: %s", error)

    record_page_view()
    return render_template(
        "zabbix_configuration/zabbix_configuration.html",
        catalog=catalog,
        configuration=configuration,
        selected_target_kind=target_kind,
        selected_target_id=target_id,
        error_message=error_message,
        success_message=success_message,
        target_kind_host=TARGET_KIND_HOST,
        target_kind_template=TARGET_KIND_TEMPLATE,
        action_save=ACTION_SAVE,
        action_restore=ACTION_RESTORE,
    )


@zabbix_configuration_bp.route("/macro", methods=["POST"])
def update_macro():
    """Apply one explicit macro change and return to the selected target."""
    target_kind = str(request.form.get("target_kind") or "").strip().lower()
    target_id = str(request.form.get("target_id") or "").strip()
    macro_name = str(request.form.get("macro_name") or "").strip()
    action = str(request.form.get("action") or "").strip().lower()
    value = request.form.get("value")

    try:
        apply_macro_change(
            target_kind=target_kind,
            target_id=target_id,
            macro_name=macro_name,
            action=action,
            submitted_value=value,
        )
    except (ZabbixApiError, ZabbixConfigurationError) as error:
        current_app.logger.warning("zabbix_configuration_change_failed: %s", error)
        return redirect(
            url_for(
                "zabbix_configuration.configuration_dashboard",
                target_kind=target_kind,
                target_id=target_id,
                error="change_failed",
            )
        )

    notice = "restored" if action == ACTION_RESTORE else "saved"
    return redirect(
        url_for(
            "zabbix_configuration.configuration_dashboard",
            target_kind=target_kind,
            target_id=target_id,
            notice=notice,
        )
    )


def _notice_success_message(notice: str | None) -> str | None:
    """Translate a redirect status into the operator-facing confirmation."""
    messages = {
        "saved": "Configuração salva no Zabbix.",
        "restored": "A sobrescrita foi removida e a herança foi restaurada.",
    }
    return messages.get(str(notice or "").strip())


def _notice_error_message(error: str | None) -> str | None:
    """Keep redirect failures clear without exposing remote request details."""
    messages = {
        "change_failed": (
            "Não foi possível alterar a macro no Zabbix. Verifique as permissões "
            "da API e tente novamente."
        ),
    }
    return messages.get(str(error or "").strip())
