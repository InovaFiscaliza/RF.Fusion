"""Service boundary between WebFusion routes and the Zabbix API client."""

from __future__ import annotations

import copy
import os
import time
from pathlib import Path
from typing import Any

from zabbix.zabbix_api import (
    PROTECTED_MACRO_NAMES,
    ZabbixApiClient,
    ZabbixApiError,
)


CATALOG_CACHE_SECONDS = 60
TARGET_KIND_HOST = "host"
TARGET_KIND_TEMPLATE = "template"
ACTION_SAVE = "save"
ACTION_RESTORE = "restore"
BACKUP_PATH_MACRO = "{$BACKUP_PATH}"
BACKUP_EXTENSION_MACRO = "{$BACKUP_EXTENSION}"
OPERATIONAL_HOST_FIELD_BY_MACRO = {
    "{$SSH_PORT}": "NA_HOST_PORT",
    "{$SSH_USER}": "NA_HOST_USER",
    "{$SSH_PASSWD}": "NA_HOST_PASSWORD",
}
ZABBIX_SECRET_FILE = (
    Path(__file__).resolve().parents[3] / "zabbix" / ".secret" / "zabbix_api.env"
)

_catalog_cache: dict[str, Any] = {"expires_at": 0.0, "value": None}


class ZabbixConfigurationError(RuntimeError):
    """Represent an invalid or unavailable configuration operation."""

    def __init__(self, message: str, *, notice_code: str = "change_failed") -> None:
        super().__init__(message)
        self.notice_code = notice_code


def get_catalog() -> dict[str, list[dict[str, Any]]]:
    """Return a short-lived cache of selectable RF.Fusion targets."""
    now = time.monotonic()
    cached_value = _catalog_cache["value"]
    if cached_value is not None and now < _catalog_cache["expires_at"]:
        return copy.deepcopy(cached_value)

    catalog = _build_client().list_catalog()
    _catalog_cache["value"] = catalog
    _catalog_cache["expires_at"] = now + CATALOG_CACHE_SECONDS
    return copy.deepcopy(catalog)


def get_configuration(target_kind: str, target_id: str) -> dict[str, Any]:
    """Load one approved host or template configuration on demand."""
    normalized_kind = _normalize_target_kind(target_kind)
    normalized_id = _normalize_target_id(target_id)
    catalog = get_catalog()
    _ensure_target_is_managed(normalized_kind, normalized_id, catalog)
    client = _build_client()
    if normalized_kind == TARGET_KIND_HOST:
        configuration = client.get_host_configuration(normalized_id)
        configuration["operational_host_id"] = _get_operational_host_id(configuration)
        return configuration
    return client.get_template_configuration(normalized_id)


def _get_operational_host_id(configuration: dict[str, Any]) -> int | None:
    """Read the BPDATA host identifier exposed by the appCataloga macro."""
    for macro in configuration.get("macros") or []:
        if macro.get("name") != "{$HOST_ID}":
            continue
        raw_value = str(macro.get("editable_value") or "").strip()
        if raw_value.isdigit() and int(raw_value) > 0:
            return int(raw_value)
    return None


def get_hosts_backup_defaults(
    host_ids: list[str | int],
) -> dict[str, dict[str, str | None]]:
    """Load effective backup defaults for managed hosts in one Zabbix batch."""
    requested_host_ids = sorted(
        {
            str(host_id)
            for host_id in host_ids
            if str(host_id).isdigit() and int(str(host_id)) > 0
        },
        key=int,
    )
    if not requested_host_ids:
        return {}

    catalog = get_catalog()
    managed_host_ids = {
        str(host.get("hostid"))
        for host in catalog["hosts"]
    }
    selected_host_ids = [
        host_id for host_id in requested_host_ids if host_id in managed_host_ids
    ]
    if not selected_host_ids:
        return {}

    configurations = _build_client().get_host_configurations(selected_host_ids)
    return {
        host_id: _extract_backup_defaults(configuration)
        for host_id, configuration in configurations.items()
    }


def apply_macro_change(
    *,
    target_kind: str,
    target_id: str,
    macro_name: str,
    action: str,
    submitted_value: str | None = None,
) -> bool:
    """Save a direct override or restore the macro inheritance chain."""
    normalized_kind = _normalize_target_kind(target_kind)
    normalized_id = _normalize_target_id(target_id)
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {ACTION_SAVE, ACTION_RESTORE}:
        raise ZabbixConfigurationError("A ação de configuração informada não é válida.")

    configuration = get_configuration(normalized_kind, normalized_id)
    macro = _find_macro(configuration, macro_name)
    if macro["name"] in PROTECTED_MACRO_NAMES:
        raise ZabbixConfigurationError(
            "O identificador primário do host é controlado pelo Zabbix e não pode ser alterado."
        )
    _validate_secret_restore_sync(
        target_kind=normalized_kind,
        macro=macro,
        action=normalized_action,
    )
    _validate_operational_sync_target(
        target_kind=normalized_kind,
        configuration=configuration,
        macro=macro,
    )
    client = _build_client()

    if normalized_action == ACTION_RESTORE:
        if not macro["is_direct_on_target"]:
            raise ZabbixConfigurationError(
                "Esta macro já utiliza a configuração herdada."
            )
        client.delete_macro(macro_id=macro["source_macro_id"])
        _clear_catalog_cache()
        restored_configuration = get_configuration(normalized_kind, normalized_id)
        restored_macro = _find_macro(restored_configuration, macro_name)
        return _sync_operational_host_macro(
            target_kind=normalized_kind,
            configuration=restored_configuration,
            macro=restored_macro,
            value=restored_macro.get("editable_value"),
        )

    if not macro["accepts_value"]:
        raise ZabbixConfigurationError(
            "Macros vinculadas a cofre não podem ser alteradas nesta tela."
        )
    value = "" if submitted_value is None else str(submitted_value)
    if macro["type"] == "1" and not value:
        raise ZabbixConfigurationError(
            "Informe um novo valor para atualizar uma macro secreta."
        )
    _validate_operational_macro_value(macro=macro, value=value)
    if macro["is_direct_on_target"]:
        client.update_macro(macro_id=macro["source_macro_id"], value=value)
    else:
        client.create_macro(
            owner_id=normalized_id,
            macro_name=macro["name"],
            macro_type=macro["type"],
            value=value,
        )

    _clear_catalog_cache()
    return _sync_operational_host_macro(
        target_kind=normalized_kind,
        configuration=configuration,
        macro=macro,
        value=value,
    )


def _validate_secret_restore_sync(
    *,
    target_kind: str,
    macro: dict[str, Any],
    action: str,
) -> None:
    """Prevent an inherited secret from leaving Zabbix and BPDATA divergent."""
    if (
        target_kind == TARGET_KIND_HOST
        and action == ACTION_RESTORE
        and macro.get("name") == "{$SSH_PASSWD}"
        and str(macro.get("type")) == "1"
    ):
        raise ZabbixConfigurationError(
            "A senha SSH herdada não pode ser lida pela API do Zabbix. "
            "Para manter o RF.Fusion sincronizado, mantenha a sobrescrita ou "
            "informe a senha desejada novamente.",
            notice_code="secret_restore_requires_sync",
        )


def _sync_operational_host_macro(
    *,
    target_kind: str,
    configuration: dict[str, Any],
    macro: dict[str, Any],
    value: Any,
) -> bool:
    """Synchronize a host connection macro after Zabbix accepts its change."""
    column = OPERATIONAL_HOST_FIELD_BY_MACRO.get(str(macro.get("name") or ""))
    if target_kind != TARGET_KIND_HOST or column is None:
        return False

    host_id = _require_operational_host_id(configuration)

    normalized_value = _normalize_operational_host_value(column=column, value=value)
    try:
        _persist_operational_host_connection(
            host_id=host_id,
            column=column,
            value=normalized_value,
        )
    except Exception as error:
        raise ZabbixConfigurationError(
            "A macro foi atualizada no Zabbix, mas o host operacional não pôde ser "
            "sincronizado. Não inicie tarefas até corrigir a sincronização.",
            notice_code="operational_sync_failed",
        ) from error
    return True


def _validate_operational_sync_target(
    *,
    target_kind: str,
    configuration: dict[str, Any],
    macro: dict[str, Any],
) -> None:
    """Reject an operational macro write without a valid BPDATA host target."""
    if (
        target_kind == TARGET_KIND_HOST
        and str(macro.get("name") or "") in OPERATIONAL_HOST_FIELD_BY_MACRO
    ):
        _require_operational_host_id(configuration)


def _require_operational_host_id(configuration: dict[str, Any]) -> int:
    """Return the BPDATA host key required by a connection synchronization."""
    host_id = configuration.get("operational_host_id")
    if not isinstance(host_id, int) or host_id <= 0:
        raise ZabbixConfigurationError(
            "A estação não possui um identificador operacional válido para sincronização.",
            notice_code="operational_sync_failed",
        )
    return host_id


def _validate_operational_macro_value(*, macro: dict[str, Any], value: Any) -> None:
    """Validate a connection value before changing either configuration source."""
    column = OPERATIONAL_HOST_FIELD_BY_MACRO.get(str(macro.get("name") or ""))
    if column is not None:
        _normalize_operational_host_value(column=column, value=value)


def _persist_operational_host_connection(
    *,
    host_id: int,
    column: str,
    value: int | str,
) -> None:
    """Load the WebFusion database helper only for a write operation."""
    from db import update_host_connection_value

    update_host_connection_value(
        host_id=host_id,
        column=column,
        value=value,
    )


def _normalize_operational_host_value(*, column: str, value: Any) -> int | str:
    """Validate the value before it reaches a BPDATA connection column."""
    if column != "NA_HOST_PORT":
        return str(value or "")

    raw_port = str(value or "").strip()
    if not raw_port.isdigit() or not 1 <= int(raw_port) <= 65535:
        raise ZabbixConfigurationError(
            "A porta SSH deve ser um número entre 1 e 65535.",
        )
    return int(raw_port)


def _build_client() -> ZabbixApiClient:
    """Build the client only while a request actually needs Zabbix."""
    settings = _get_zabbix_settings()
    timeout_raw = settings.get("ZABBIX_API_TIMEOUT_SECONDS", "10")
    try:
        timeout_seconds = int(timeout_raw)
    except ValueError:
        timeout_seconds = 10

    return ZabbixApiClient(
        settings.get("ZABBIX_API_URL", ""),
        settings.get("ZABBIX_API_TOKEN", ""),
        timeout_seconds=timeout_seconds,
    )


def _get_zabbix_settings() -> dict[str, str]:
    """Load deployment settings without exposing a token through the process."""
    file_settings = _read_local_secret_file()
    setting_names = (
        "ZABBIX_API_URL",
        "ZABBIX_API_TOKEN",
        "ZABBIX_API_TIMEOUT_SECONDS",
    )
    return {
        name: str(os.getenv(name) or file_settings.get(name) or "").strip()
        for name in setting_names
    }


def _read_local_secret_file() -> dict[str, str]:
    """Read the mounted local secret file when container environment lacks it."""
    try:
        lines = ZABBIX_SECRET_FILE.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return {}
    except OSError as error:
        raise ZabbixConfigurationError(
            "Não foi possível ler a configuração local da API do Zabbix."
        ) from error

    values: dict[str, str] = {}
    for line in lines:
        stripped_line = line.strip()
        if not stripped_line or stripped_line.startswith("#"):
            continue
        key, separator, value = stripped_line.partition("=")
        if separator and key in {
            "ZABBIX_API_URL",
            "ZABBIX_API_TOKEN",
            "ZABBIX_API_TIMEOUT_SECONDS",
        }:
            values[key] = value.strip()
    return values


def _normalize_target_kind(target_kind: str) -> str:
    """Keep target selection within hosts and managed templates."""
    normalized = str(target_kind or "").strip().lower()
    if normalized not in {TARGET_KIND_HOST, TARGET_KIND_TEMPLATE}:
        raise ZabbixConfigurationError("Selecione um host ou template válido.")
    return normalized


def _normalize_target_id(target_id: str) -> str:
    """Reject arbitrary identifiers before calling the remote API."""
    value = str(target_id or "").strip()
    if not value.isdigit() or int(value) <= 0:
        raise ZabbixConfigurationError("O identificador selecionado não é válido.")
    return value


def _ensure_target_is_managed(
    target_kind: str,
    target_id: str,
    catalog: dict[str, list[dict[str, Any]]],
) -> None:
    """Prevent form tampering from reaching unrelated Zabbix objects."""
    collection_name = "hosts" if target_kind == TARGET_KIND_HOST else "templates"
    id_field = "hostid" if target_kind == TARGET_KIND_HOST else "templateid"
    if not any(str(item.get(id_field)) == target_id for item in catalog[collection_name]):
        raise ZabbixConfigurationError(
            "O item selecionado não pertence ao escopo RF.Fusion gerenciado."
        )


def _find_macro(configuration: dict[str, Any], macro_name: str) -> dict[str, Any]:
    """Resolve the submitted macro against the current API state."""
    normalized_name = str(macro_name or "").strip()
    for macro in configuration.get("macros", []):
        if macro["name"] == normalized_name:
            return macro
    raise ZabbixConfigurationError(
        "A macro selecionada não faz parte da configuração atual do alvo."
    )


def _extract_backup_defaults(
    configuration: dict[str, Any],
) -> dict[str, str | None]:
    """Expose only plain-text backup values from an effective host config."""
    field_by_macro = {
        BACKUP_PATH_MACRO: "file_path",
        BACKUP_EXTENSION_MACRO: "extension",
    }
    defaults: dict[str, str | None] = {
        "file_path": None,
        "extension": None,
    }

    for macro in configuration.get("macros", []):
        field_name = field_by_macro.get(str(macro.get("name") or ""))
        if not field_name:
            continue
        if str(macro.get("type")) != "0" or not macro.get("accepts_value"):
            continue

        value = str(macro.get("editable_value") or "").strip()
        if value:
            defaults[field_name] = value

    return defaults


def _clear_catalog_cache() -> None:
    """Drop target metadata after a successful remote configuration change."""
    _catalog_cache["value"] = None
    _catalog_cache["expires_at"] = 0.0


__all__ = [
    "ACTION_RESTORE",
    "ACTION_SAVE",
    "TARGET_KIND_HOST",
    "TARGET_KIND_TEMPLATE",
    "ZabbixApiError",
    "ZabbixConfigurationError",
    "apply_macro_change",
    "get_catalog",
    "get_configuration",
    "get_hosts_backup_defaults",
]
