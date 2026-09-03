"""Build configured Zabbix clients for WebFusion feature modules.

Reads environment settings or the local secret file without exposing the token.
"""

from __future__ import annotations

import os
from pathlib import Path

from zabbix_api.client import ZabbixApiClient, ZabbixApiError


ZABBIX_SECRET_FILE = (
    Path(__file__).resolve().parents[2] / "zabbix" / ".secret" / "zabbix_api.env"
)
SETTING_NAMES = (
    "ZABBIX_API_URL",
    "ZABBIX_API_TOKEN",
    "ZABBIX_API_TIMEOUT_SECONDS",
)


def build_client() -> ZabbixApiClient:
    """Build an authenticated client from the available configuration.

    Args:
            None.

    Returns:
        ZabbixApiClient: Client with configured URL, token, and timeout.

    Raises:
            ZabbixApiError: If the URL or token is not configured.
    """
    settings = get_settings()
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


def get_api_url() -> str:
    """Return the configured API URL without exposing the token.

    Args:
        None.

    Returns:
        str: API URL, or an empty string when it is not configured.
    """
    return get_settings().get("ZABBIX_API_URL", "")


def get_settings() -> dict[str, str]:
    """Load settings with environment values taking priority over the secret.

    Args:
        None.

    Returns:
        dict[str, str]: Dictionary with `ZABBIX_API_URL`, `ZABBIX_API_TOKEN`,
        and `ZABBIX_API_TIMEOUT_SECONDS`; absent values are empty strings.

    Raises:
            ZabbixApiError: If the secret file cannot be read.
    """
    file_settings = _read_secret_file()
    # Deployment settings must override the mounted fallback secret.
    return {
        name: str(os.getenv(name) or file_settings.get(name) or "").strip()
        for name in SETTING_NAMES
    }


def _read_secret_file() -> dict[str, str]:
    """Read the mounted secret when the environment lacks configuration.

    Args:
        None.

    Returns:
        dict[str, str]: Recognized secret-file values, or an empty dictionary
        when the file does not exist.

    Raises:
            ZabbixApiError: If another file-read error occurs.
    """
    try:
        lines = ZABBIX_SECRET_FILE.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        # Containers may provide every setting through their environment.
        return {}
    except OSError as error:
        raise ZabbixApiError(
            "Não foi possível ler a configuração local da API do Zabbix."
        ) from error

    values: dict[str, str] = {}
    for line in lines:
        stripped_line = line.strip()
        if not stripped_line or stripped_line.startswith("#"):
            continue
        key, separator, value = stripped_line.partition("=")
        # Ignore unrelated values if the secret file is shared by deployments.
        if separator and key in SETTING_NAMES:
            values[key] = value.strip()
    return values