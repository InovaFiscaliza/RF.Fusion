"""Read current RF.Fusion container health without querying application data."""

from __future__ import annotations

import json
import os
import subprocess
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Lock, RLock
from time import monotonic
from typing import Any


DEFAULT_TIMEOUT_SECONDS = 3.0
DEFAULT_CACHE_SECONDS = 15.0
MIN_CACHE_SECONDS = 5.0
MAX_CACHE_SECONDS = 60.0
ALLOWED_STATUSES = {"healthy", "degraded", "unavailable"}
_SNAPSHOT_CACHE_LOCK = RLock()
_SNAPSHOT_REFRESH_LOCK = Lock()
_SNAPSHOT_CACHE: dict[str, Any] = {
    "payload": None,
    "expires_at": 0.0,
}


def _read_timeout_seconds() -> float:
    """Return the bounded timeout used by every health subprocess."""
    raw_timeout = os.getenv("WEBFUSION_RUNTIME_HEALTH_TIMEOUT_SECONDS", "3")

    try:
        return min(max(float(raw_timeout), 1.0), 5.0)
    except ValueError:
        return DEFAULT_TIMEOUT_SECONDS


def _read_cache_seconds() -> float:
    """Return the bounded lifetime for one collected health snapshot."""
    raw_cache_seconds = os.getenv(
        "WEBFUSION_RUNTIME_HEALTH_CACHE_SECONDS",
        str(DEFAULT_CACHE_SECONDS),
    )

    try:
        return min(
            max(float(raw_cache_seconds), MIN_CACHE_SECONDS),
            MAX_CACHE_SECONDS,
        )
    except ValueError:
        return DEFAULT_CACHE_SECONDS


def _run_command(command: list[str], *, timeout_seconds: float) -> dict[str, Any]:
    """Run one health command and return its JSON response or a current failure."""
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            check=False,
            text=True,
            timeout=timeout_seconds,
        )
    except FileNotFoundError:
        return {
            "status": "unavailable",
            "checks": [],
            "message": "Cliente SSH ou shell de verificação indisponível.",
        }
    except subprocess.TimeoutExpired:
        return {
            "status": "unavailable",
            "checks": [],
            "message": f"Sem resposta em até {timeout_seconds:g} segundos.",
        }
    except OSError:
        return {
            "status": "unavailable",
            "checks": [],
            "message": "Não foi possível iniciar a verificação de saúde.",
        }

    if result.returncode != 0:
        stderr = (result.stderr or "").lower()
        if "permission denied" in stderr:
            message = "Autenticação SSH recusada."
        elif "connection refused" in stderr:
            message = "Conexão SSH recusada pelo container."
        elif "no route to host" in stderr or "could not resolve hostname" in stderr:
            message = "Container não alcançável pela rede interna."
        else:
            message = "O container não retornou a verificação de saúde."
        return {"status": "unavailable", "checks": [], "message": message}

    try:
        payload = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError):
        return {
            "status": "unavailable",
            "checks": [],
            "message": "Resposta de saúde inválida recebida do container.",
        }

    if not isinstance(payload, dict) or payload.get("status") not in ALLOWED_STATUSES:
        return {
            "status": "unavailable",
            "checks": [],
            "message": "Resposta de saúde incompleta recebida do container.",
        }

    checks = payload.get("checks")
    payload["checks"] = checks if isinstance(checks, list) else []
    payload.setdefault("message", None)
    return payload


def _unconfigured_component(component: str, label: str, message: str) -> dict[str, Any]:
    """Return a current configuration failure without opening an SSH session."""
    return {
        "component": component,
        "label": label,
        "status": "unconfigured",
        "checks": [],
        "message": message,
    }


def _run_remote_component(
    *,
    component: str,
    label: str,
    host: str,
    port: int,
    remote_script: str,
    key_path: str,
    known_hosts_path: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Execute one forced read-only health command through SSH."""
    if not host:
        return _unconfigured_component(component, label, "Host interno não configurado.")

    if not os.path.isfile(key_path):
        return _unconfigured_component(
            component,
            label,
            "Chave de saúde SSH não está montada no WebFusion.",
        )

    if not os.path.isfile(known_hosts_path):
        return _unconfigured_component(
            component,
            label,
            "Arquivo known_hosts da saúde SSH não está montado no WebFusion.",
        )

    command = [
        "ssh",
        "-i",
        key_path,
        "-o",
        "BatchMode=yes",
        "-o",
        "PasswordAuthentication=no",
        "-o",
        "KbdInteractiveAuthentication=no",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts_path}",
        "-o",
        f"ConnectTimeout={max(1, int(timeout_seconds))}",
        "-p",
        str(port),
        f"root@{host}",
        remote_script,
    ]
    payload = _run_command(command, timeout_seconds=timeout_seconds)
    payload["component"] = component
    payload["label"] = label
    return payload


def _run_local_webfusion_health(*, timeout_seconds: float) -> dict[str, Any]:
    """Run the WebFusion script locally instead of opening an SSH loopback."""
    script_path = os.getenv(
        "WEBFUSION_RUNTIME_HEALTH_LOCAL_SCRIPT",
        "/RF.Fusion/src/webfusion/scripts/webfusion_runtime_health.sh",
    ).strip()
    payload = _run_command(["bash", script_path], timeout_seconds=timeout_seconds)
    payload["component"] = "webfusion"
    payload["label"] = "WebFusion"
    return payload


def _read_port(environment_name: str, default: int) -> int:
    """Read one SSH port while retaining a safe deployment default."""
    raw_port = os.getenv(environment_name, str(default))

    try:
        port = int(raw_port)
    except ValueError:
        return default

    return port if 1 <= port <= 65535 else default


def _overall_status(components: list[dict[str, Any]]) -> str:
    """Summarize the current component states without retaining stale values."""
    statuses = {component.get("status") for component in components}

    if statuses == {"healthy"}:
        return "healthy"
    if statuses and statuses <= {"unavailable"}:
        return "unavailable"
    return "degraded"


def _collect_runtime_health_snapshot() -> dict[str, Any]:
    """Collect one fresh, database-independent container health snapshot."""
    timeout_seconds = _read_timeout_seconds()
    key_path = os.getenv(
        "WEBFUSION_RUNTIME_HEALTH_SSH_KEY_PATH",
        "/run/secrets/rffusion_runtime_health_ed25519",
    ).strip()
    known_hosts_path = os.getenv(
        "WEBFUSION_RUNTIME_HEALTH_KNOWN_HOSTS_PATH",
        "/run/secrets/rffusion_runtime_health_known_hosts",
    ).strip()

    remote_components = (
        {
            "component": "appcataloga",
            "label": "appCataloga",
            "host": os.getenv("WEBFUSION_RUNTIME_HEALTH_APPCATALOGA_HOST", "10.88.0.2").strip(),
            "port": _read_port("WEBFUSION_RUNTIME_HEALTH_APPCATALOGA_PORT", 22),
            "remote_script": "/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shell/appCataloga_runtime_health.sh",
        },
        {
            "component": "mariadb",
            "label": "MariaDB",
            "host": os.getenv("WEBFUSION_RUNTIME_HEALTH_MARIADB_HOST", "10.88.0.33").strip(),
            "port": _read_port("WEBFUSION_RUNTIME_HEALTH_MARIADB_PORT", 2828),
            "remote_script": "/RFFusion/src/mariadb/scripts/mariadb_runtime_health.sh",
        },
    )

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="runtime-health") as executor:
        futures = [
            executor.submit(
                _run_remote_component,
                **component,
                key_path=key_path,
                known_hosts_path=known_hosts_path,
                timeout_seconds=timeout_seconds,
            )
            for component in remote_components
        ]
        local_component = _run_local_webfusion_health(timeout_seconds=timeout_seconds)
        remote_results = [future.result() for future in futures]

    components = [local_component, *remote_results]
    return {
        "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": _overall_status(components),
        "components": components,
    }


def _get_cached_snapshot(
    *,
    now: float,
    allow_expired: bool = False,
) -> dict[str, Any] | None:
    """Return a copy of the cached snapshot when it is usable."""
    with _SNAPSHOT_CACHE_LOCK:
        cached_payload = _SNAPSHOT_CACHE["payload"]
        expires_at = float(_SNAPSHOT_CACHE["expires_at"])

        if not isinstance(cached_payload, dict):
            return None

        if not allow_expired and expires_at <= now:
            return None

        return deepcopy(cached_payload)


def _refresh_runtime_health_snapshot() -> dict[str, Any]:
    """Collect and cache one current runtime health snapshot."""
    snapshot = _collect_runtime_health_snapshot()

    with _SNAPSHOT_CACHE_LOCK:
        _SNAPSHOT_CACHE["payload"] = snapshot
        _SNAPSHOT_CACHE["expires_at"] = monotonic() + _read_cache_seconds()

    return deepcopy(snapshot)


def get_runtime_health_snapshot() -> dict[str, Any]:
    """Return a recent snapshot while limiting command execution."""
    now = monotonic()
    cached_snapshot = _get_cached_snapshot(now=now)
    if cached_snapshot is not None:
        return cached_snapshot

    if not _SNAPSHOT_REFRESH_LOCK.acquire(blocking=False):
        cached_snapshot = _get_cached_snapshot(now=now, allow_expired=True)
        if cached_snapshot is not None:
            return cached_snapshot

        with _SNAPSHOT_REFRESH_LOCK:
            cached_snapshot = _get_cached_snapshot(now=monotonic())
            if cached_snapshot is not None:
                return cached_snapshot
            return _refresh_runtime_health_snapshot()

    try:
        cached_snapshot = _get_cached_snapshot(now=monotonic())
        if cached_snapshot is not None:
            return cached_snapshot
        return _refresh_runtime_health_snapshot()
    finally:
        _SNAPSHOT_REFRESH_LOCK.release()
