"""
Shared host connectivity probes for appCataloga workers.

This module centralizes ICMP and short SSH confirmation helpers so recurring
maintenance and queued HOST_TASK workers classify host reachability the same
way. The probe is intentionally lightweight: it is suitable for quick
supervisory checks, not for long-lived data-plane sessions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
import ipaddress
import socket
import sys
import paramiko
from typing import Any, Optional

from ping3 import ping

from . import errors


BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)

CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402


NO_VALID_CONNECTIONS_REASON_MAP = {
    "timeout": "ssh_timeout",
    "refused": "ssh_connection_refused",
    "reset": "ssh_connection_reset",
    "unreachable": "ssh_network_unreachable",
    "mixed": "ssh_connection_mixed_failure",
    "unknown": "ssh_connection_failed",
}


@dataclass(frozen=True)
class ConnectivityProbeResult:
    """
    Immutable result returned by the short connectivity probe.

    The workers still consume dictionaries for backward compatibility, but
    building results through a small value object keeps the probe logic easier
    to read and less error-prone than hand-writing the same dict shape in every
    exception branch.
    """

    state: str
    reason: str
    icmp_online: bool
    ssh_online: bool
    error: Optional[str] = None

    def as_dict(self) -> dict:
        return {
            "state": self.state,
            "reason": self.reason,
            "icmp_online": self.icmp_online,
            "ssh_online": self.ssh_online,
            "error": self.error,
        }


def _probe_result(
    *,
    state: str,
    reason: str,
    icmp_online: bool,
    ssh_online: bool,
    error: Optional[str] = None,
) -> dict:
    """Return the canonical probe payload shared by host workers."""
    return ConnectivityProbeResult(
        state=state,
        reason=reason,
        icmp_online=icmp_online,
        ssh_online=ssh_online,
        error=error,
    ).as_dict()


def resolve_host_addresses(host_addr: str) -> list[str]:
    """
    Resolve a host into a stable list of candidate IP addresses.

    Some stations publish multiple A records, for example a stable 172.x.x.x
    operational network plus another VPN-facing IP that may not be reachable
    from the RF.Fusion VM. Resolving once and picking the preferred family
    avoids intra-probe DNS flapping where ICMP and SSH accidentally land on
    different endpoints.
    """
    try:
        infos = socket.getaddrinfo(host_addr, None, family=socket.AF_INET, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return [host_addr]
    except Exception:
        return [host_addr]

    addresses: list[str] = []
    for _family, _type, _proto, _canonname, sockaddr in infos:
        addr = sockaddr[0]
        if addr not in addresses:
            addresses.append(addr)

    if not addresses:
        return [host_addr]

    preferred_172 = []
    for addr in addresses:
        try:
            ip_obj = ipaddress.ip_address(addr)
        except ValueError:
            continue

        if ip_obj.version == 4 and str(ip_obj).startswith("172."):
            preferred_172.append(addr)

    # When a station exposes both VPN/public and operational network addresses,
    # the operational 172.x.x.x endpoint is the one we want to supervise and
    # use for SSH/SFTP sessions. Falling back to the other records here was the
    # source of many false offline/degraded diagnoses.
    if preferred_172:
        return preferred_172

    return addresses


def resolve_primary_host_address(host_addr: str) -> str:
    """
    Return the preferred concrete address for a host.

    This is the single-address counterpart of `resolve_host_addresses()` and is
    intended for long-lived data-plane connections such as backup/discovery SSH
    sessions. Keeping this choice centralized ensures control-plane probes and
    data-plane transports follow the same routing preference.
    """
    return resolve_host_addresses(host_addr)[0]


def _ping_address(addr: str, timeout_sec: float) -> bool:
    """Ping a concrete address without triggering another DNS lookup."""
    try:
        return ping(addr, timeout=timeout_sec) is not None
    except Exception:
        return False


def _connect_short_ssh_probe(addr: str, port: int, user: str, password: str) -> None:
    """
    Attempt the short supervisory SSH login used by host probes.

    The helper intentionally raises the original Paramiko/socket exception so
    callers can classify the failure without losing stage-specific details.
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(
            hostname=addr,
            port=int(port),
            username=user,
            password=password,
            timeout=k.HOST_CHECK_SSH_PROBE_TIMEOUT_SEC,
            banner_timeout=k.HOST_CHECK_SSH_PROBE_TIMEOUT_SEC,
            auth_timeout=k.HOST_CHECK_SSH_PROBE_TIMEOUT_SEC,
            look_for_keys=False,
            allow_agent=False,
        )
    finally:
        try:
            client.close()
        except Exception:
            pass


def _classify_generic_ssh_probe_failure(exc: Exception) -> dict:
    """
    Classify non-specialized SSH probe failures as degraded operational states.
    """
    if errors.is_timeout_like_sftp_init_error(exc):
        reason = "ssh_timeout"
    elif errors.is_transient_sftp_init_error(exc):
        reason = "ssh_transient_failure"
    else:
        reason = "ssh_unreachable"

    return _probe_result(
        state="degraded",
        reason=reason,
        icmp_online=True,
        ssh_online=False,
        error=str(exc),
    )


def _classify_no_valid_connections_failure(
    exc: paramiko.ssh_exception.NoValidConnectionsError,
) -> dict:
    """
    Translate Paramiko's aggregate connection wrapper into one stable reason.
    """
    details = errors.classify_no_valid_connections_error(exc)
    return _probe_result(
        state="degraded",
        reason=NO_VALID_CONNECTIONS_REASON_MAP.get(
            details["summary"],
            "ssh_connection_failed",
        ),
        icmp_online=True,
        ssh_online=False,
        error=str(exc),
    )


def persist_host_connectivity_state(
    *,
    db: Any,
    log: Any,
    host_id: int,
    was_offline: bool,
    online: bool,
    now: datetime,
) -> None:
    """
    Persist the HOST offline/online state machine and its side effects.

    Both the queued host worker and the recurring maintenance daemon need the
    same transition contract. Keeping it here prevents the two services from
    drifting semantically every time one branch gets touched under pressure.
    """
    if online:
        db.host_update(
            host_id=host_id,
            IS_OFFLINE=False,
            check_busy_timeout=True,
            DT_LAST_CHECK=now,
            NU_HOST_CHECK_ERROR=0,
        )

        if was_offline:
            log.event(
                "host_state_transition",
                host_id=host_id,
                previous_state="offline",
                current_state="online",
            )
            db.host_task_resume_by_host(host_id)
            db.file_task_resume_by_host(host_id)
            db.file_history_resume_by_host(host_id)

        return

    update_fields = {
        "IS_OFFLINE": True,
        "DT_LAST_CHECK": now,
    }

    if not was_offline:
        log.event(
            "host_state_transition",
            host_id=host_id,
            previous_state="online",
            current_state="offline",
        )
        db.host_task_suspend_by_host(host_id)
        db.file_task_suspend_by_host(host_id)
        db.file_history_suspend_by_host(host_id)
        update_fields.update(
            IS_BUSY=False,
            NU_PID=k.HOST_UNLOCKED_PID,
            NU_HOST_CHECK_ERROR=1,
            DT_LAST_FAIL=now,
        )

    db.host_update(host_id=host_id, **update_fields)


def is_host_online(host_addr: str, timeout_sec=None) -> bool:
    """
    Check host reachability through ICMP without surfacing ping library errors.

    This helper is deliberately tolerant: any ping library exception is treated
    as "not reachable" so callers can stay focused on state transitions.
    """
    timeout = k.ICMP_TIMEOUT_SEC if timeout_sec is None else timeout_sec
    for resolved_addr in resolve_host_addresses(host_addr):
        if _ping_address(resolved_addr, timeout):
            return True
    return False


def probe_host_operational_connectivity(
    addr: str,
    port: int,
    user: str,
    password: str,
) -> dict:
    """
    Classify host operational connectivity for discovery/backup purposes.

    The returned dictionary is designed for both decision-making and logging.
    It captures a coarse state plus enough detail to distinguish:
        - ICMP unreachable
        - SSH unavailable
        - SSH degraded/timeouting
        - authentication failures
        - fully operational hosts

    States:
        - online: ICMP and a short SSH login probe succeeded
        - offline: ICMP itself is unreachable
        - degraded: host pings, but SSH could not be confirmed by the short probe
        - auth_error: host is reachable, but credentials were explicitly rejected
    """
    resolved_addrs = resolve_host_addresses(addr)
    timeout = k.ICMP_TIMEOUT_SEC
    saw_icmp = False
    best_failure: dict | None = None

    for resolved_addr in resolved_addrs:
        if not _ping_address(resolved_addr, timeout):
            continue

        saw_icmp = True

        try:
            # This is a deliberately short supervisory login, not a real worker
            # session. Its only job is to answer "can discovery/backup start now?".
            _connect_short_ssh_probe(
                addr=resolved_addr,
                port=port,
                user=user,
                password=password,
            )
            result = _probe_result(
                state="online",
                reason="ssh_connect_ok",
                icmp_online=True,
                ssh_online=True,
            )
            result["resolved_addr"] = resolved_addr
            result["resolved_candidates"] = resolved_addrs
            return result
        except paramiko.AuthenticationException as e:
            if errors.is_auth_timeout_error(e):
                failure = _probe_result(
                    state="degraded",
                    reason="ssh_auth_timeout",
                    icmp_online=True,
                    ssh_online=True,
                    error=str(e),
                )
            else:
                failure = _probe_result(
                    state="auth_error",
                    reason="ssh_auth_failed",
                    icmp_online=True,
                    ssh_online=True,
                    error=str(e),
                )
        except paramiko.ssh_exception.NoValidConnectionsError as e:
            failure = _classify_no_valid_connections_failure(e)
        except Exception as e:
            failure = _classify_generic_ssh_probe_failure(e)

        failure["resolved_addr"] = resolved_addr
        failure["resolved_candidates"] = resolved_addrs

        if best_failure is None or best_failure["state"] != "auth_error":
            best_failure = failure
        elif failure["state"] == "auth_error":
            best_failure = failure

    if best_failure is not None:
        return best_failure

    result = _probe_result(
        state="offline",
        reason="icmp_unreachable",
        icmp_online=False,
        ssh_online=False,
    )
    result["resolved_candidates"] = resolved_addrs
    return result
