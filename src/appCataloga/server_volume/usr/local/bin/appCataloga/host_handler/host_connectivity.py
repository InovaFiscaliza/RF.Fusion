"""
Shared host connectivity probes for appCataloga workers.

This module centralizes ICMP and short SSH confirmation helpers so recurring
maintenance and queued HOST_TASK workers classify host reachability the same
way. The probe is intentionally lightweight: it is suitable for quick
supervisory checks, not for long-lived data-plane sessions.
"""

from __future__ import annotations

from datetime import datetime
import ipaddress
import os
import socket
import sys
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.dbHandlerBKP import dbHandlerBKP
    from shared.logging_utils import log as logger_type

from ping3 import ping

from .host_ssh_utils import (
    ConnectivityProbePayload,
    persist_auth_error,
    ssh_probe,
)


BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)

CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402

# =====================================================================
# Host address resolution
# =====================================================================

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
        literal_ip = ipaddress.ip_address(host_addr)
    except ValueError:
        literal_ip = None

    # Literal IPs bypass DNS. This keeps explicit host entries deterministic
    # and avoids surprising resolver behavior when operators pin a station.
    if literal_ip is not None:
        return [str(literal_ip)]

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
            if ip_obj.version == 4 and str(ip_obj).startswith("172."):
                preferred_172.append(addr)
        except ValueError:
            pass

    # When a station exposes both VPN/public and operational network addresses,
    # the operational 172.x.x.x endpoint is the one we want. Falling back to
    # other records was the source of many false offline/degraded diagnoses.
    return preferred_172 if preferred_172 else addresses


def build_connectivity_probe_fields(
    *,
    host_id: int,
    addr: str,
    probe: ConnectivityProbePayload,
    port: int | None = None,
    host_name: str | None = None,
) -> dict:
    """
    Build one structured connectivity-probe payload.

    The probe helpers return data; this helper maps that data into the stable
    field names shared by host workers and maintenance flows.
    """
    payload = {
        "component": "host_connectivity",
        "operation": "probe",
        "host_id": host_id,
        "address": addr,
        "state": probe["state"],
        "reason": probe["reason"],
        "online": probe["state"] == "online",
        "icmp_online": probe["icmp_online"],
        "ssh_online": probe["ssh_online"],
        "error": probe["error"],
    }

    if port is not None:
        payload["port"] = port

    if host_name is not None:
        payload["host"] = host_name

    if "resolved_addr" in probe:
        payload["resolved_addr"] = probe["resolved_addr"]

    if "resolved_candidates" in probe:
        payload["resolved_candidates"] = probe["resolved_candidates"]

    return payload


def _ping_address(addr: str, timeout_sec: float) -> bool:
    """Ping a concrete address without triggering another DNS lookup."""
    try:
        return ping(addr, timeout=timeout_sec) is not None
    except Exception:
        return False


def persist_host_connectivity_state(
    *,
    db: dbHandlerBKP,
    log: logger_type,
    host_id: int,
    was_offline: bool,
    online: bool,
    now: datetime,
    resume_dependent_tasks: bool = False,
) -> None:
    """
    Persist the HOST offline/online state machine and its side effects.

    Both the queued host worker and the recurring maintenance daemon need the
    same transition contract. Keeping it here prevents the two services from
    drifting semantically every time one branch gets touched under pressure.

    Truth table:
        - `(was_offline=0, online=0)`:
          transition from online -> offline. Suspend dependent queues and
          initialize the definitive offline counter/timestamps.
        - `(was_offline=0, online=1)`:
          steady online refresh. Keep queues as they are, unless the caller
          explicitly asks to resume previously suspended work after an
          operational failure such as SSH/auth degradation.
        - `(was_offline=1, online=0)`:
          steady offline refresh. Keep the host offline and update only the
          observation timestamp; do not suspend or increment again.
        - `(was_offline=1, online=1)`:
          transition from offline -> online. Resume dependent queues and clear
          the definitive offline/error markers.

    This helper only persists definitive online/offline outcomes. Ambiguous
    states such as SSH degradation are handled by the caller before this point.
    """
    def _suspend_host_dependent_work() -> None:
        """
        Reassert suspension of host-dependent queues for an offline host.

        This is intentionally safe to call on every offline confirmation. The
        DB helpers only touch DISCOVERY/BACKUP and only when they are still in
        live states such as PENDING/RUNNING, so the calls act as an idempotent
        reconciliation pass after restarts or manual requeues.
        """
        db.host_task_suspend_by_host(host_id)
        db.file_task_suspend_by_host(host_id)
        db.file_history_suspend_by_host(host_id)

    if online:
        # `(0,1)` and `(1,1)` both land here. In both cases the persisted
        # result is "host is operational now", so we explicitly assign the
        # fields instead of relying on host_update's additive integer mode.
        db.host_update(
            host_id=host_id,
            reset=True,
            IS_OFFLINE=False,
            check_busy_timeout=True,
            DT_LAST_CHECK=now,
            NU_HOST_CHECK_ERROR=0,
        )

        if was_offline:
            # `(1,1)`: this is the real recovery edge. The host was offline
            # before this probe and is online now, so downstream queues may be
            # resumed exactly once on the transition.
            log.event(
                "host_state_transition",
                component="host_connectivity",
                operation="persist_state",
                host_id=host_id,
                previous_state="offline",
                current_state="online",
            )
            db.host_task_resume_by_host(host_id)
            db.file_task_resume_by_host(host_id)
            db.file_history_resume_by_host(host_id)
        elif resume_dependent_tasks:
            # Some failures, such as explicit SSH authentication problems, can
            # suspend work without ever marking the host offline. A later
            # successful operational probe should be allowed to resume that
            # suspended work even though `(was_offline, online) == (0,1)`.
            log.event(
                "host_operational_recovery",
                component="host_connectivity",
                operation="persist_state",
                host_id=host_id,
                previous_state="degraded_or_auth",
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
        # `(0,0)`: this is the real online -> offline transition. We suspend
        # dependent queues once, release any lingering BUSY ownership, and set
        # the definitive offline counter to 1. `reset=True` below is important
        # because host_update would otherwise *increment* the positive integer.
        log.event(
            "host_state_transition",
            component="host_connectivity",
            operation="persist_state",
            host_id=host_id,
            previous_state="online",
            current_state="offline",
        )
        _suspend_host_dependent_work()
        update_fields.update(
            IS_BUSY=False,
            NU_PID=k.HOST_UNLOCKED_PID,
            NU_HOST_CHECK_ERROR=1,
            DT_LAST_FAIL=now,
        )
    else:
        # `(1,0)`: the host was already offline and remains offline. Reassert
        # the suspension contract so host-dependent work that was reset or
        # manually requeued while the app was down does not remain PENDING.
        _suspend_host_dependent_work()

    db.host_update(host_id=host_id, reset=True, **update_fields)


def is_host_online(host_addr: str, timeout_sec: float | None = None) -> bool:
    """
    Check host reachability through ICMP without surfacing ping library errors.

    This helper is deliberately tolerant: any ping library exception is treated
    as "not reachable" so callers can stay focused on state transitions.
    """
    timeout = k.ICMP_TIMEOUT_SEC if timeout_sec is None else timeout_sec
    return any(_ping_address(addr, timeout) for addr in resolve_host_addresses(host_addr))


def probe_host_connectivity(
    addr: str,
    port: int,
    user: str,
    password: str,
) -> ConnectivityProbePayload:
    """
    Classify host operational connectivity for discovery/backup purposes.

    States:
        - online:     ICMP and a short SSH login probe succeeded
        - offline:    ICMP itself is unreachable
        - degraded:   host pings, but SSH could not be confirmed
        - auth_error: host is reachable, but credentials were rejected
    """
    resolved_addrs = resolve_host_addresses(addr)
    reachable = [a for a in resolved_addrs if _ping_address(a, k.ICMP_TIMEOUT_SEC)]

    if not reachable:
        return {
            "state": k.HOST_CONN_OFFLINE,
            "reason": "icmp_unreachable",
            "icmp_online": False,
            "ssh_online": False,
            "error": None,
            "resolved_candidates": resolved_addrs,
        }

    best_failure: dict | None = None

    for resolved_addr in reachable:
        result = ssh_probe(addr=resolved_addr, port=port, user=user, password=password)
        result["resolved_addr"] = resolved_addr
        result["resolved_candidates"] = resolved_addrs

        if result["state"] == k.HOST_CONN_ONLINE:
            return result

        # Auth rejection outranks degradation — operators need to fix credentials.
        if best_failure is None or result["state"] == k.HOST_CONN_AUTH_ERROR:
            best_failure = result

    # reachable is non-empty, so best_failure is always set after the loop.
    assert best_failure is not None
    return best_failure


# --- connectivity task handlers (called from appCataloga_host_check.py) ---


def _persist_degraded(
    db: dbHandlerBKP,
    task: dict,
) -> tuple[int, str]:
    """
    Persist degraded connectivity state for a host.

    Outcomes:
        - below threshold:  increments error counter; returns PENDING
        - at threshold:     returns ERROR; host stays BUSY until recovery
    """
    next_count = max(0, int(task["host_check_error_count"] or 0)) + 1
    threshold = k.HOST_CHECK_SSH_TIMEOUT_CONFIRMATIONS

    db.host_update(
        host_id=task["host_id"],
        reset=True,
        DT_LAST_CHECK=task["now"],
        DT_LAST_FAIL=task["now"],
        NU_HOST_CHECK_ERROR=next_count,
    )

    if next_count >= threshold:
        return (
            k.TASK_ERROR,
            f"SSH supervision degraded threshold reached while ICMP still responds ({next_count}/{threshold})",
        )

    return (
        k.TASK_PENDING,
        f"SSH supervision degraded while ICMP still responds | confirmation {next_count}/{threshold}",
    )


def _finalize_check(
    db: dbHandlerBKP,
    task: dict,
    online: bool,
    *,
    promote_to_processing: bool,
    resume_dependent_tasks: bool,
    logger: logger_type,
) -> tuple[int, str]:
    """
    Apply the final connectivity result to HOST and return the task close state.

    Outcomes:
        - offline:             returns ERROR
        - online + promote:    queues discovery task; returns DONE
        - online + no promote: returns DONE (connectivity-only check)
    """
    persist_host_connectivity_state(
        db=db,
        log=logger,
        host_id=task["host_id"],
        was_offline=task["was_offline"],
        online=online,
        now=task["now"],
        resume_dependent_tasks=resume_dependent_tasks,
    )

    if not online:
        return (k.TASK_ERROR, "Host unreachable (connectivity check failed)")

    if promote_to_processing:
        db.queue_host_task(
            host_id=task["host_id"],
            task_type=k.HOST_TASK_PROCESSING_TYPE,
            task_status=k.TASK_PENDING,
            filter_dict=task["host_filter"],
        )
        return (k.TASK_DONE, "Host check completed; discovery task queued")

    return (k.TASK_DONE, "Host connectivity reconciliation completed successfully")


def run_check(
    db: dbHandlerBKP,
    task: dict,
    *,
    service_name: str,
    logger: logger_type,
    promote_to_processing: bool,
) -> tuple[int, str]:
    """
    Execute one queued connectivity task (CHECK or CHECK_CONNECTION).

    Probes the host, logs the result, then dispatches to the matching handler:
        - degraded:    increments error counter; retries until threshold
        - auth_error:  suspends all dependent work
        - online:      persists online state; optionally promotes to discovery
        - offline:     persists offline state

    Returns (status, message) for the caller to close the task.
    Raises on any DB failure — does not catch internally.
    """
    event_name = (
        k.EVENT_HOST_CHECK
        if promote_to_processing else
        k.EVENT_CHECK_CONNECTION
    )
    started_at = time.monotonic()

    probe_started_at = time.monotonic()
    connectivity = probe_host_connectivity(
        addr=task["addr"],
        port=task["port"],
        user=task["user"],
        password=task["password"],
    )

    logger.event(
        event_name,
        **build_connectivity_probe_fields(
            host_id=task["host_id"],
            addr=task["addr"],
            port=task["port"],
            probe=connectivity,
        )
    )
    probe_elapsed_sec = round(time.monotonic() - probe_started_at, 3)
    logger.task_phase(
        service_name,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=task["task_type"],
        phase="probe",
        elapsed_sec=probe_elapsed_sec,
        since_start_sec=round(time.monotonic() - started_at, 3),
        reason=connectivity["reason"],
        state=connectivity["state"],
    )

    persist_started_at = time.monotonic()
    match connectivity["state"]:
        case k.HOST_CONN_DEGRADED:
            result = _persist_degraded(db, task)
        case k.HOST_CONN_AUTH_ERROR:
            result = persist_auth_error(
                db, task,
                detail=connectivity["error"] or connectivity["reason"],
                logger=logger,
            )
        case _:
            # Covers HOST_CONN_ONLINE and HOST_CONN_OFFLINE.
            # A non-zero error count means previous degraded probes already
            # suspended dependent queues; resume them on a successful recovery.
            result = _finalize_check(
                db, task,
                online=(connectivity["state"] == k.HOST_CONN_ONLINE),
                promote_to_processing=promote_to_processing,
                resume_dependent_tasks=(task["host_check_error_count"] > 0),
                logger=logger,
            )

    persist_elapsed_sec = round(time.monotonic() - persist_started_at, 3)
    since_start_sec = round(time.monotonic() - started_at, 3)
    logger.task_phase(
        service_name,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=task["task_type"],
        phase="persist",
        elapsed_sec=persist_elapsed_sec,
        since_start_sec=since_start_sec,
        state=connectivity["state"],
        status=result[0],
    )

    if promote_to_processing and connectivity["state"] == k.HOST_CONN_ONLINE:
        logger.task_phase(
            service_name,
            host_id=task["host_id"],
            task_id=task["task_id"],
            task_type=task["task_type"],
            phase="queue_followup",
            elapsed_sec=0.0,
            since_start_sec=since_start_sec,
            queued_task_type=k.HOST_TASK_PROCESSING_TYPE,
        )

    return result
