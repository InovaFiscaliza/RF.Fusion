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
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from db.dbHandlerBKP import dbHandlerBKP
    from shared.logging_utils import log as logger_type

from ping3 import ping

from .host_ssh_utils import (
    ConnectivityProbePayload,
    persist_auth_error,
    record_ssh_failure,
    record_ssh_success,
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

    Some stations publish multiple A records. We resolve once and pick a
    stable preference so ICMP and SSH do not probe different endpoints.

    DNS failures return an empty list. Callers should treat that as
    "unreachable" instead of passing the raw hostname into the ICMP library.
    """
    try:
        literal_ip = ipaddress.ip_address(host_addr)
    except ValueError:
        literal_ip = None

    # Literal IPs bypass DNS so explicit host entries stay deterministic.
    if literal_ip is not None:
        return [str(literal_ip)]

    try:
        infos = socket.getaddrinfo(host_addr, None, family=socket.AF_INET, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return []
    except Exception:
        return []

    addresses: list[str] = []
    for _family, _type, _proto, _canonname, sockaddr in infos:
        addr = sockaddr[0]
        if addr not in addresses:
            addresses.append(addr)

    if not addresses:
        return []

    preferred_172 = []
    for addr in addresses:
        try:
            ip_obj = ipaddress.ip_address(addr)
            if ip_obj.version == 4 and str(ip_obj).startswith("172."):
                preferred_172.append(addr)
        except ValueError:
            pass

    # Prefer the operational 172.x.x.x endpoint when it exists.
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

    The probe helpers return data. This helper maps it to the stable log fields
    shared by host workers and maintenance flows.
    """
    payload = {
        "component": "host_connectivity",
        "operation": "probe",
        "host_id": host_id,
        "address": addr,
        "state": probe["state"],
        "reason": probe["reason"],
        "online": probe["online"],
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
    """Ping one concrete address without triggering another DNS lookup."""
    try:
        return ping(addr, timeout=timeout_sec) is not None
    except Exception:
        return False


def _ping_latency_ms(addr: str, timeout_sec: float) -> float | None:
    """Return ICMP latency in milliseconds for the interactive progress view."""
    try:
        latency_sec = ping(addr, timeout=timeout_sec)
    except Exception:
        return None

    if latency_sec is None:
        return None

    return round(float(latency_sec) * 1000, 1)


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
        "DT_LAST_OFFLINE_AT": now,
        "NA_LAST_OFFLINE_DESCRIPTION": k.HOST_OFFLINE_DESCRIPTION,
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
    addr = resolve_host_addresses(host_addr)
    return any(_ping_address(a, timeout) for a in addr)


def probe_host_connectivity(
    addr: str,
    port: int,
    user: str,
    password: str,
    progress_reporter: Callable[[str], None] | None = None,
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
    reachable: list[str] = []

    for resolved_addr in resolved_addrs:
        if progress_reporter is None:
            if _ping_address(resolved_addr, k.ICMP_TIMEOUT_SEC):
                reachable.append(resolved_addr)
            continue

        progress_reporter(f"ICMP: verificando {resolved_addr}.")
        latency_ms = _ping_latency_ms(resolved_addr, k.ICMP_TIMEOUT_SEC)
        if latency_ms is not None:
            reachable.append(resolved_addr)
            progress_reporter(
                f"ICMP: {resolved_addr} respondeu em {latency_ms:.1f} ms."
            )

    if not reachable:
        if progress_reporter is not None:
            progress_reporter("ICMP: a estação não respondeu ao teste de conectividade.")
        return {
            "state": k.HOST_CONN_OFFLINE,
            "online": False,
            "reason": "icmp_unreachable",
            "icmp_online": False,
            "ssh_online": False,
            "error": None,
            "resolved_candidates": resolved_addrs,
        }

    best_failure: dict | None = None

    for resolved_addr in reachable:
        if progress_reporter is not None:
            progress_reporter(
                f"SSH: verificando acesso em {resolved_addr}:{port}."
            )
        result = ssh_probe(addr=resolved_addr, port=port, user=user, password=password)
        result["resolved_addr"] = resolved_addr
        result["resolved_candidates"] = resolved_addrs

        if result["state"] == k.HOST_CONN_ONLINE:
            if progress_reporter is not None:
                progress_reporter("SSH: acesso confirmado.")
            return result

        # Auth rejection outranks degradation — operators need to fix credentials.
        if best_failure is None or result["state"] == k.HOST_CONN_AUTH_ERROR:
            best_failure = result

    if progress_reporter is not None and best_failure is not None:
        if best_failure["state"] == k.HOST_CONN_AUTH_ERROR:
            progress_reporter("SSH: autenticação recusada pela estação.")
        else:
            progress_reporter("SSH: não foi possível confirmar o acesso à estação.")

    # reachable is non-empty, so best_failure is always set after the loop.
    assert best_failure is not None
    return best_failure


def persist_ssh_probe_signal(
    host_id: int,
    connectivity: ConnectivityProbePayload,
    *,
    observed_at: datetime,
    logger: logger_type,
) -> None:
    """Persist only the SSH state proven by one completed probe."""
    match connectivity["state"]:
        case k.HOST_CONN_ONLINE:
            record_ssh_success(
                host_id,
                observed_at=observed_at,
                logger=logger,
            )
        case k.HOST_CONN_AUTH_ERROR:
            record_ssh_failure(
                host_id,
                observed_at=observed_at,
                failure_code=k.SSH_FAILURE_CODE_AUTHENTICATION,
                description=connectivity["error"] or connectivity["reason"],
                logger=logger,
            )
        case k.HOST_CONN_DEGRADED:
            record_ssh_failure(
                host_id,
                observed_at=observed_at,
                failure_code=k.SSH_FAILURE_CODE_CONNECTIVITY,
                description=connectivity["error"] or connectivity["reason"],
                logger=logger,
            )
        case k.HOST_CONN_OFFLINE:
            return
        case _:
            raise ValueError(f"Unsupported connectivity state: {connectivity['state']}")


def persist_icmp_observation(
    *,
    db: dbHandlerBKP,
    host_id: int,
    observed_at: datetime,
) -> None:
    """
    Refresh the last connectivity-check timestamp after a successful ICMP probe.

    This is an observation only. It does not change `IS_OFFLINE`, counters, or
    dependent queues, which require the definitive state-machine helper.
    """
    db.host_update(
        host_id=host_id,
        DT_LAST_CHECK=observed_at,
    )


def persist_reachable_probe_observation(
    *,
    db: dbHandlerBKP,
    host_id: int,
    connectivity: ConnectivityProbePayload,
    observed_at: datetime,
    logger: logger_type,
) -> None:
    """
    Persist the SSH signal and ICMP observation for a reachable host.

    This path intentionally does not transition the host online or offline.
    It is used when the canonical probe confirms ICMP but reports either SSH
    success or degradation, leaving definitive state transitions to
    `persist_host_connectivity_state(...)`.
    """
    if not connectivity["icmp_online"]:
        raise ValueError("Reachable probe observation requires a positive ICMP result")

    persist_ssh_probe_signal(
        host_id,
        connectivity,
        observed_at=observed_at,
        logger=logger,
    )
    persist_icmp_observation(
        db=db,
        host_id=host_id,
        observed_at=observed_at,
    )


# --- connectivity task handlers (called from appCataloga_host_check.py) ---


def _persist_connectivity_outcome(
    db: dbHandlerBKP,
    task: dict,
    connectivity: ConnectivityProbePayload,
    *,
    promote_to_processing: bool,
    logger: logger_type,
) -> tuple[int, str, bool]:
    """
    Persist one connectivity outcome and return `(status, message, queued_followup)`.

    This keeps the state machine linear inside `run_check()`:
        1. probe
        2. persist outcome
        3. return worker task status/message
    """
    match connectivity["state"]:
        case k.HOST_CONN_DEGRADED:
            # SSH degradation is treated as a confirmation flow: keep the host
            # operational for now, but escalate to definitive ERROR after the
            # configured number of consecutive degraded probes.
            next_count = max(0, int(task["host_check_error_count"] or 0)) + 1
            threshold = k.HOST_CHECK_SSH_TIMEOUT_CONFIRMATIONS

            db.host_update(
                host_id=task["host_id"],
                reset=True,
                DT_LAST_CHECK=task["now"],
                DT_LAST_FAIL=task["now"],
                NU_HOST_CHECK_ERROR=next_count,
            )
            record_ssh_failure(
                task["host_id"],
                observed_at=task["now"],
                failure_code=k.SSH_FAILURE_CODE_CONNECTIVITY,
                description=(
                    connectivity["error"]
                    or connectivity["reason"]
                    or k.SSH_FAILURE_DESCRIPTION
                ),
                logger=logger,
            )

            if next_count >= threshold:
                return (
                    k.TASK_ERROR,
                    f"SSH supervision degraded threshold reached while ICMP still responds ({next_count}/{threshold})",
                    False,
                )

            return (
                k.TASK_PENDING,
                f"SSH supervision degraded while ICMP still responds | confirmation {next_count}/{threshold}",
                False,
            )

        case k.HOST_CONN_AUTH_ERROR:
            # Auth rejection is not transient operational noise. We suspend all
            # dependent work immediately and let credentials be fixed manually.
            status, message = persist_auth_error(
                db,
                task,
                detail=connectivity["error"] or connectivity["reason"],
                logger=logger,
            )
            return status, message, False

        case k.HOST_CONN_OFFLINE:
            # Offline is a definitive connectivity failure, so the host state
            # machine is persisted first and the worker task closes as ERROR.
            persist_host_connectivity_state(
                db=db,
                log=logger,
                host_id=task["host_id"],
                was_offline=task["was_offline"],
                online=connectivity["online"],
                now=task["now"],
                resume_dependent_tasks=(task["host_check_error_count"] > 0),
            )
            return (k.TASK_ERROR, "Host unreachable (connectivity check failed)", False)

        case k.HOST_CONN_ONLINE:
            # A successful supervisory probe reconciles the persisted host state
            # and may queue discovery only for the full CHECK task variant.
            record_ssh_success(
                task["host_id"],
                observed_at=task["now"],
                logger=logger,
            )
            persist_host_connectivity_state(
                db=db,
                log=logger,
                host_id=task["host_id"],
                was_offline=task["was_offline"],
                online=connectivity["online"],
                now=task["now"],
                resume_dependent_tasks=(task["host_check_error_count"] > 0),
            )

            if promote_to_processing:
                db.queue_host_task(
                    host_id=task["host_id"],
                    task_type=k.HOST_TASK_PROCESSING_TYPE,
                    task_status=k.TASK_PENDING,
                    filter_dict=task["host_filter"],
                )
                return (k.TASK_DONE, "Host check completed; discovery task queued", True)

            return (k.TASK_DONE, "Host connectivity reconciliation completed successfully", False)

        case _:
            raise ValueError(f"Unsupported connectivity state: {connectivity['state']}")


def _build_interactive_result(
    connectivity: ConnectivityProbePayload,
    status: int,
    message: str,
) -> tuple[int, str]:
    """Render a human-facing terminal message for one manual station test."""
    match connectivity["state"]:
        case k.HOST_CONN_ONLINE:
            return (
                k.TASK_DONE,
                "Teste concluído com sucesso: ICMP e SSH confirmados.",
            )
        case k.HOST_CONN_OFFLINE:
            return (
                k.TASK_ERROR,
                "Teste concluído com falha: a estação não respondeu ao ICMP.",
            )
        case k.HOST_CONN_AUTH_ERROR:
            return (
                k.TASK_ERROR,
                "Teste concluído com falha: a autenticação SSH foi recusada.",
            )
        case k.HOST_CONN_DEGRADED:
            return (
                k.TASK_ERROR,
                "Teste concluído com falha: ICMP respondeu, mas o acesso SSH não foi confirmado.",
            )
        case _:
            return status, message


def run_check(
    db: dbHandlerBKP,
    task: dict,
    *,
    service_name: str,
    logger: logger_type,
    promote_to_processing: bool,
    progress_reporter: Callable[[str], None] | None = None,
    terminal_on_degraded: bool = False,
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

    # The entrypoint measures total `_do_work()` time. The domain only measures
    # completed internal phases (`probe`, `persist`, `queue_followup`).
    probe_started_at = time.monotonic()

    # Run connectivity test - ICMP + SSH probe
    connectivity = probe_host_connectivity(
        addr=task["addr"],
        port=task["port"],
        user=task["user"],
        password=task["password"],
        progress_reporter=progress_reporter,
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
    since_start_sec = probe_elapsed_sec
    logger.task_phase(
        service_name,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=task["task_type"],
        phase="probe",
        elapsed_sec=probe_elapsed_sec,
        since_start_sec=since_start_sec,
        reason=connectivity["reason"],
        state=connectivity["state"],
    )

    if progress_reporter is not None:
        progress_reporter("Atualizando o estado operacional da estação.")

    persist_started_at = time.monotonic()
    # Persistence decides both queue outcome and whether this online probe
    # should fan out into a follow-up discovery task.
    status, message, queued_followup = _persist_connectivity_outcome(
        db,
        task,
        connectivity,
        promote_to_processing=promote_to_processing,
        logger=logger,
    )
    if terminal_on_degraded:
        status, message = _build_interactive_result(
            connectivity,
            status,
            message,
        )
    persist_elapsed_sec = round(time.monotonic() - persist_started_at, 3)
    since_start_sec = round(since_start_sec + persist_elapsed_sec, 3)
    logger.task_phase(
        service_name,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=task["task_type"],
        phase="persist",
        elapsed_sec=persist_elapsed_sec,
        since_start_sec=since_start_sec,
        state=connectivity["state"],
        status=status,
    )

    if queued_followup:
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

    return status, message


def run_interactive_check(
    db: dbHandlerBKP,
    task: dict,
    *,
    service_name: str,
    logger: logger_type,
    progress_reporter: Callable[[str], None],
) -> tuple[int, str]:
    """Run one operator-requested test without retrying degraded SSH results."""
    return run_check(
        db,
        task,
        service_name=service_name,
        logger=logger,
        promote_to_processing=False,
        progress_reporter=progress_reporter,
        terminal_on_degraded=True,
    )
