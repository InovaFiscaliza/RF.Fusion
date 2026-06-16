"""
Domain helpers for recurring host-maintenance sweeps.

`appCataloga_host_maintenance.py` owns the daemon lifecycle and cadence. This
module owns the host-specific meaning of one maintenance pass:
which HOST rows are due, how offline recovery is confirmed, and what one sweep
iteration should do with one host snapshot.

Reading guide:
    1. `select_due_hosts(...)` trims the full HOST list to the stale batch the
       recurring sweep should touch right now.
    2. `_recover_offline_host_if_operational(...)` handles the strict recovery
       path used when a host is already marked offline.
    3. `process_due_host(...)` applies one maintenance pass to one HOST row.

The helpers intentionally work with the raw DB row shape. For this sweep, a
small amount of explicit field access is easier to maintain than introducing a
wrapper object that only renames dictionary keys.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, TypedDict

from host_handler import host_connectivity

if TYPE_CHECKING:
    from db.dbHandlerBKP import dbHandlerBKP
    from host_handler.host_ssh_utils import ConnectivityProbePayload
    from shared.logging_utils import log as logger_type


class HostSnapshot(TypedDict, total=False):
    """Minimal HOST row shape used by the maintenance sweep."""

    ID_HOST: int
    NA_HOST_NAME: str
    NA_HOST_ADDRESS: str
    NA_HOST_PORT: int
    NA_HOST_USER: str
    NA_HOST_PASSWORD: str
    IS_BUSY: bool
    IS_OFFLINE: bool
    DT_LAST_CHECK: datetime | None


class HostSweepResult(TypedDict):
    """Structured one-host outcome used by the batch summary."""

    checked: bool
    skipped_busy: bool
    icmp_online: bool
    recovery_probe: bool


def _log_maintenance_state_change(
    *,
    log: logger_type,
    host: HostSnapshot,
    previous_state: str,
    current_state: str,
    reason: str,
) -> None:
    """Emit one maintenance-specific state-change event for a host."""

    log.event(
        "host_check_all_state_change",
        component="host_maintenance",
        operation="process_due_host",
        host_id=host["ID_HOST"],
        host=host.get("NA_HOST_NAME"),
        address=host["NA_HOST_ADDRESS"],
        previous_state=previous_state,
        current_state=current_state,
        reason=reason,
    )


def _probe_host_icmp(host: HostSnapshot, timeout_sec: float) -> bool:
    """Run the lightweight ICMP pre-check for one host snapshot."""

    return host_connectivity.is_host_online(
        host["NA_HOST_ADDRESS"],
        timeout_sec=timeout_sec,
    )


def _probe_host_operational_state(host: HostSnapshot) -> ConnectivityProbePayload:
    """Run the shared operational probe for one host snapshot."""

    return host_connectivity.probe_host_connectivity(
        addr=host["NA_HOST_ADDRESS"],
        port=int(host["NA_HOST_PORT"]),
        user=host["NA_HOST_USER"],
        password=host["NA_HOST_PASSWORD"],
    )


def _run_icmp_probe_batch(
    hosts: list[HostSnapshot],
    *,
    timeout_sec: float,
    max_workers: int,
) -> dict[int, bool]:
    """Probe ICMP reachability in parallel and return one result per host."""

    if not hosts:
        return {}

    worker_count = min(max_workers, len(hosts))
    if worker_count <= 1:
        return {
            host["ID_HOST"]: _probe_host_icmp(host, timeout_sec)
            for host in hosts
        }

    results: dict[int, bool] = {}
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="host-maint-icmp",
    ) as executor:
        future_to_host = {
            executor.submit(_probe_host_icmp, host, timeout_sec): host
            for host in hosts
        }

        for future in as_completed(future_to_host):
            host = future_to_host[future]
            results[host["ID_HOST"]] = bool(future.result())

    return results


def _run_operational_probe_batch(
    hosts: list[HostSnapshot],
    *,
    max_workers: int,
) -> dict[int, ConnectivityProbePayload]:
    """Probe operational connectivity in parallel and return one result per host."""

    if not hosts:
        return {}

    worker_count = min(max_workers, len(hosts))
    if worker_count <= 1:
        return {
            host["ID_HOST"]: _probe_host_operational_state(host)
            for host in hosts
        }

    results: dict[int, ConnectivityProbePayload] = {}
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="host-maint-probe",
    ) as executor:
        future_to_host = {
            executor.submit(_probe_host_operational_state, host): host
            for host in hosts
        }

        for future in as_completed(future_to_host):
            host = future_to_host[future]
            results[host["ID_HOST"]] = future.result()

    return results


def select_due_hosts(
    host_rows: list[HostSnapshot],
    now: datetime,
    *,
    stale_after_sec: int,
    batch_size: int,
) -> list[HostSnapshot]:
    """
    Return the oldest stale hosts that should be inspected in this batch.

    The source snapshot may prioritize online hosts ahead of offline hosts, so
    freshness can no longer be inferred from the first fresh row alone. Filter
    the full snapshot and keep only stale rows until the batch is full.

    The returned rows are the original DB dictionaries.
    That keeps the sweep transparent and avoids a wrapper that only renames keys.
    """
    stale_after = timedelta(seconds=stale_after_sec)
    due_hosts: list[HostSnapshot] = []

    for row in host_rows:
        last_check = row.get("DT_LAST_CHECK")

        if last_check and (now - last_check) < stale_after:
            continue

        due_hosts.append(row)

        # The sweep stays bounded even on very large host fleets.
        if len(due_hosts) >= batch_size:
            break

    return due_hosts


def _persist_recovery_probe_result(
    *,
    db: dbHandlerBKP,
    log: logger_type,
    host: HostSnapshot,
    checked_at: datetime,
    connectivity: ConnectivityProbePayload,
) -> None:
    """
    Persist the recovery decision for one host already marked offline.

    Recovery stays deliberately strict:
        - `ICMP up + SSH online`  -> recover the host
        - anything else           -> keep the host offline
    """
    log.event(
        "host_check_all_recovery_probe",
        **host_connectivity.build_connectivity_probe_fields(
            host_id=host["ID_HOST"],
            host_name=host.get("NA_HOST_NAME"),
            addr=host["NA_HOST_ADDRESS"],
            port=int(host["NA_HOST_PORT"]),
            probe=connectivity,
        ),
    )

    if connectivity["state"] == "online":
        # Only a fully operational result may resume suspended work.
        _log_maintenance_state_change(
            log=log,
            host=host,
            previous_state="offline",
            current_state="online",
            reason=connectivity["reason"],
        )
        host_connectivity.persist_host_connectivity_state(
            db=db,
            log=log,
            host_id=host["ID_HOST"],
            was_offline=bool(host.get("IS_OFFLINE")),
            online=True,
            now=checked_at,
        )
        return

    # Keep the host offline until the SSH endpoint is confirmed.
    # Reuse the shared offline path so drifted queues are suspended again.
    host_connectivity.persist_host_connectivity_state(
        db=db,
        log=log,
        host_id=host["ID_HOST"],
        was_offline=bool(host.get("IS_OFFLINE")),
        online=False,
        now=checked_at,
    )
    return


def _persist_offline_confirmation_result(
    *,
    db: dbHandlerBKP,
    log: logger_type,
    host: HostSnapshot,
    checked_at: datetime,
    connectivity: ConnectivityProbePayload,
) -> None:
    """
    Persist the confirmation result for one host still marked online.

    The short ICMP sweep is only a pre-check. The canonical probe decides
    whether the host stays online or transitions to offline.
    """
    log.event(
        "host_check_all_offline_confirmation_probe",
        **host_connectivity.build_connectivity_probe_fields(
            host_id=host["ID_HOST"],
            host_name=host.get("NA_HOST_NAME"),
            addr=host["NA_HOST_ADDRESS"],
            port=int(host["NA_HOST_PORT"]),
            probe=connectivity,
        ),
    )

    if connectivity["icmp_online"]:
        # The host still answers ICMP with the canonical timeout. Keep the
        # current operational state and only refresh the observation timestamp.
        db.host_update(
            host_id=host["ID_HOST"],
            DT_LAST_CHECK=checked_at,
        )
        return

    _log_maintenance_state_change(
        log=log,
        host=host,
        previous_state="online",
        current_state="offline",
        reason=connectivity["reason"],
    )
    host_connectivity.persist_host_connectivity_state(
        db=db,
        log=log,
        host_id=host["ID_HOST"],
        was_offline=bool(host.get("IS_OFFLINE")),
        online=False,
        now=checked_at,
    )


def process_due_host(
    *,
    db: dbHandlerBKP,
    log: logger_type,
    host: HostSnapshot,
    checked_at: datetime,
    icmp_timeout_sec: float,
    icmp_online: bool | None = None,
    connectivity: ConnectivityProbePayload | None = None,
) -> HostSweepResult:
    """
    Process one stale host snapshot from the recurring maintenance sweep.

    Flow:
        1. skip BUSY hosts so maintenance does not compete with real work
        2. run the lightweight ICMP sweep
        3. if the host was offline, run the strict recovery probe
        4. otherwise persist the steady-state online/offline refresh

    Return a small structured result used by the batch summary.

    The maintenance daemon is high-frequency, so the caller aggregates these
    results into one compact batch log.
    """
    if bool(host.get("IS_BUSY")):
        # The recurring sweep must not compete with the data plane.
        return {
            "checked": False,
            "skipped_busy": True,
            "icmp_online": False,
            "recovery_probe": False,
        }

    online = (
        _probe_host_icmp(host, icmp_timeout_sec)
        if icmp_online is None
        else bool(icmp_online)
    )
    recovery_probe = False

    if bool(host.get("IS_OFFLINE")):
        # Offline recovery must use the canonical operational probe. This lets
        # maintenance correct stale offline state even when the short ICMP
        # sweep misses a host that still answers the longer shared probe.
        recovery_probe = True
        probe_result = (
            _probe_host_operational_state(host)
            if connectivity is None
            else connectivity
        )
        _persist_recovery_probe_result(
            db=db,
            log=log,
            host=host,
            checked_at=checked_at,
            connectivity=probe_result,
        )
    elif online:
        # Steady online refresh: no queue side effect is needed here.
        db.host_update(
            host_id=host["ID_HOST"],
            DT_LAST_CHECK=checked_at,
        )
    else:
        if bool(host.get("IS_OFFLINE")):
            # Already-offline hosts can stay on the lightweight path.
            host_connectivity.persist_host_connectivity_state(
                db=db,
                log=log,
                host_id=host["ID_HOST"],
                was_offline=bool(host.get("IS_OFFLINE")),
                online=False,
                now=checked_at,
            )
        else:
            probe_result = (
                _probe_host_operational_state(host)
                if connectivity is None
                else connectivity
            )
            _persist_offline_confirmation_result(
                db=db,
                log=log,
                host=host,
                checked_at=checked_at,
                connectivity=probe_result,
            )

    return {
        "checked": True,
        "skipped_busy": False,
        "icmp_online": bool(online),
        "recovery_probe": recovery_probe,
    }


def run_host_check_all_batch(
    *,
    db: dbHandlerBKP,
    log: logger_type,
    now: datetime,
    process_status: dict[str, bool],
    stale_after_sec: int,
    batch_size: int,
    icmp_timeout_sec: float,
    icmp_max_workers: int,
    probe_max_workers: int,
) -> int:
    """
    Refresh one bounded oldest-first batch of stale HOST connectivity snapshots.

    This is the recurring-sweep counterpart to the queue-driven host-check
    worker:
        - it never consumes HOST_TASK rows
        - it works from stale HOST timestamps instead of queued tasks
        - it still uses the same host connectivity/state machine helpers

    The batch stays small and oldest-first so the daemon keeps making progress
    without monopolizing the service loop.
    """
    # Phase 1: take one ordered snapshot of the HOST table.
    hosts = db.host_list_for_connectivity_check()
    if not hosts:
        return 0

    # Phase 2: trim the full snapshot to the stale slice for this pass.
    due_hosts = select_due_hosts(
        hosts,
        now,
        stale_after_sec=stale_after_sec,
        batch_size=batch_size,
    )
    if not due_hosts:
        return 0

    non_busy_hosts = [
        host
        for host in due_hosts
        if not bool(host.get("IS_BUSY"))
    ]
    icmp_results = _run_icmp_probe_batch(
        non_busy_hosts,
        timeout_sec=icmp_timeout_sec,
        max_workers=icmp_max_workers,
    )
    operational_probe_hosts = [
        host
        for host in non_busy_hosts
        if bool(host.get("IS_OFFLINE"))
        or not icmp_results.get(host["ID_HOST"], False)
    ]
    operational_probe_results = _run_operational_probe_batch(
        operational_probe_hosts,
        max_workers=probe_max_workers,
    )

    checked = 0
    skipped_busy = 0
    icmp_online = 0
    icmp_offline = 0
    recovery_probes = 0

    # Phase 3: apply the probe results sequentially.
    # Network fan-out is parallel, but persisted host state remains linear.
    for host in due_hosts:
        if not process_status["running"]:
            # Shutdown should stop the batch between hosts, never in the middle
            # of one host's reconciliation path.
            break

        try:
            result = process_due_host(
                db=db,
                log=log,
                host=host,
                checked_at=now,
                icmp_timeout_sec=icmp_timeout_sec,
                icmp_online=icmp_results.get(host["ID_HOST"]),
                connectivity=operational_probe_results.get(host["ID_HOST"]),
            )

            if result["skipped_busy"]:
                skipped_busy += 1
                continue

            if result["checked"]:
                checked += 1
                if result["icmp_online"]:
                    icmp_online += 1
                else:
                    icmp_offline += 1
                if result["recovery_probe"]:
                    recovery_probes += 1
        except Exception as e:
            # A per-host maintenance failure is logged with enough identity to
            # investigate later, while the batch keeps moving forward.
            log.error_event(
                "host_check_all_failed",
                component="host_maintenance",
                operation="run_host_check_all_batch",
                host_id=host["ID_HOST"],
                host=host.get("NA_HOST_NAME"),
                address=host["NA_HOST_ADDRESS"],
                error=e,
            )

    # Emit one compact batch summary for the whole maintenance pass.
    # Recovery probes and state transitions keep their own detailed logs.
    log.event(
        "host_check_all_batch_done",
        component="host_maintenance",
        operation="run_host_check_all_batch",
        selected=len(due_hosts),
        checked=checked,
        skipped_busy=skipped_busy,
        icmp_online=icmp_online,
        icmp_offline=icmp_offline,
        recovery_probes=recovery_probes,
        stale_after_sec=stale_after_sec,
        timeout_sec=icmp_timeout_sec,
    )

    return checked


def run_periodic_host_cleanup(
    *,
    db: dbHandlerBKP,
    log: logger_type,
    task_stale_after_sec: int,
    host_busy_timeout_sec: int,
) -> None:
    """
    Run the recurring cleanup steps that keep host orchestration healthy.

    Cleanup is intentionally split into two independent steps:
        1. recover stale operational HOST_TASK rows left by crashed workers
        2. release stale HOST.IS_BUSY locks whose owner process no longer exists

    Each step is logged and isolated so one cleanup failure does not hide the
    other or abort the rest of the maintenance loop.
    """
    try:
        # Step 1: recover HOST_TASK rows that were left pending or running by
        # crashed workers so the queue does not stall indefinitely.
        db.host_task_cleanup_stale_operational_tasks(
            stale_after_seconds=task_stale_after_sec
        )
    except Exception as e:
        log.error_event(
            "host_task_cleanup_failed",
            component="host_maintenance",
            operation="cleanup_stale_operational_tasks",
            error=e,
        )

    try:
        # Step 2: release HOST.IS_BUSY locks whose owner process no longer
        # exists, then let orchestration reconcile safely afterwards.
        db.host_cleanup_stale_locks(
            threshold_seconds=host_busy_timeout_sec
        )
    except Exception as e:
        log.error_event(
            "host_cleanup_failed",
            component="host_maintenance",
            operation="cleanup_stale_host_locks",
            error=e,
        )
