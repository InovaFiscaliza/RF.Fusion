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
       path used only when a host is already marked offline.
    3. `process_due_host(...)` applies one maintenance pass to one HOST row.

The helpers intentionally work with the raw DB row shape. For this sweep, a
small amount of explicit field access is easier to maintain than introducing a
wrapper object that only renames dictionary keys.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, TYPE_CHECKING

from host_handler import host_connectivity

if TYPE_CHECKING:
    from db.dbHandlerBKP import dbHandlerBKP
    from shared.logging_utils import log as logger_type


def select_due_hosts(
    host_rows: list[dict],
    now: datetime,
    *,
    stale_after_sec: int,
    batch_size: int,
) -> list[dict]:
    """
    Return the oldest stale hosts that should be inspected in this batch.

    The DB query is already ordered by age, so the first fresh row lets the
    sweep stop early instead of scanning the entire HOST table every cycle.

    The returned rows are the original DB dictionaries.
    That keeps the sweep transparent and avoids a wrapper that only renames keys.
    """
    stale_after = timedelta(seconds=stale_after_sec)
    due_hosts: list[dict] = []

    for row in host_rows:
        last_check = row.get("DT_LAST_CHECK")

        # The source query is oldest-first. As soon as we encounter one host
        # that is still fresh enough, every row after it is also fresh enough
        # for this pass and the loop can stop immediately.
        if last_check and (now - last_check) < stale_after:
            break

        due_hosts.append(row)

        # The sweep stays bounded even on very large host fleets.
        if len(due_hosts) >= batch_size:
            break

    return due_hosts


def _recover_offline_host_if_operational(
    *,
    db: dbHandlerBKP,
    log: logger_type,
    host: dict,
    checked_at: datetime,
    connectivity_module: Any,
) -> None:
    """
    Recover an offline host only after both ICMP and SSH confirm it is back.

    ICMP alone is not enough for stations that keep a modem alive while the
    industrial PC behind the modem is still hung. Recovery therefore uses the
    short operational probe before any suspended work is resumed.

    This helper is deliberately strict:
        - `ICMP up + SSH online`  -> recover the host
        - anything else           -> keep the host offline

    That conservatism avoids reviving a queue that would fail again on the
    first real SSH/SFTP use.
    """
    # This is the expensive branch of the recurring sweep.
    # We only pay for SSH confirmation after ICMP suggests recovery.
    connectivity = connectivity_module.probe_host_connectivity(
        addr=host["NA_HOST_ADDRESS"],
        port=int(host["NA_HOST_PORT"]),
        user=host["NA_HOST_USER"],
        password=host["NA_HOST_PASSWORD"],
    )
    log.event(
        "host_check_all_recovery_probe",
        **connectivity_module.build_connectivity_probe_fields(
            host_id=host["ID_HOST"],
            host_name=host.get("NA_HOST_NAME"),
            addr=host["NA_HOST_ADDRESS"],
            port=int(host["NA_HOST_PORT"]),
            probe=connectivity,
        ),
    )

    if connectivity["state"] == "online":
        # Only a fully operational result may resume suspended work.
        connectivity_module.persist_host_connectivity_state(
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
    connectivity_module.persist_host_connectivity_state(
        db=db,
        log=log,
        host_id=host["ID_HOST"],
        was_offline=bool(host.get("IS_OFFLINE")),
        online=False,
        now=checked_at,
    )
    return


def process_due_host(
    *,
    db: dbHandlerBKP,
    log: logger_type,
    host: dict,
    checked_at: datetime,
    icmp_timeout_sec: float,
    connectivity_module: Any = host_connectivity,
) -> dict[str, Any]:
    """
    Process one stale host snapshot from the recurring maintenance sweep.

    Flow:
        1. skip BUSY hosts so maintenance does not compete with real work
        2. run the lightweight ICMP sweep
        3. if the host was offline and now pings, run the strict recovery probe
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

    online = connectivity_module.is_host_online(
        host["NA_HOST_ADDRESS"],
        timeout_sec=icmp_timeout_sec,
    )
    recovery_probe = False

    if online and bool(host.get("IS_OFFLINE")):
        # Offline recovery needs stronger proof than ICMP alone.
        recovery_probe = True
        _recover_offline_host_if_operational(
            db=db,
            log=log,
            host=host,
            checked_at=checked_at,
            connectivity_module=connectivity_module,
        )
    elif online:
        # Steady online refresh: no queue side effect is needed here.
        db.host_update(
            host_id=host["ID_HOST"],
            DT_LAST_CHECK=checked_at,
        )
    else:
        # The shared state machine decides the offline side effects.
        connectivity_module.persist_host_connectivity_state(
            db=db,
            log=log,
            host_id=host["ID_HOST"],
            was_offline=bool(host.get("IS_OFFLINE")),
            online=False,
            now=checked_at,
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
    process_status: dict,
    stale_after_sec: int,
    batch_size: int,
    icmp_timeout_sec: float,
    connectivity_module: Any = host_connectivity,
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

    checked = 0
    skipped_busy = 0
    icmp_online = 0
    icmp_offline = 0
    recovery_probes = 0

    # Process each due host independently.
    # One bad station must not abort the rest of the batch.
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
                connectivity_module=connectivity_module,
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
