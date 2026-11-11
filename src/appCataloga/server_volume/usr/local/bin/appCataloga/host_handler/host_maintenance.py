"""
Domain helpers for recurring host-maintenance sweeps.

`appCataloga_host_maintenance.py` owns the daemon lifecycle and cadence. This
module owns the host-specific meaning of one maintenance pass:
which HOST rows are due, how offline recovery is confirmed, and what one sweep
iteration should do with one host snapshot.

Reading guide:
    1. `select_due_hosts(...)` trims the full HOST list to the stale batch the
       recurring sweep should touch right now.
    2. `_persist_recovery_probe_result(...)` handles the strict recovery path
       used when a host is already marked offline.
    3. `process_due_host(...)` applies one maintenance pass to one HOST row.

The helpers intentionally work with the raw DB row shape. For this sweep, a
small amount of explicit field access is easier to maintain than introducing a
wrapper object that only renames dictionary keys.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, TypedDict, cast

from db import constants as db_constants
import config as k
from shared import tools

from host_handler import host_connectivity

if TYPE_CHECKING:
    from db.dbHandlerBKP import dbHandlerBKP
    from host_handler.host_ssh_utils import ConnectivityProbePayload
    from shared.logging_utils import log as logger_type


class HostSnapshot(TypedDict, total=False):
    """
    DB row fields consumed by the maintenance sweep.

    The production query supplies every field below. `total=False` keeps the
    type usable by focused tests that only need to model one branch of the
    maintenance state machine.
    """

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
    """
    One-host outcome aggregated into the maintenance batch summary.

    `checked` means the host passed the BUSY guard. `icmp_online` records the
    short pre-check result, while `recovery_probe` identifies hosts that needed
    the stricter ICMP-plus-SSH recovery decision.
    """

    checked: bool
    skipped_busy: bool
    icmp_online: bool
    recovery_probe: bool


def _probe_host_icmp(host: HostSnapshot, timeout_sec: float) -> bool:
    """
    Run the lightweight ICMP pre-check for one host snapshot.

    A false result only selects the host for the canonical confirmation probe;
    it does not by itself mark the host offline or change queue state.
    """

    return host_connectivity.is_host_online(
        host["NA_HOST_ADDRESS"],
        timeout_sec=timeout_sec,
    )


def _probe_host_operational_state(host: HostSnapshot) -> ConnectivityProbePayload:
    """
    Run the shared ICMP-and-SSH operational probe for one host snapshot.

    This helper only collects the shared probe payload. The caller owns the
    persistence decision, so the same result can be applied sequentially after
    parallel network work has completed.
    """

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
    """
    Probe ICMP reachability with bounded parallelism.

    Results remain keyed by `ID_HOST`, allowing the caller to persist state in
    the original batch order rather than in completion order.
    """

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
    """
    Run bounded parallel operational probes and return their raw payloads.

    The probes may finish in any order. Returning a host-keyed mapping keeps
    network fan-out separate from the later sequential DB reconciliation.
    """

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
    Return the first stale hosts from the DB ordering for this maintenance pass.

    The source snapshot may prioritize online hosts ahead of offline hosts, so
    freshness can no longer be inferred from the first fresh row alone. Filter
    the full snapshot and keep only stale rows until the batch is full.

    The returned rows are the original DB dictionaries. That keeps the sweep
    transparent and avoids a wrapper that only renames keys.
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

    The probe event and SSH signal are recorded before the shared state helper
    decides whether suspended host-dependent work can be resumed.
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
    host_connectivity.persist_ssh_probe_signal(
        host["ID_HOST"],
        connectivity,
        observed_at=checked_at,
        logger=log,
    )

    if connectivity["state"] == "online":
        # Only a fully operational result may resume suspended work.
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
    whether the host transitions to offline. A host that still answers ICMP
    keeps its current operational state, even when the SSH signal is degraded;
    that signal is persisted separately for operators and queue safeguards.
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
        host_connectivity.persist_reachable_probe_observation(
            db=db,
            host_id=host["ID_HOST"],
            connectivity=connectivity,
            observed_at=checked_at,
            logger=log,
        )
        return

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
        2. use the supplied or local lightweight ICMP pre-check
        3. use the supplied or local canonical probe when confirmation is due
        4. persist the resulting steady-state, recovery, or offline transition

    Return a small structured result used by the batch summary.

    The maintenance daemon is high-frequency, so the caller aggregates these
    results into one compact batch log. Precomputed probe payloads let the
    batch run network I/O in parallel without repeating it during persistence.
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
        host_connectivity.persist_icmp_observation(
            db=db,
            host_id=host["ID_HOST"],
            observed_at=checked_at,
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

    # BUSY hosts are counted later but never receive maintenance network I/O.
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
    # Offline hosts always require strict recovery. Online hosts only need the
    # more expensive probe after the short ICMP pre-check has failed.
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

    # Phase 3: apply the probe results sequentially. Network fan-out is
    # parallel, but persisted host state remains linear and deterministic.
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


def cleanup_stale_host_locks(
    db: dbHandlerBKP,
    *,
    log: logger_type,
    threshold_seconds: int,
) -> None:
    """Recover abandoned host reservations and request a new connection check.

    Args:
        db (dbHandlerBKP): Operational persistence session.
        log (logger_type): Maintenance logger for per-host failures and long locks.
        threshold_seconds (int): Busy age after which a dead owner can be reclaimed;
            the no-task grace and transient SFTP cooldown retain their own limits.

    Returns:
        None. Preserve live owners and recent cooldowns. Release an eligible
        reservation only when its observed PID and timestamp still match, then
        enqueue a connection check. A failed host does not stop the remaining sweep.

    Raises:
        psycopg.Error: If reading the maintenance snapshot fails. Per-host write
            failures are logged and the sweep continues.
    """
    now = datetime.utcnow()
    rows = db.host_list_busy_with_task_state(
        task_status=db_constants['tasks']['states']['running'],
        host_task_type=db_constants['tasks']['host_types']['processing'],
    )
    for row in rows:
        host_id = cast(int, row["ID_HOST"])
        busy_since = cast(datetime | None, row["DT_BUSY"])
        pid = cast(int | None, row["NU_PID"])
        file_running = row["FILE_RUNNING"]
        host_processing_running = row["HOST_PROCESSING_RUNNING"]
        if busy_since is None:
            elapsed = float("inf")
        else:
            elapsed = (now - busy_since).total_seconds()
        release = False
        if (
            pid == db_constants['hosts']['transient_busy_pid']
            and elapsed <= k.SFTP_BUSY_COOLDOWN_SECONDS
        ):
            continue
        if pid == db_constants['hosts']['transient_busy_pid']:
            release = True
        elif not file_running and (not host_processing_running):
            release = elapsed > k.HOST_CLEANUP_NO_TASK_GRACE_SEC
        elif elapsed > threshold_seconds and (
            pid in (None, db_constants['hosts']['unlocked_pid']) or not tools.pid_exists(pid)
        ):
            release = True
        elif elapsed > threshold_seconds:
            log.warning_event(
                "db_long_running_lock",
                operation="host_cleanup_stale_locks",
                table="HOST",
                host_id=host_id,
                owner_pid=pid,
                busy_for_sec=round(elapsed, 1),
            )
        if not release:
            continue
        try:
            if not db.host_release_reservation(
                host_id=host_id,
                owner_pid=pid,
                busy_since=busy_since,
            ):
                continue
        except Exception as e:
            log.error_event(
                "db_update_failed",
                operation="host_cleanup_stale_locks",
                table="HOST",
                host_id=host_id,
                error=type(e).__name__,
            )
            continue
        try:
            db.host_task_enqueue(
                host_id=host_id,
                task_type=db_constants['tasks']['host_types']['check_connection'],
                task_status=db_constants['tasks']['states']['pending'],
                filter_dict=db_constants['tasks']['default_filter'],
            )
        except Exception as e:
            log.error_event(
                "db_insert_failed",
                operation="host_cleanup_queue_connection_check",
                table="HOST_TASK",
                host_id=host_id,
                error=type(e).__name__,
            )


def cleanup_stale_operational_tasks(
    db: dbHandlerBKP,
    *,
    log: logger_type,
    stale_after_seconds: int,
) -> None:
    """Recover stale host tasks using process liveness and host ownership.

    Args:
        db (dbHandlerBKP): Operational persistence session.
        log (logger_type): Maintenance logger for isolated per-task failures.
        stale_after_seconds (int): Minimum RUNNING age before owner checks;
            missing timestamps count as stale. PENDING timestamps are normalized.

    Returns:
        None. Requeue eligible stale tasks and release only stale host owners,
        comparing the reservation's observed PID and timestamp before release.
        Active matching owners are preserved, including long-running tasks.

    Raises:
        psycopg.Error: If reading tasks and host state fails. Individual recovery
            failures are logged without aborting the remaining sweep.
    """
    now = datetime.utcnow()
    task_types = (
        db_constants['tasks']['host_types']['check'],
        db_constants['tasks']['host_types']['processing'],
        db_constants['tasks']['host_types']['check_connection'],
        db_constants['tasks']['host_types']['backlog_control'],
        db_constants['tasks']['host_types']['backlog_rollback'],
        db_constants['tasks']['host_types']['interactive_check'],
    )
    rows = db.host_task_list_with_host_state(
        task_types=task_types,
        task_statuses=(db_constants['tasks']['states']['pending'], db_constants['tasks']['states']['running']),
    )
    for row in rows:
        task_id = cast(int, row["ID_HOST_TASK"])
        host_id = cast(int, row["FK_HOST"])
        status = row["NU_STATUS"]
        task_started_at = cast(datetime | None, row["DT_HOST_TASK"])
        task_pid = cast(int | None, row["NU_PID"])
        host_is_busy = bool(row["IS_BUSY"])
        host_owner_pid = cast(int | None, row["HOST_OWNER_PID"])
        if status == db_constants['tasks']['states']['pending'] and task_started_at is None:
            try:
                db.host_task_update(
                    task_id=task_id,
                    DT_HOST_TASK=now,
                    NA_MESSAGE="Operational HOST_TASK timestamp normalized by janitor for reservation TTL control",
                )
            except Exception as e:
                log.error_event(
                    "db_update_failed",
                    operation="host_task_cleanup_normalize_pending",
                    table="HOST_TASK",
                    task_id=task_id,
                    host_id=host_id,
                    error=type(e).__name__,
                )
            continue
        if status != db_constants['tasks']['states']['running']:
            continue
        elapsed = (
            float("inf")
            if task_started_at is None
            else (now - task_started_at).total_seconds()
        )
        if elapsed <= stale_after_seconds:
            continue
        task_pid_alive = bool(task_pid) and tools.pid_exists(task_pid)
        host_owner_alive = bool(host_owner_pid) and tools.pid_exists(
            host_owner_pid
        )
        if host_is_busy and task_pid_alive and (host_owner_pid == task_pid):
            continue
        reasons = []
        if not host_is_busy:
            reasons.append("HOST_NOT_BUSY")
        if task_pid in (None, db_constants['hosts']['unlocked_pid']):
            reasons.append("TASK_PID_MISSING")
        elif not task_pid_alive:
            reasons.append("TASK_PID_STALE")
        if host_is_busy and host_owner_pid not in (None, db_constants['hosts']['unlocked_pid']):
            if not host_owner_alive:
                reasons.append("HOST_PID_STALE")
            elif (
                task_pid not in (None, db_constants['hosts']['unlocked_pid'])
                and host_owner_pid != task_pid
            ):
                reasons.append("HOST_PID_MISMATCH")
        elif host_is_busy:
            reasons.append("HOST_PID_MISSING")
        if not reasons:
            reasons.append("STALE_RUNNING_TTL_EXPIRED")
        try:
            db.host_task_update(
                task_id=task_id,
                NU_STATUS=db_constants['tasks']['states']['pending'],
                DT_HOST_TASK=now,
                NA_MESSAGE=f"Stale operational HOST_TASK recovered by janitor ({', '.join(reasons)})",
            )
        except Exception as e:
            log.error_event(
                "db_update_failed",
                operation="host_task_cleanup_recover_running",
                table="HOST_TASK",
                task_id=task_id,
                host_id=host_id,
                error=type(e).__name__,
            )
            continue
        should_release_host = host_is_busy and (
            host_owner_pid in (None, db_constants['hosts']['unlocked_pid'])
            or not host_owner_alive
            or (
                task_pid not in (None, db_constants['hosts']['unlocked_pid'])
                and host_owner_pid == task_pid
                and (not task_pid_alive)
            )
        )
        if not should_release_host:
            continue
        try:
            db.host_release_reservation(
                host_id=host_id,
                owner_pid=host_owner_pid,
                busy_since=cast(datetime | None, row["DT_BUSY"]),
            )
        except Exception as e:
            log.error_event(
                "db_update_failed",
                operation="host_task_cleanup_release_host_lock",
                table="HOST",
                task_id=task_id,
                host_id=host_id,
                error=type(e).__name__,
            )


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

    Args:
        db (dbHandlerBKP): Operational persistence session.
        log (logger_type): Maintenance logger for each isolated failure.
        task_stale_after_sec (int): RUNNING task age before owner checks.
        host_busy_timeout_sec (int): BUSY age before stale-owner recovery.

    Returns:
        None. Run task recovery before host-lock recovery; log failures and
        continue to the independent step.
    """
    try:
        # Step 1: recover HOST_TASK rows that were left pending or running by
        # crashed workers so the queue does not stall indefinitely.
        cleanup_stale_operational_tasks(
            db=db, log=log, stale_after_seconds=task_stale_after_sec
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
        cleanup_stale_host_locks(
            db=db, log=log, threshold_seconds=host_busy_timeout_sec
        )
    except Exception as e:
        log.error_event(
            "host_cleanup_failed",
            component="host_maintenance",
            operation="cleanup_stale_host_locks",
            error=e,
        )
