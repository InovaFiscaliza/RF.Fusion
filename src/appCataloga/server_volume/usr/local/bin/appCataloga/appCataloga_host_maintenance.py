#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Recurring host maintenance daemon.

This daemon owns periodic maintenance that is not triggered by explicit
HOST_TASK rows:

    - stale operational HOST_TASK cleanup
    - stale HOST lock cleanup
    - background oldest-first ICMP sweep for stale HOST snapshots

Queue-driven HOST_TASK processing stays in `appCataloga_host_check.py`. This
daemon intentionally does not create new HOST_TASK rows; it resolves recurring
checks directly to avoid mixing scheduler work with queue work.
"""

from __future__ import annotations

import inspect
import os
import signal
import sys
from datetime import datetime, timedelta


# =================================================
# PROJECT ROOT (shared/, db/, stations/)
# =================================================
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# =================================================
# Config directory (etc/appCataloga)
# =================================================
_CFG_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga")
)
if _CFG_DIR not in sys.path and os.path.isdir(_CFG_DIR):
    sys.path.append(_CFG_DIR)

# =================================================
# DB directory
# =================================================
_DB_DIR = os.path.join(PROJECT_ROOT, "db")
if _DB_DIR not in sys.path and os.path.isdir(_DB_DIR):
    sys.path.append(_DB_DIR)

from db.dbHandlerBKP import dbHandlerBKP
from shared import host_connectivity, legacy, logging_utils
import config as k


log = logging_utils.log()
process_status = {"running": True}


class MaintenanceHost:
    """Small immutable view over the HOST fields used by maintenance."""

    def __init__(
        self,
        *,
        host_id: int,
        address: str,
        name: str | None,
        port: int,
        user: str,
        password: str,
        is_busy: bool,
        was_offline: bool,
        last_check: datetime | None,
    ) -> None:
        self.host_id = host_id
        self.address = address
        self.name = name
        self.port = port
        self.user = user
        self.password = password
        self.is_busy = is_busy
        self.was_offline = was_offline
        self.last_check = last_check

    @classmethod
    def from_row(cls, row: dict) -> "MaintenanceHost":
        return cls(
            host_id=row["ID_HOST"],
            address=row["NA_HOST_ADDRESS"],
            name=row.get("NA_HOST_NAME"),
            port=int(row["NA_HOST_PORT"]),
            user=row["NA_HOST_USER"],
            password=row["NA_HOST_PASSWORD"],
            is_busy=bool(row.get("IS_BUSY")),
            was_offline=bool(row.get("IS_OFFLINE")),
            last_check=row.get("DT_LAST_CHECK"),
        )


def release_busy_hosts_on_exit() -> None:
    """
    Release all HOST records marked as BUSY by this process PID.
    """
    try:
        pid = os.getpid()
        log.event("cleanup_busy_hosts", pid=pid)
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
        db.host_release_by_pid(pid)
    except Exception as e:
        log.error(f"event=cleanup_busy_hosts_failed error={e}")


def _signal_handler(signal_name: str) -> None:
    current_function = inspect.currentframe().f_back.f_code.co_name
    log.signal_received(signal_name, handler=current_function)
    process_status["running"] = False
    release_busy_hosts_on_exit()


def sigterm_handler(signal=None, frame=None) -> None:
    _signal_handler("SIGTERM")


def sigint_handler(signal=None, frame=None) -> None:
    _signal_handler("SIGINT")


signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


def _select_due_hosts(host_rows: list[dict], now: datetime) -> list[MaintenanceHost]:
    """
    Return the oldest stale hosts that should be inspected in this batch.

    The DB query is already ordered by age, so the first fresh row lets us stop
    scanning immediately without touching the remainder of the list.
    """
    stale_after = timedelta(seconds=k.HOST_CHECK_ALL_STALE_AFTER_SEC)
    due_hosts: list[MaintenanceHost] = []

    for row in host_rows:
        host = MaintenanceHost.from_row(row)

        if host.last_check and (now - host.last_check) < stale_after:
            break

        due_hosts.append(host)

        if len(due_hosts) >= k.HOST_CHECK_ALL_BATCH_SIZE:
            break

    return due_hosts


def _log_host_check_all(host: MaintenanceHost, *, online: bool) -> None:
    """Emit the coarse ICMP-only result for the recurring sweep."""
    log.event(
        "host_check_all",
        host_id=host.host_id,
        host=host.name,
        address=host.address,
        online=online,
    )


def _log_host_recovery_probe(host: MaintenanceHost, connectivity: dict) -> None:
    """Emit the stricter recovery probe used only for previously offline hosts."""
    log.event(
        "host_check_all_recovery_probe",
        host_id=host.host_id,
        host=host.name,
        address=host.address,
        state=connectivity["state"],
        reason=connectivity["reason"],
        icmp_online=connectivity["icmp_online"],
        ssh_online=connectivity["ssh_online"],
        error=connectivity["error"],
    )


def _recover_offline_host_if_operational(
    db: dbHandlerBKP,
    host: MaintenanceHost,
    checked_at: datetime,
) -> None:
    """
    Recover an offline host only after both ICMP and SSH confirm it is back.
    """
    connectivity = host_connectivity.probe_host_operational_connectivity(
        addr=host.address,
        port=host.port,
        user=host.user,
        password=host.password,
    )
    _log_host_recovery_probe(host, connectivity)

    if connectivity["state"] == "online":
        host_connectivity.persist_host_connectivity_state(
            db=db,
            log=log,
            host_id=host.host_id,
            was_offline=host.was_offline,
            online=True,
            now=checked_at,
        )
        return

    # Keep the host offline until the operational SSH endpoint is confirmed.
    db.host_update(
        host_id=host.host_id,
        IS_OFFLINE=True,
        DT_LAST_CHECK=checked_at,
        DT_LAST_FAIL=checked_at,
    )


def _process_due_host(db: dbHandlerBKP, host: MaintenanceHost, checked_at: datetime) -> bool:
    """
    Process one stale host snapshot.

    Returns True when the host was actually checked, False when it was skipped.
    """
    if host.is_busy:
        log.event(
            "host_check_all_skipped_busy",
            host_id=host.host_id,
            host=host.name,
            address=host.address,
        )
        return False

    online = host_connectivity.is_host_online(
        host.address,
        timeout_sec=k.HOST_CHECK_ALL_ICMP_TIMEOUT_SEC,
    )
    _log_host_check_all(host, online=online)

    if online and host.was_offline:
        _recover_offline_host_if_operational(db=db, host=host, checked_at=checked_at)
    elif online:
        db.host_update(
            host_id=host.host_id,
            DT_LAST_CHECK=checked_at,
        )
    else:
        host_connectivity.persist_host_connectivity_state(
            db=db,
            log=log,
            host_id=host.host_id,
            was_offline=host.was_offline,
            online=False,
            now=checked_at,
        )

    return True


def run_host_check_all_batch(db: dbHandlerBKP, now: datetime) -> int:
    """
    Refresh a small oldest-first batch of stale HOST connectivity snapshots.

    The recurring sweep intentionally skips BUSY hosts so it does not compete
    with discovery/backup for the same SSH endpoint and produce self-inflicted
    false negatives.
    """
    if not k.HOST_CHECK_ALL_ENABLED:
        return 0

    hosts = db.host_list_for_connectivity_check()
    if not hosts:
        return 0

    due_hosts = _select_due_hosts(hosts, now)
    if not due_hosts:
        return 0

    log.event(
        "host_check_all_batch",
        batch_size=len(due_hosts),
        stale_after_sec=k.HOST_CHECK_ALL_STALE_AFTER_SEC,
        timeout_sec=k.HOST_CHECK_ALL_ICMP_TIMEOUT_SEC,
    )

    checked = 0

    for host in due_hosts:
        if not process_status["running"]:
            break

        try:
            if _process_due_host(db=db, host=host, checked_at=now):
                checked += 1
        except Exception as e:
            log.error(
                f"event=host_check_all_failed host_id={host.host_id} "
                f"host={host.name} address={host.address} error={e}"
            )

    if checked:
        log.event("host_check_all_done", checked=checked)

    return checked


def main() -> None:
    """
    Run periodic host maintenance until shutdown is requested.

    This loop never consumes queued HOST_TASK rows. It only performs recurring
    reconciliation that should happen even when no explicit host task exists.
    """
    log.service_start("appCataloga_host_maintenance")
    last_host_cleanup = datetime.min

    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"event=db_init_failed service=appCataloga_host_maintenance error={e}")
        sys.exit(1)

    while process_status["running"]:
        now = datetime.now()

        try:
            if now - last_host_cleanup > timedelta(seconds=k.HOST_CLEANUP_INTERVAL):
                # Cleanup runs on its own cadence so stale task recovery and
                # stale lock recovery stay decoupled from the slower ICMP sweep.
                try:
                    db.host_task_cleanup_stale_operational_tasks(
                        stale_after_seconds=k.HOST_TASK_OPERATIONAL_STALE_SEC
                    )
                except Exception as e:
                    log.error(f"event=host_task_cleanup_failed error={e}")

                try:
                    db.host_cleanup_stale_locks(
                        threshold_seconds=k.HOST_BUSY_TIMEOUT
                    )
                except Exception as e:
                    log.error(f"event=host_cleanup_failed error={e}")

                last_host_cleanup = now

            try:
                # The sweep is intentionally lightweight and oldest-first; it
                # refreshes only a bounded batch per loop iteration.
                run_host_check_all_batch(db=db, now=now)
            except Exception as e:
                log.error(f"event=host_check_all_batch_failed error={e}")

            legacy._random_jitter_sleep()

        except Exception as e:
            log.error(f"event=host_maintenance_loop_failed error={e}")
            legacy._random_jitter_sleep()

    log.service_stop("appCataloga_host_maintenance")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        release_busy_hosts_on_exit()
        raise
