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

import sys
from datetime import datetime, timedelta

from bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

from db.dbHandlerBKP import dbHandlerBKP
from host_handler import host_connectivity, host_runtime, maintenance as maintenance_flow
from server_handler import signal_runtime, sleep as runtime_sleep
from shared import logging_utils
import config as k


SERVICE_NAME = "appCataloga_host_maintenance"
log = logging_utils.log()
process_status = {"running": True}


def _shutdown_cleanup(signal_name: str) -> None:
    """
    Release BUSY host locks during process shutdown.
    """
    host_runtime.release_busy_hosts_for_current_pid(
        db_factory=dbHandlerBKP,
        database_name=k.BKP_DATABASE_NAME,
        logger=log,
    )


signal_runtime.install_shutdown_handlers(
    process_status=process_status,
    logger=log,
    on_shutdown=_shutdown_cleanup,
)


def main() -> None:
    """
    Run periodic host maintenance until shutdown is requested.

    This loop never consumes queued HOST_TASK rows. It only performs recurring
    reconciliation that should happen even when no explicit host task exists.
    """
    log.service_start(SERVICE_NAME)
    last_host_cleanup = datetime.min

    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"event=db_init_failed service={SERVICE_NAME} error={e}")
        sys.exit(1)

    while process_status["running"]:
        now = datetime.now()

        try:
            if now - last_host_cleanup > timedelta(seconds=k.HOST_CLEANUP_INTERVAL):
                # Cleanup runs on its own cadence so stale task recovery and
                # stale lock recovery stay decoupled from the slower ICMP sweep.
                maintenance_flow.run_periodic_host_cleanup(
                    db=db,
                    log=log,
                    task_stale_after_sec=k.HOST_TASK_OPERATIONAL_STALE_SEC,
                    host_busy_timeout_sec=k.HOST_BUSY_TIMEOUT,
                )
                last_host_cleanup = now

            try:
                # The sweep is intentionally lightweight and oldest-first; it
                # refreshes only a bounded batch per loop iteration.
                if k.HOST_CHECK_ALL_ENABLED:
                    maintenance_flow.run_host_check_all_batch(
                        db=db,
                        log=log,
                        now=now,
                        process_status=process_status,
                        stale_after_sec=k.HOST_CHECK_ALL_STALE_AFTER_SEC,
                        batch_size=k.HOST_CHECK_ALL_BATCH_SIZE,
                        icmp_timeout_sec=k.HOST_CHECK_ALL_ICMP_TIMEOUT_SEC,
                        connectivity_module=host_connectivity,
                    )
            except Exception as e:
                log.error(f"event=host_check_all_batch_failed error={e}")

            runtime_sleep.random_jitter_sleep()

        except Exception as e:
            log.error(f"event=host_maintenance_loop_failed error={e}")
            runtime_sleep.random_jitter_sleep()

    log.service_stop(SERVICE_NAME)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # This is the daemon-level crash path. Per-iteration failures are
        # already contained inside `main()`, so reaching here means the
        # process itself is not trustworthy anymore.
        host_runtime.release_busy_hosts_for_current_pid(
            db_factory=dbHandlerBKP,
            database_name=k.BKP_DATABASE_NAME,
            logger=log,
        )
        raise
