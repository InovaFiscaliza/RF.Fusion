#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
appCataloga_host_check.py — Host Connectivity and Task State Synchronizer

This microservice verifies the connectivity of all registered hosts and ensures
consistency between host availability (HOST.IS_OFFLINE) and the state of pending
tasks in HOST_TASK and FILE_TASK tables.

Behavior:
    1. Fetch all hosts from the HOST table.
    2. For each host, test TCP connectivity to its configured port.
    3. If the host is OFFLINE:
         - Mark it as offline in HOST.
         - Increment NU_HOST_CHECK_ERROR counter.
         - Suspend all its HOST_TASK and FILE_TASK entries.
    4. If the host is ONLINE:
         - Clear offline flag.
         - Resume any suspended or error tasks to PENDING.
    5. Use adaptive intervals based on DT_LAST_CHECK and host state.
"""

# ======================================================================
# Imports
# ======================================================================
import sys
import os
import time
import socket
import inspect
import signal
from datetime import datetime, timezone, timedelta

# Configuration and database imports
_CFG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga"))
if _CFG_DIR not in sys.path and os.path.isdir(_CFG_DIR):
    sys.path.append(_CFG_DIR)

_DB_DIR = os.path.join(os.path.dirname(__file__), "db")
if _DB_DIR not in sys.path and os.path.isdir(_DB_DIR):
    sys.path.append(_DB_DIR)

import shared as sh
from db.dbHandlerBKP import dbHandlerBKP
import config as k


# ======================================================================
# Globals
# ======================================================================
log = sh.log()
process_status = {"running": True}


# ======================================================================
# Signal Handling
# ======================================================================
def _signal_handler(sig=None, frame=None):
    """Handle SIGTERM and SIGINT for graceful shutdown."""
    global process_status
    func = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"Signal {sig} received at {func}() — stopping host check loop.")
    process_status["running"] = False


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


# ======================================================================
# Connectivity Helper
# ======================================================================
def is_host_online(host_addr: str, host_port: int, timeout: int = 3) -> bool:
    """
    Perform a lightweight TCP connectivity test.

    Args:
        host_addr (str): IP or hostname of the target.
        host_port (int): TCP port to connect.
        timeout (int, optional): Timeout in seconds. Defaults to 3.

    Returns:
        bool: True if reachable, False otherwise.
    """
    try:
        with socket.create_connection((host_addr, host_port), timeout=timeout):
            return True
    except Exception:
        return False


# ======================================================================
# Main Routine
# ======================================================================
def main():
    """Main host monitoring loop ensuring host-task consistency."""
    log.entry("[INIT] Host check microservice started.")
    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Failed to initialize database: {e}")
        sys.exit(1)

    while process_status["running"]:
        try:
            # ----------------------------------------------------------
            # Step 1 — Fetch all registered hosts
            # ----------------------------------------------------------
            hosts = db.host_read_all()
            if not hosts:
                log.entry("No hosts registered. Sleeping 60s.")
                time.sleep(60)
                continue

            now = datetime.now()

            for host in hosts:
                host_id = host["ID_HOST"]
                addr = host["NA_HOST_ADDRESS"]
                port = host.get("NA_HOST_PORT", 22)
                is_offline = bool(host.get("IS_OFFLINE", False))
                last_check = host.get("DT_LAST_CHECK")

                # ------------------------------------------------------
                # Step 2 — Adaptive check interval
                # ------------------------------------------------------
                online_interval = getattr(k, "HOST_CHECK_INTERVAL", 300)  # Default 5 min
                offline_interval = getattr(k, "HOST_CHECK_OFFLINE_INTERVAL", 60)  # Default 1 min

                # Determine appropriate check interval
                interval = offline_interval if is_offline else online_interval
                if last_check and isinstance(last_check, datetime):
                    elapsed = (now - last_check).total_seconds()
                    if elapsed < interval:
                        continue  # Skip if recently checked

                # ------------------------------------------------------
                # Step 3 — Connectivity test
                # ------------------------------------------------------
                online = is_host_online(addr, port)
                log.entry(f"[CHECK] Host {addr}:{port} → {'ONLINE' if online else 'OFFLINE'}")

                # ------------------------------------------------------
                # Step 4 — Atomic transaction for consistency
                # ------------------------------------------------------
                try:
                    if not online:
                        # Host unreachable → mark offline and suspend tasks
                        db.host_update(
                            host_id=host_id,
                            IS_OFFLINE=True,
                            NU_HOST_CHECK_ERROR=1,  # increment failure counter
                            DT_LAST_FAIL=now,
                            DT_LAST_CHECK=now,
                        )

                        db.host_task_suspend_by_host(host_id)
                        db.file_task_suspend_by_host(host_id)

                    else:
                        # Host reachable → reset offline flag and resume tasks if needed
                        db.host_update(
                            host_id=host_id,
                            IS_OFFLINE=False,
                            DT_LAST_CHECK=now,
                        )

                        # Host is online - check pending or error task and resume them
                        db.host_task_resume_by_host(host_id)
                        db.file_task_resume_by_host(host_id)

                except Exception as e:
                    log.error(f"[DB] Transaction failed for host {addr}: {e}")
                    time.sleep(1)

            # ----------------------------------------------------------
            # Step 5 — Sleep before next iteration
            # ----------------------------------------------------------
            sh._random_jitter_sleep()

        except Exception as e:
            log.error(f"[MAIN] Unexpected error in host check loop: {e}")
            time.sleep(10)

    log.entry("[STOP] Host check microservice stopped gracefully.")


# ======================================================================
# Entry Point
# ======================================================================
if __name__ == "__main__":
    main()
