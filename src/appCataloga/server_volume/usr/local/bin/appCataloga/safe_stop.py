#!/usr/bin/python3
"""
Force cleanup of BUSY hosts and reset RUNNING FILE_TASKs and HOST_TASKs to PENDING.

This script must be executed only after stopping all appCataloga workers.
"""

from datetime import datetime
import sys, os

# ----------------------------------------------------------------------
# Load configuration and database modules
# ----------------------------------------------------------------------
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

# Import customized libs
from db.dbHandlerBKP import dbHandlerBKP
from shared import  logging_utils
import config as k

log = logging_utils.log()

def cleanup_hosts_and_tasks():
    log.entry("[CLEANUP] Starting forced cleanup (HOST + HOST_TASK + FILE_TASK)")

    db = None
    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
        db._connect()

        # ---------------------------------------------------------
        # 1) Reset RUNNING FILE_TASKs → PENDING
        # ---------------------------------------------------------
        rows = db._select_rows(
            table="FILE_TASK",
            where={"NU_STATUS": k.TASK_RUNNING},
            cols=["ID_FILE_TASK", "FK_HOST", "NU_PID"],
        )

        for row in rows:
            task_id = row["ID_FILE_TASK"]
            host_id = row["FK_HOST"]
            pid = row.get("NU_PID", 0)

            log.warning(
                f"[CLEANUP] Resetting FILE_TASK to PENDING "
                f"(task={task_id}, host={host_id}, pid={pid})"
            )

            db.file_task_update(
                task_id=task_id,
                NU_STATUS=k.TASK_PENDING,
                DT_FILE_TASK=datetime.now(),
                NA_MESSAGE="Task reset to PENDING due to controlled shutdown",
            )

        log.entry(f"[CLEANUP] Reset {len(rows)} RUNNING FILE_TASKs")

        # ---------------------------------------------------------
        # 2) Reset RUNNING HOST_TASKs → PENDING   (NEW)
        # ---------------------------------------------------------
        host_tasks = db._select_rows(
            table="HOST_TASK",
            where={"NU_STATUS": k.TASK_RUNNING},
            cols=["ID_HOST_TASK", "FK_HOST", "NU_PID"],
        )

        for row in host_tasks:
            host_task_id = row["ID_HOST_TASK"]
            host_id = row["FK_HOST"]
            pid = row.get("NU_PID", 0)

            log.warning(
                f"[CLEANUP] Resetting HOST_TASK to PENDING "
                f"(host_task={host_task_id}, host={host_id}, pid={pid})"
            )

            db.host_task_update(
                task_id=host_task_id,
                NU_STATUS=k.TASK_PENDING,
                DT_HOST_TASK=None,
                NA_MESSAGE="Host task reset to PENDING due to controlled shutdown",
            )

        log.entry(f"[CLEANUP] Reset {len(host_tasks)} RUNNING HOST_TASKs")

        # ---------------------------------------------------------
        # 3) Force release BUSY HOSTs
        # ---------------------------------------------------------
        hosts = db._select_rows(
            table="HOST",
            where={"IS_BUSY": True},
            cols=["ID_HOST", "NU_PID"],
        )

        for row in hosts:
            host_id = row["ID_HOST"]
            pid = row.get("NU_PID", 0)

            log.warning(
                f"[CLEANUP] Forcing HOST release "
                f"(host={host_id}, pid={pid})"
            )

            db.host_update(
                host_id=host_id,
                IS_BUSY=False,
                NU_PID=k.HOST_UNLOCKED_PID,
                DT_BUSY=None,
            )

        log.entry(f"[CLEANUP] Released {len(hosts)} BUSY hosts")

    except Exception as e:
        log.error(f"[CLEANUP] Failed during forced cleanup: {e}")

    finally:
        if db:
            db._disconnect()

    log.entry("[CLEANUP] Forced cleanup finished")


if __name__ == "__main__":
    cleanup_hosts_and_tasks()
