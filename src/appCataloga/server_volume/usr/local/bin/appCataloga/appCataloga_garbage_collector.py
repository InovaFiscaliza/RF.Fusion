#!/usr/bin/python3
"""
appCataloga_garbage_collector

Worker responsible for cleaning payload files that remain in the repository
trash area beyond a defined quarantine period.

Pipeline (deterministic):

    ACT I       - Fetch FILE_TASK_HISTORY garbage candidates
    ACT II      - Validate filesystem path
    ACT III     - Delete payload file
    ACT IV      - Mark payload deletion in FILE_TASK_HISTORY

Design principles:
- FILE_TASK_HISTORY is the single source of truth
- Payload deletion must be idempotent
- Missing files are treated as already deleted
- No modification of FILE_TASK
"""

import sys
import os
import time
import signal
import inspect
from datetime import datetime


# ---------------------------------------------------------------
# Configuration path (shared config and handlers)
# ---------------------------------------------------------------
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


# ---------------------------------------------------------------
# Internal modules
# ---------------------------------------------------------------
import config as k
from shared import logging_utils
from db.dbHandlerBKP import dbHandlerBKP


# ===============================================================
# GLOBAL STATE
# ===============================================================

log = logging_utils.log(target_screen=False)
process_status = {"running": True}


# ===============================================================
# SIGNAL HANDLING
# ===============================================================

def _signal_handler(signal=None, frame=None):
    """
    Gracefully stop worker.
    """
    fn = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"SIGNAL received at {fn}()")
    process_status["running"] = False


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


# ===============================================================
# FILE OPERATIONS
# ===============================================================

def delete_file(path):
    """
    Delete a payload file.

    Behavior:
        - If file exists → delete
        - If file missing → treat as already deleted
    """

    try:
        os.remove(path)
        log.entry(f"[GC] Deleted payload: {path}")
        return True

    except FileNotFoundError:
        log.warning(f"[GC] File already missing: {path}")
        return True

    except Exception as e:
        log.error(f"[GC] Failed to delete {path}: {e}")
        return False


# ===============================================================
# MAIN LOOP
# ===============================================================

def main():
    """
    Main garbage collection loop.
    """

    log.entry("[INIT] appCataloga_garbage_collector started")

    db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)

    while process_status["running"]:

        try:

            rows = db_bp.file_history_get_gc_candidates(
                batch_size=k.GC_BATCH_SIZE,
                quarantine_days=k.GC_QUARANTINE_DAYS
            )

            if not rows:
                log.entry("[GC] No eligible garbage files")
                time.sleep(k.GC_IDLE_SLEEP)
                continue

            deleted = 0

            for row in rows:

                server_path = row["NA_SERVER_FILE_PATH"]
                server_file = row["NA_SERVER_FILE_NAME"]

                if not server_path or not server_file:
                    log.warning("[GC] Invalid path metadata")
                    continue

                # Safety check: refuse deletion outside trash
                if k.TRASH_FOLDER not in server_path:
                    log.error(
                        f"[GC] Refusing deletion outside trash: {server_path}"
                    )
                    continue

                file_path = os.path.join(server_path, server_file)

                if delete_file(file_path):

                    db_bp.file_history_update(
                        history_id=row["ID_HISTORY"],
                        IS_PAYLOAD_DELETED=1,
                        DT_PAYLOAD_DELETED=datetime.now()
                    )

                    deleted += 1

            db_bp.commit()

            log.entry(f"[GC] Processed {deleted} payload deletions")

        except Exception as e:
            log.error(f"[GC ERROR] {e}")

        time.sleep(k.GC_LOOP_SLEEP)


if __name__ == "__main__":
    main()