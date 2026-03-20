#!/usr/bin/python3
"""
Repository garbage-collection worker.

Worker responsible for cleaning payload files that remain in the repository
trash area beyond a defined quarantine period. The worker is deliberately small
because it is the last step in the file lifecycle and must stay easy to reason
about during production cleanup incidents.
"""

import os
import inspect
import signal
import sys
import time
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
from shared import errors, logging_utils
from db.dbHandlerBKP import dbHandlerBKP


# ===============================================================
# GLOBAL STATE
# ===============================================================

log = logging_utils.log(target_screen=False)
process_status = {"running": True}


# ===============================================================
# Signal handling
# ===============================================================
def _signal_handler(signal_name: str) -> None:
    """
    Register shutdown intent for the garbage-collection loop.
    """
    fn = inspect.currentframe().f_back.f_code.co_name
    log.signal_received(signal_name, handler=fn)
    process_status["running"] = False


def sigterm_handler(signal=None, frame=None) -> None:
    """
    Handle SIGTERM by requesting a graceful shutdown.
    """
    _signal_handler("SIGTERM")


def sigint_handler(signal=None, frame=None) -> None:
    """
    Handle SIGINT by requesting a graceful shutdown.
    """
    _signal_handler("SIGINT")


signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


# ===============================================================
# File operations
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
        log.event("garbage_file_deleted", path=path)
        return True

    except FileNotFoundError:
        log.warning(f"event=garbage_file_missing path={path}")
        return True

    except Exception as e:
        log.error(f"event=garbage_delete_failed path={path} error={e}")
        return False


def build_resolved_files_trash_path() -> str:
    """
    Return the dedicated quarantine used by appAnalise export leftovers.

    Files in this folder are no longer part of the canonical repository
    lineage, so they are collected directly from the filesystem instead of
    through `FILE_TASK_HISTORY`.
    """
    return os.path.join(
        k.REPO_FOLDER,
        k.TRASH_FOLDER,
        k.RESOLVED_FILES_TRASH_SUBDIR,
    )


def get_resolved_files_gc_candidates(batch_size: int, quarantine_days: int):
    """
    Return aged files from `trash/resolved_files` for direct filesystem cleanup.
    """
    resolved_root = build_resolved_files_trash_path()

    if not os.path.isdir(resolved_root):
        return []

    cutoff_ts = time.time() - (quarantine_days * 86400)
    candidates = []

    for root, _, files in os.walk(resolved_root):
        for name in files:
            full_path = os.path.join(root, name)

            try:
                modified_at = os.path.getmtime(full_path)
            except FileNotFoundError:
                continue
            except Exception as e:
                log.error(
                    f"event=garbage_resolved_files_stat_failed path={full_path} error={e}"
                )
                continue

            if modified_at <= cutoff_ts:
                candidates.append((modified_at, full_path))

    candidates.sort(key=lambda item: item[0])
    return [path for _, path in candidates[:batch_size]]


# ===============================================================
# Main loop
# ===============================================================
def main():
    """
    Run the garbage-collection loop until shutdown is requested.
    """

    log.service_start("appCataloga_garbage_collector")

    db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)

    while process_status["running"]:

        try:

            history_rows = db_bp.file_history_get_gc_candidates(
                batch_size=k.GC_BATCH_SIZE,
                quarantine_days=k.GC_QUARANTINE_DAYS
            )
            resolved_rows = get_resolved_files_gc_candidates(
                batch_size=k.GC_BATCH_SIZE,
                quarantine_days=k.GC_QUARANTINE_DAYS,
            )

            if not history_rows and not resolved_rows:
                log.event("garbage_candidates_empty")
                time.sleep(k.GC_IDLE_SLEEP)
                continue

            deleted = 0
            deleted_history_payloads = 0
            deleted_resolved_files = 0

            for row in history_rows:

                server_path = row["NA_SERVER_FILE_PATH"]
                server_file = row["NA_SERVER_FILE_NAME"]

                if not server_path or not server_file:
                    log.warning("event=garbage_invalid_path_metadata")
                    continue

                # Refuse to delete anything outside the managed trash area,
                # even if metadata drifted or history was corrupted.
                if k.TRASH_FOLDER not in server_path:
                    log.error(
                        f"event=garbage_refused_outside_trash path={server_path}"
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
                    deleted_history_payloads += 1

            for file_path in resolved_rows:
                # `resolved_files` is an explicit quarantine for superseded
                # export artifacts, so filesystem age is the source of truth.
                if k.RESOLVED_FILES_TRASH_SUBDIR not in file_path:
                    log.error(
                        f"event=garbage_refused_outside_resolved_files path={file_path}"
                    )
                    continue

                if delete_file(file_path):
                    deleted += 1
                    deleted_resolved_files += 1

            db_bp.commit()

            log.event(
                "garbage_batch_processed",
                deleted=deleted,
                deleted_history_payloads=deleted_history_payloads,
                deleted_resolved_files=deleted_resolved_files,
            )

        except Exception as e:
            log.error(f"event=garbage_loop_error error={e}")

        time.sleep(k.GC_LOOP_SLEEP)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal garbage-collector worker crash",
            stage="MAIN",
            exc=e,
        )
        err.log_error()
        raise
