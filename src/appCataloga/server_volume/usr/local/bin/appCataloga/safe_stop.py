#!/usr/bin/python3
"""
Force cleanup of BUSY hosts and reset RUNNING FILE_TASKs and HOST_TASKs to PENDING.

This script must be executed only after stopping all appCataloga workers.
"""

from datetime import datetime
import sys, os
from bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

# Import customized libs
from db.dbHandlerBKP import dbHandlerBKP
from shared import  logging_utils
import config as k

log = logging_utils.log()

def cleanup_repository_tmp_files(*, repo_tmp_root: str) -> int:
    """
    Delete stale backup temporary files left in the repository TMP area.

    Why this belongs here:
        `appCataloga_file_bkp.py` writes into `<repo>/tmp/<host_uid>/*.tmp`
        while a transfer is still in progress. Those files are not retired
        artifacts owned by the garbage collector; they are interrupted-transfer
        leftovers. `safe_stop.py` runs only after all workers are stopped, so it
        is the safest place to purge them without racing an active download.
    """
    if not os.path.isdir(repo_tmp_root):
        log.entry(f"[CLEANUP] Repository TMP root not found: {repo_tmp_root}")
        return 0

    deleted = 0

    for root, _, files in os.walk(repo_tmp_root):
        for name in files:
            if not name.endswith(".tmp"):
                continue

            path = os.path.join(root, name)

            try:
                os.remove(path)
                deleted += 1
                log.warning(f"[CLEANUP] Deleted stale TMP file: {path}")
            except FileNotFoundError:
                continue
            except Exception as e:
                log.error(f"[CLEANUP] Failed to delete TMP file {path}: {e}")

    log.entry(
        f"[CLEANUP] Deleted {deleted} stale repository TMP file(s) "
        f"from {repo_tmp_root}"
    )
    return deleted


def prune_empty_repository_tmp_dirs(*, repo_tmp_root: str) -> int:
    """
    Remove empty host directories left behind after TMP-file cleanup.

    The repository TMP root itself is preserved. Only empty descendants are
    pruned so the next backup cycle starts from a tidy layout.
    """
    if not os.path.isdir(repo_tmp_root):
        return 0

    removed = 0

    for root, _, _ in os.walk(repo_tmp_root, topdown=False):
        if os.path.normpath(root) == os.path.normpath(repo_tmp_root):
            continue

        try:
            if not os.listdir(root):
                os.rmdir(root)
                removed += 1
                log.warning(f"[CLEANUP] Removed empty TMP directory: {root}")
        except FileNotFoundError:
            continue
        except OSError:
            continue
        except Exception as e:
            log.error(f"[CLEANUP] Failed to prune TMP directory {root}: {e}")

    if removed:
        log.entry(
            f"[CLEANUP] Removed {removed} empty repository TMP directorie(s)"
        )

    return removed


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

        # ---------------------------------------------------------
        # 4) Purge stale repository TMP leftovers from backup
        # ---------------------------------------------------------
        repo_tmp_root = os.path.join(k.REPO_FOLDER, k.TMP_FOLDER)
        cleanup_repository_tmp_files(repo_tmp_root=repo_tmp_root)
        prune_empty_repository_tmp_dirs(repo_tmp_root=repo_tmp_root)

    except Exception as e:
        log.error(f"[CLEANUP] Failed during forced cleanup: {e}")

    finally:
        if db:
            db._disconnect()

    log.entry("[CLEANUP] Forced cleanup finished")


if __name__ == "__main__":
    cleanup_hosts_and_tasks()
