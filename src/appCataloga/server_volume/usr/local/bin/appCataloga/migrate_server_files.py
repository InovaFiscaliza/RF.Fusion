#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import shutil
from datetime import datetime
import signal

import shared as sh
from db.dbHandlerBKP import dbHandlerBKP
import config as k

# ======================================================================
# Directories
# ======================================================================

ALLOWED_ROOTS = [
    "/mnt/reposfi/RF.Fusion_Processado",
    "/mnt/reposfi/trash",
]

TMP_ROOT = os.path.join(k.REPO_FOLDER, k.TMP_FOLDER)

# ======================================================================
# Signal handling
# ======================================================================

def _handle_sigterm(sig, frame) -> None:
    """Handle SIGTERM/SIGINT to stop the main loop gracefully."""
    sys.exit(0)

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


# ======================================================================
# Migration logic
# ======================================================================

def migrate():
    log = sh.log("migrate")

    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Failed to initialize database: {e}")
        sys.exit(1)

    log.entry("=== MIGRATION STARTED ===")

    for base_root in ALLOWED_ROOTS:

        if not os.path.isdir(base_root):
            log.warning(f"[SKIP] Directory does not exist → {base_root}")
            continue

        log.entry(f"[SCAN] Walking directory → {base_root}")

        for root, dirs, files in os.walk(base_root):

            # ---------------------------------------------------------
            # Never touch tmp
            # ---------------------------------------------------------
            if root.startswith(TMP_ROOT):
                dirs[:] = []
                continue

            for fname in files:

                if not fname.lower().endswith(".bin"):
                    continue

                full_path = os.path.join(root, fname)

                # -----------------------------------------------------
                # 1) DISCOVERY DONE must exist
                # -----------------------------------------------------
                discovery = db.check_file_task(
                    NA_HOST_FILE_NAME=fname,
                    NU_TYPE=k.FILE_TASK_DISCOVERY,
                    NU_STATUS=k.TASK_DONE,
                )

                if not discovery:
                    log.entry(f"[IGNORE] No valid DISCOVERY → {fname}")
                    continue

                task = discovery[0]
                host_id = task["FK_HOST"]

                # -----------------------------------------------------
                # 2) Load host
                # -----------------------------------------------------
                host = db.host_read_access(host_id)
                if not host:
                    log.warning(f"[WARN] HOST not found → {host_id}")
                    continue

                # -----------------------------------------------------
                # 3) Already migrated?
                # -----------------------------------------------------
                if task.get("NA_SERVER_FILE_PATH"):
                    log.entry(f"[SKIP] Already processed → {fname}")
                    continue

                # -----------------------------------------------------
                # 4) Load HISTORY (optional)
                # -----------------------------------------------------
                history = db.check_file_history(
                    FK_HOST=host_id,
                    NA_HOST_FILE_NAME=fname,
                )

                # -----------------------------------------------------
                # 5) Prepare destination folder
                # -----------------------------------------------------
                local_path = os.path.join(
                    k.REPO_FOLDER,
                    k.TMP_FOLDER,
                    host["host_uid"],
                )

                try:
                    os.makedirs(local_path, exist_ok=True)
                except Exception as e:
                    log.error(f"[FAIL] Creating folder {local_path}: {e}")
                    continue

                dest_file = os.path.join(local_path, fname)

                # -----------------------------------------------------
                # 6) Move file
                # -----------------------------------------------------
                try:
                    shutil.move(full_path, dest_file)
                except Exception as e:
                    log.error(f"[FAIL MOVING] {fname}: {e}")
                    continue

                log.entry(f"[MOVE] {full_path} → {dest_file} (HOST {host_id})")

                # -----------------------------------------------------
                # 7) Update FILE_TASK (DISCOVERY → BACKUP DONE)
                # -----------------------------------------------------
                try:
                    db.file_task_update(
                        task_id=task["ID_FILE_TASK"],
                        NU_TYPE=k.FILE_TASK_BACKUP_TYPE,
                        NU_STATUS=k.TASK_DONE,
                        NA_SERVER_FILE_PATH=local_path,
                        NA_SERVER_FILE_NAME=fname,
                        NA_MESSAGE=sh._compose_message(
                            task_type=k.FILE_TASK_BACKUP_TYPE,
                            task_status=k.TASK_DONE,
                            path=task["NA_HOST_FILE_PATH"],
                            name=task["NA_HOST_FILE_NAME"],
                        ),
                    )
                except Exception as e:
                    log.error(
                        f"[FAIL] Updating FILE_TASK {task['ID_FILE_TASK']}: {e}"
                    )

                # -----------------------------------------------------
                # 8) Update FILE_TASK_HISTORY (if exists)
                # -----------------------------------------------------
                if history:
                    try:
                        db.file_history_update(
                            task_type=k.FILE_TASK_BACKUP_TYPE,
                            file_name=fname,
                            host_id=host_id,
                            NA_SERVER_FILE_PATH=local_path,
                            NA_SERVER_FILE_NAME=fname,
                            DT_BACKUP=datetime.now(),
                            NU_STATUS_BACKUP=k.TASK_DONE,
                            NA_MESSAGE=sh._compose_message(
                                task_type=k.FILE_TASK_BACKUP_TYPE,
                                task_status=k.TASK_DONE,
                                path=task["NA_HOST_FILE_PATH"],
                                name=task["NA_HOST_FILE_NAME"],
                            ),
                        )
                    except Exception as e:
                        log.error(
                            f"[FAIL] Updating FILE_TASK_HISTORY for {fname}: {e}"
                        )

    log.entry("=== MIGRATION COMPLETE ===")


if __name__ == "__main__":
    migrate()
