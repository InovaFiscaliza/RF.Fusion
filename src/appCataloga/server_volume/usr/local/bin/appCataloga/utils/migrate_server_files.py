#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Conservative server-side migration / recovery utility.

Rules:
• No heuristics
• No destructive action without deterministic identity
• Silent ignore on ambiguous filenames
• Uses FILE_TASK as the single source of truth for uniqueness
• Compatible with RF.Fusion naming contract
"""

import os
import sys
import shutil
import hashlib
import re
from datetime import datetime
from pathlib import Path
import signal

ROOT = Path(__file__).resolve().parents[1]
sys.pathinsert = sys.path.insert(0, str(ROOT))

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
# Filename helpers (CONTRACT)
# ======================================================================

SERVER_FILE_REGEX = re.compile(r"^p-([a-f0-9]{8})--(.+)$")


def parse_server_filename(filename: str):
    """
    Parse server-side filename.

    Returns:
        (hash | None, original_name)
    """
    m = SERVER_FILE_REGEX.match(filename)
    if not m:
        return None, filename
    return m.group(1), m.group(2)


def build_server_filename(host_uid: str, remote_path: str, filename: str) -> str:
    """
    Build deterministic server filename.

    Contract:
        p-<hash>--<original_filename>.bin
    """
    h = hashlib.sha1(
        f"{host_uid}:{remote_path}".encode("utf-8")
    ).hexdigest()[:8]
    return f"p-{h}--{filename}"


# ======================================================================
# Signal handling
# ======================================================================

def _handle_sigterm(sig, frame):
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
            continue

        for root, dirs, files in os.walk(base_root):

            # Never touch TMP
            if root.startswith(TMP_ROOT):
                dirs[:] = []
                continue

            for fname in files:

                if not fname.lower().endswith(".bin"):
                    continue

                full_path = os.path.join(root, fname)

                # -------------------------------------------------
                # Normalize filename (with or without hash)
                # -------------------------------------------------
                _, original_name = parse_server_filename(fname)

                # -------------------------------------------------
                # RULE 1 — SILENT HARD IGNORE ON AMBIGUITY
                # -------------------------------------------------
                rows = db.check_file_task(
                    NA_HOST_FILE_NAME=original_name,
                    NU_TYPE=k.FILE_TASK_DISCOVERY,
                    NU_STATUS=k.TASK_DONE,
                )

                if not rows or len(rows) != 1:
                    continue

                task = rows[0]
                host_id = task["FK_HOST"]

                # -------------------------------------------------
                # Load host
                # -------------------------------------------------
                host = db.host_read_access(host_id)
                if not host:
                    continue

                # -------------------------------------------------
                # Already migrated?
                # -------------------------------------------------
                if task.get("NA_SERVER_FILE_PATH"):
                    continue

                # -------------------------------------------------
                # Build correct server filename
                # -------------------------------------------------
                new_server_name = build_server_filename(
                    host_uid=host["host_uid"],
                    remote_path=task["NA_HOST_FILE_PATH"],
                    filename=original_name,
                )

                local_path = os.path.join(
                    k.REPO_FOLDER,
                    k.TMP_FOLDER,
                    host["host_uid"],
                )
                os.makedirs(local_path, exist_ok=True)

                dest_file = os.path.join(local_path, new_server_name)

                # -------------------------------------------------
                # Move & rename file
                # -------------------------------------------------
                try:
                    shutil.move(full_path, dest_file)
                except Exception:
                    continue

                # -------------------------------------------------
                # Update FILE_TASK
                # -------------------------------------------------
                try:
                    db.file_task_update(
                        task_id=task["ID_FILE_TASK"],
                        NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
                        NU_STATUS=k.TASK_PENDING,
                        NA_SERVER_FILE_PATH=local_path,
                        NA_SERVER_FILE_NAME=new_server_name,
                        NA_MESSAGE=sh._compose_message(
                            task_type=k.FILE_TASK_PROCESS_TYPE,
                            task_status=k.TASK_PENDING,
                            path=task["NA_HOST_FILE_PATH"],
                            name=original_name,
                        ),
                    )
                except Exception:
                    pass

                # -------------------------------------------------
                # Update FILE_TASK_HISTORY (if exists)
                # -------------------------------------------------
                try:
                    db.file_history_update(
                        task_type=k.FILE_TASK_BACKUP_TYPE,
                        host_file_name=original_name,
                        host_id=host_id,
                        NA_SERVER_FILE_PATH=local_path,
                        NA_SERVER_FILE_NAME=new_server_name,
                        DT_BACKUP=datetime.now(),
                        NU_STATUS_BACKUP=k.TASK_DONE,
                        NA_MESSAGE=sh._compose_message(
                            task_type=k.FILE_TASK_BACKUP_TYPE,
                            task_status=k.TASK_DONE,
                            path=task["NA_HOST_FILE_PATH"],
                            name=original_name,
                        ),
                    )
                except Exception:
                    pass

    print("=== MIGRATION COMPLETE ===")


if __name__ == "__main__":
    migrate()
