#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Requeue backup tasks from audit CSV.

Safe:
    - Does NOT modify FILE_TASK_HISTORY
    - Reuses existing FILE_TASK when present
    - Creates new FILE_TASK only if missing
    - Preserves DT_FILE_CREATED from CSV
"""

import os
import sys
import csv
from pathlib import Path
from datetime import datetime

# =================================================
# Resolve paths
# =================================================

SCRIPT_PATH = Path(__file__).resolve()

SERVER_VOLUME = None
for p in SCRIPT_PATH.parents:
    if p.name == "server_volume":
        SERVER_VOLUME = p
        break

if SERVER_VOLUME is None:
    raise RuntimeError("server_volume not found")

APP_ROOT = SERVER_VOLUME / "usr" / "local" / "bin" / "appCataloga"
DB_ROOT = APP_ROOT / "db"
SHARED_ROOT = APP_ROOT / "shared"
ETC_ROOT = SERVER_VOLUME / "etc" / "appCataloga"

def safe_add_path(path: Path):
    if path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))

safe_add_path(ETC_ROOT)
safe_add_path(APP_ROOT)
safe_add_path(DB_ROOT)
safe_add_path(SHARED_ROOT)

# =================================================
# Imports
# =================================================

import config as k
from shared import logging_utils
from db.dbHandlerBKP import dbHandlerBKP

log = logging_utils.log("requeue_from_csv")

# =================================================
# Config
# =================================================

CSV_PATH = "audit_failed_processing_20260302_203905.csv"
DRY_RUN = False

# =================================================
# Helpers
# =================================================

def parse_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except Exception:
            log.warning(f"Failed to parse datetime: {value}")
            return None

# =================================================
# Main
# =================================================

def main():

    if not os.path.exists(CSV_PATH):
        raise RuntimeError(f"CSV not found: {CSV_PATH}")

    db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    db._connect()

    updated = 0
    created = 0

    with open(CSV_PATH, newline="", encoding="utf-8") as csvfile:

        reader = csv.DictReader(csvfile)

        for row in reader:

            id_history = int(row["ID_HISTORY"])
            dt_created = parse_datetime(row.get("DT_FILE_CREATED"))

            # -------------------------------------------------
            # Recover original host file identity
            # -------------------------------------------------

            db.cursor.execute("""
                SELECT 
                    FK_HOST,
                    NA_HOST_FILE_PATH,
                    NA_HOST_FILE_NAME,
                    VL_FILE_SIZE_KB
                FROM FILE_TASK_HISTORY
                WHERE ID_HISTORY = %s
            """, (id_history,))

            history = db.cursor.fetchone()

            if not history:
                log.warning(f"History not found: ID_HISTORY={id_history}")
                continue

            fk_host, host_path, host_name, size_kb = history

            # -------------------------------------------------
            # Check if FILE_TASK already exists (ANY status/type)
            # -------------------------------------------------

            db.cursor.execute("""
                SELECT ID_FILE_TASK
                FROM FILE_TASK
                WHERE FK_HOST = %s
                AND NA_HOST_FILE_PATH = %s
                AND NA_HOST_FILE_NAME = %s
                LIMIT 1
            """, (fk_host, host_path, host_name))

            existing = db.cursor.fetchone()

            extension = os.path.splitext(host_name)[1]

            if existing:

                file_task_id = existing[0]
                log.entry(f"Resetting existing FILE_TASK: {host_name}")

                if not DRY_RUN:
                    db.cursor.execute("""
                        UPDATE FILE_TASK
                        SET
                            NU_TYPE = %s,
                            NU_STATUS = %s,
                            NU_PID = NULL,
                            NA_SERVER_FILE_PATH = NULL,
                            NA_SERVER_FILE_NAME = NULL,
                            NA_SERVER_FILE_MD5 = NULL,
                            NA_EXTENSION = %s,
                            VL_FILE_SIZE_KB = %s,
                            DT_FILE_TASK = %s,
                            DT_FILE_CREATED = %s,
                            DT_FILE_MODIFIED = %s,
                            NA_MESSAGE = %s
                        WHERE ID_FILE_TASK = %s
                    """, (
                        k.FILE_TASK_BACKUP_TYPE,
                        k.TASK_PENDING,
                        extension,
                        size_kb,
                        datetime.now(),
                        dt_created,
                        dt_created,
                        f"Recovery Backup requeued (ID_HISTORY={id_history})",
                        file_task_id
                    ))

                    db.db_connection.commit()

                updated += 1

            else:

                log.entry(f"Creating new FILE_TASK: {host_name}")

                if not DRY_RUN:
                    db.cursor.execute("""
                        INSERT INTO FILE_TASK (
                            FK_HOST,
                            DT_FILE_TASK,
                            NU_TYPE,
                            NA_HOST_FILE_PATH,
                            NA_HOST_FILE_NAME,
                            NU_HOST_FILE_MD5,
                            NA_SERVER_FILE_PATH,
                            NA_SERVER_FILE_NAME,
                            NA_SERVER_FILE_MD5,
                            NU_STATUS,
                            NU_PID,
                            NA_EXTENSION,
                            VL_FILE_SIZE_KB,
                            DT_FILE_CREATED,
                            DT_FILE_MODIFIED,
                            NA_MESSAGE
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        fk_host,
                        datetime.now(),
                        k.FILE_TASK_BACKUP_TYPE,
                        host_path,
                        host_name,
                        None,
                        None,
                        None,
                        None,
                        k.TASK_PENDING,
                        None,
                        extension,
                        size_kb,
                        dt_created,
                        dt_created,
                        f"Recovery Backup queued (ID_HISTORY={id_history})"
                    ))

                    db.db_connection.commit()

                created += 1

    log.entry(f"Finished. Updated={updated} | Created={created}")


if __name__ == "__main__":
    main()