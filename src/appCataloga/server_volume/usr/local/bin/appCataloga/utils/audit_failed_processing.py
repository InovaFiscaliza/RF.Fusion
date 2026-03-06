#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Audit script for failed PROCESS records.

Purpose:
    - Compare DB file size vs real filesystem size
    - Detect meaningful inconsistencies
    - Export audit results to CSV
    - Prepare dataset for safe FILE_TASK reconstruction if needed

Safe:
    - Read-only
    - No DB writes
"""

import os
import sys
import csv
from pathlib import Path
from datetime import datetime

# =================================================
# Configuration
# =================================================

SIZE_THRESHOLD_KB = 10.0  # differences below this are considered rounding noise

# =================================================
# Resolve paths (same logic used in other utils)
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
# Imports after path resolution
# =================================================

import config as k
from shared import logging_utils
from db.dbHandlerBKP import dbHandlerBKP

log = logging_utils.log("audit_failed_processing")

OUTPUT_FILE = (
    f"audit_failed_processing_"
    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
)

# =================================================
# Main
# =================================================

def main():

    db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    db._connect()

    log.entry("Loading failed PROCESS records from DB...")

    db.cursor.execute("""
        SELECT 
            ID_HISTORY,
            FK_HOST,
            NA_HOST_FILE_PATH,
            NA_HOST_FILE_NAME,
            NA_SERVER_FILE_PATH,
            NA_SERVER_FILE_NAME,
            VL_FILE_SIZE_KB,
            NA_MESSAGE,
            DT_FILE_CREATED
        FROM FILE_TASK_HISTORY
        WHERE NU_STATUS_PROCESSING = -1
        AND NA_SERVER_FILE_PATH IS NOT NULL
        AND NA_SERVER_FILE_NAME IS NOT NULL
    """)

    rows_raw = db.cursor.fetchall()

    if not rows_raw:
        log.warning("No failed PROCESS records found.")
        return

    # Convert tuple rows into dictionaries
    columns = [col[0] for col in db.cursor.description]
    rows = [dict(zip(columns, r)) for r in rows_raw]

    total = len(rows)
    log.entry(f"Loaded {total} failed records.")

    audit_rows = []

    for idx, row in enumerate(rows, start=1):

        server_path = row["NA_SERVER_FILE_PATH"]
        server_name = row["NA_SERVER_FILE_NAME"]

        full_path = os.path.join(server_path, server_name)

        db_size_kb = float(row["VL_FILE_SIZE_KB"] or 0)
        db_size_bytes = int(db_size_kb * 1024)

        if os.path.exists(full_path):
            real_size_bytes = os.path.getsize(full_path)
            real_size_kb = real_size_bytes / 1024
            exists = True
        else:
            real_size_bytes = 0
            real_size_kb = 0
            exists = False

        delta_bytes = real_size_bytes - db_size_bytes
        delta_kb = real_size_kb - db_size_kb

        significant = abs(delta_kb) > SIZE_THRESHOLD_KB

        if significant:
            audit_rows.append({
                "ID_HISTORY": row["ID_HISTORY"],
                "FK_HOST": row["FK_HOST"],
                "NA_HOST_FILE_PATH": row["NA_HOST_FILE_PATH"],
                "NA_HOST_FILE_NAME": row["NA_HOST_FILE_NAME"],
                "DT_FILE_CREATED": row["DT_FILE_CREATED"],
                "FILE_PATH": full_path,
                "DB_SIZE_KB": round(db_size_kb, 2),
                "REAL_SIZE_KB": round(real_size_kb, 2),
                "REAL_SIZE_BYTES": real_size_bytes,
                "DELTA_KB": round(delta_kb, 4),
                "FILE_EXISTS": exists,
                "NA_MESSAGE": row["NA_MESSAGE"],
            })

        if idx % 1000 == 0:
            log.entry(f"Processed {idx}/{total}")

    log.entry("Exporting CSV...")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = list(audit_rows[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(audit_rows)

    log.entry(f"Audit complete. CSV generated: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()