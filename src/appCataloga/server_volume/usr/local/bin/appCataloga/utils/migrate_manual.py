#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import os
import re
import sys
import hashlib
import time
from pathlib import Path

# =================================================
# Resolve paths robustly
# =================================================

SCRIPT_PATH = Path(__file__).resolve()

SERVER_VOLUME = None
for p in SCRIPT_PATH.parents:
    if p.name == "server_volume":
        SERVER_VOLUME = p
        break

if SERVER_VOLUME is None:
    raise RuntimeError(
        f"server_volume directory not found from {SCRIPT_PATH}"
    )

APP_ROOT = SERVER_VOLUME / "usr" / "local" / "bin" / "appCataloga"
DB_ROOT = APP_ROOT / "db"
SHARED_ROOT = APP_ROOT / "shared"
ETC_ROOT = SERVER_VOLUME / "etc" / "appCataloga"

def safe_add_path(path: Path):
    path_str = str(path)
    if path.exists() and path_str not in sys.path:
        sys.path.insert(0, path_str)

safe_add_path(ETC_ROOT)
safe_add_path(APP_ROOT)
safe_add_path(DB_ROOT)
safe_add_path(SHARED_ROOT)
safe_add_path(SCRIPT_PATH.parent)

try:
    import config as k
except ImportError as e:
    raise RuntimeError(
        f"Failed to import config. sys.path={sys.path}"
    ) from e

from shared import logging_utils
from shared.file_metadata import FileMetadata
from db.dbHandlerBKP import dbHandlerBKP

# =================================================
# Configuração
# =================================================

MANUAL_ROOT = "/mnt/reposfi/Manual/2019"
DATABASE_NAME = k.BKP_DATABASE_NAME

BATCH_SIZE = 6000
PRINT_EVERY = 50000

log = logging_utils.log("manual_import_turbo")

HOST_REGEX = re.compile(r"^rfeye\d{6}$", re.IGNORECASE)
SERVER_HASH_REGEX = re.compile(r"^p-([a-f0-9]{8})--(.+)$")

# =================================================
# Helpers
# =================================================

def extract_host_from_path(full_path):
    parts = Path(full_path).parts
    for part in reversed(parts):
        if HOST_REGEX.match(part):
            return part.lower()
    return None

HOST_FROM_FILENAME_REGEX = re.compile(r"(rfeye\d+)", re.IGNORECASE)

def extract_host(full_path):
    """
    Extract host from:
    1) Directory structure
    2) Filename (fallback)
    """

    # -------- First attempt: directory structure --------
    parts = Path(full_path).parts
    for part in reversed(parts):
        if HOST_REGEX.match(part):
            return part.lower()

    # -------- Second attempt: filename --------
    filename = os.path.basename(full_path)

    # Remove hash prefix if present
    _, clean_name = strip_hash_if_present(filename)

    match = HOST_FROM_FILENAME_REGEX.search(clean_name)
    if match:
        return match.group(1).lower()

    return None

def strip_hash_if_present(filename):
    m = SERVER_HASH_REGEX.match(filename)
    if not m:
        return None, filename
    return m.group(1), m.group(2)

def build_hash(host_uid, full_path):
    return hashlib.sha1(
        f"{host_uid}:{full_path}".encode()
    ).hexdigest()[:8]

def ensure_server_filename(full_path, host_uid, original_name):
    directory = os.path.dirname(full_path)
    filename = os.path.basename(full_path)

    hash_part, clean_name = strip_hash_if_present(filename)

    if hash_part:
        return full_path, filename

    new_hash = build_hash(host_uid, full_path)
    new_name = f"p-{new_hash}--{original_name}"
    new_full_path = os.path.join(directory, new_name)

    os.rename(full_path, new_full_path)

    return new_full_path, new_name

def build_file_metadata(full_path, original_name, legacy_host_path):
    stat = os.stat(full_path)
    dt = datetime.fromtimestamp(stat.st_mtime)

    return FileMetadata(
        NA_FULL_PATH=full_path,
        NA_PATH=legacy_host_path,
        NA_FILE=original_name,
        NA_EXTENSION=os.path.splitext(original_name)[1].lower(),
        VL_FILE_SIZE_KB=round(stat.st_size / 1024),
        DT_FILE_CREATED=dt,
        DT_FILE_MODIFIED=dt,
        DT_FILE_ACCESSED=dt,
        NA_OWNER="legacy",
        NA_GROUP="legacy",
        NA_PERMISSIONS="000",
    )

# =================================================
# Batch Insert
# =================================================

def insert_batch(db, table, rows):
    if not rows:
        return 0

    db._connect()
    processed = 0

    try:
        cursor = db.db_connection.cursor()

        columns = list(rows[0].keys())
        cols_sql = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        sql = f"""
            INSERT INTO {table} ({cols_sql})
            VALUES ({placeholders})
        """

        batch = []

        for row in rows:
            batch.append(tuple(row[col] for col in columns))

            if len(batch) >= BATCH_SIZE:
                cursor.executemany(sql, batch)
                processed += len(batch)
                batch.clear()

        if batch:
            cursor.executemany(sql, batch)
            processed += len(batch)

        db.db_connection.commit()
        return processed

    except Exception:
        db.db_connection.rollback()
        raise

    finally:
        db._disconnect()

# =================================================
# MAIN
# =================================================

def main():

    log.entry("=== TURBO START ===")
    start_time = time.time()

    db = dbHandlerBKP(database=DATABASE_NAME, log=log)
    db._connect()

    # ---------------- HOST CACHE ----------------
    host_rows = db._select_rows(
        table="HOST",
        cols=["ID_HOST", "NA_HOST_NAME"]
    )

    host_map = {
        r["NA_HOST_NAME"].lower(): r["ID_HOST"]
        for r in host_rows
    }

    # ---------------- HISTORY CACHE ----------------
    history_rows = db._select_rows(
        table="FILE_TASK_HISTORY",
        cols=[
            "FK_HOST",
            "NA_HOST_FILE_PATH",
            "NA_HOST_FILE_NAME",
            "VL_FILE_SIZE_KB"
        ]
    )

    history_identity = {
        (
            r["FK_HOST"],
            r["NA_HOST_FILE_PATH"],
            r["NA_HOST_FILE_NAME"],
            r["VL_FILE_SIZE_KB"]
        )
        for r in history_rows
    }

    db._disconnect()

    task_batch = []
    history_batch = []

    processed_files = 0
    inserted_files = 0

    now = datetime.now()

    for root, _, files in os.walk(MANUAL_ROOT):

        for fname in files:

            if not fname.lower().endswith(".bin"):
                continue

            processed_files += 1

            if processed_files % PRINT_EVERY == 0:
                elapsed = time.time() - start_time
                rate = int(processed_files / elapsed) if elapsed > 0 else 0

                print(
                    f"[PROGRESS] analisados={processed_files} "
                    f"| inseridos={inserted_files} "
                    f"| taxa≈{rate}/s"
                )

            full_path = os.path.join(root, fname)
            hash_part, original_name = strip_hash_if_present(fname)

            host_uid = extract_host(full_path)
            if not host_uid:
                continue

            host_id = host_map.get(host_uid)
            if not host_id:
                continue

            full_path, server_name = ensure_server_filename(
                full_path, host_uid, original_name
            )

            # Calculate hash for legacy path
            legacy_hash = build_hash(host_uid, full_path)
            legacy_host_path = f"/p-{legacy_hash}--LEGACY_IMPORT"
            
            # Build metadata
            file_meta = build_file_metadata(
                full_path,
                original_name,
                legacy_host_path
            )

            identity = (
                host_id,
                legacy_host_path,
                original_name,
                file_meta.VL_FILE_SIZE_KB
            )

            if identity in history_identity:
                continue

            history_identity.add(identity)

            task_batch.append({
                "FK_HOST": host_id,
                "DT_FILE_TASK": now,
                "NU_TYPE": k.FILE_TASK_PROCESS_TYPE,
                "NA_HOST_FILE_PATH": legacy_host_path,
                "NA_HOST_FILE_NAME": original_name,
                "NA_SERVER_FILE_PATH": root,
                "NA_SERVER_FILE_NAME": server_name,
                "NU_STATUS": k.TASK_PENDING,
                "NA_EXTENSION": file_meta.NA_EXTENSION,
                "VL_FILE_SIZE_KB": file_meta.VL_FILE_SIZE_KB,
                "DT_FILE_CREATED": file_meta.DT_FILE_CREATED,
                "DT_FILE_MODIFIED": file_meta.DT_FILE_MODIFIED,
                "NA_MESSAGE": "Legacy turbo import",
            })

            history_batch.append({
                "FK_HOST": host_id,
                "DT_DISCOVERED": now,
                "DT_BACKUP": now,
                "NU_STATUS_DISCOVERY": k.TASK_DONE,
                "NU_STATUS_BACKUP": k.TASK_DONE,
                "NA_HOST_FILE_PATH": legacy_host_path,
                "NA_HOST_FILE_NAME": original_name,
                "NA_SERVER_FILE_PATH": root,
                "NA_SERVER_FILE_NAME": server_name,
                "VL_FILE_SIZE_KB": file_meta.VL_FILE_SIZE_KB,
                "DT_FILE_CREATED": file_meta.DT_FILE_CREATED,
                "DT_FILE_MODIFIED": file_meta.DT_FILE_MODIFIED,
                "NA_EXTENSION": file_meta.NA_EXTENSION,
                "NA_MESSAGE": "Legacy turbo import",
            })

            inserted_files += 1

            if len(task_batch) >= BATCH_SIZE:
                insert_batch(db, "FILE_TASK", task_batch)
                insert_batch(db, "FILE_TASK_HISTORY", history_batch)
                task_batch.clear()
                history_batch.clear()

    if task_batch:
        insert_batch(db, "FILE_TASK", task_batch)
        insert_batch(db, "FILE_TASK_HISTORY", history_batch)

    log.entry(f"=== FINISHED: {inserted_files} FILES INSERTED ===")


if __name__ == "__main__":
    main()