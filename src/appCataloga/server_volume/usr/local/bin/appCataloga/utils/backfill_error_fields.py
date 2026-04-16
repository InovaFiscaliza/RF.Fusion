#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backfill structured error columns for FILE_TASK and FILE_TASK_HISTORY.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import mysql.connector


SCRIPT_PATH = Path(__file__).resolve()

SERVER_VOLUME = None
for candidate in SCRIPT_PATH.parents:
    if candidate.name == "server_volume":
        SERVER_VOLUME = candidate
        break

if SERVER_VOLUME is None:
    raise RuntimeError("server_volume not found")

APP_ROOT = SERVER_VOLUME / "usr" / "local" / "bin" / "appCataloga"
ETC_ROOT = SERVER_VOLUME / "etc" / "appCataloga"

for candidate in (ETC_ROOT, APP_ROOT):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import config as k  # noqa: E402
from shared import errors  # noqa: E402


TABLES = {
    "file_task": ("FILE_TASK", "ID_FILE_TASK"),
    "file_task_history": ("FILE_TASK_HISTORY", "ID_HISTORY"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill structured error fields from legacy NA_MESSAGE payloads.",
    )
    parser.add_argument(
        "--table",
        choices=("file_task", "file_task_history", "all"),
        default="all",
        help="Target table to process.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for SELECT/UPDATE passes.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Classify rows without issuing UPDATE statements.",
    )
    return parser.parse_args()


def connect() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        user=k.DB_USER_NAME,
        password=k.DB_PASSWORD,
        host=k.SERVER_NAME,
        port=k.DB_PORT,
        database=k.BKP_DATABASE_NAME,
        autocommit=False,
    )


def fetch_batch(cursor, table: str, pk: str, last_id: int, batch_size: int):
    cursor.execute(
        f"""
        SELECT
            {pk},
            NA_MESSAGE,
            NA_ERROR_DOMAIN,
            NA_ERROR_STAGE,
            NA_ERROR_CODE,
            NA_ERROR_SUMMARY,
            NA_ERROR_DETAIL,
            NU_ERROR_CLASSIFIER_VERSION
        FROM {table}
        WHERE {pk} > %s
        ORDER BY {pk} ASC
        LIMIT %s
        """,
        (last_id, batch_size),
    )
    return cursor.fetchall()


def build_candidate_tuple(row):
    structured = errors.classify_persisted_error_message(row.get("NA_MESSAGE"))
    current = (
        row.get("NA_ERROR_DOMAIN"),
        row.get("NA_ERROR_STAGE"),
        row.get("NA_ERROR_CODE"),
        row.get("NA_ERROR_SUMMARY"),
        row.get("NA_ERROR_DETAIL"),
        row.get("NU_ERROR_CLASSIFIER_VERSION"),
    )
    candidate = (
        structured["NA_ERROR_DOMAIN"],
        structured["NA_ERROR_STAGE"],
        structured["NA_ERROR_CODE"],
        structured["NA_ERROR_SUMMARY"],
        structured["NA_ERROR_DETAIL"],
        structured["NU_ERROR_CLASSIFIER_VERSION"],
    )

    if current == candidate:
        return None

    return candidate


def backfill_table(
    connection: mysql.connector.MySQLConnection,
    *,
    table_key: str,
    batch_size: int,
    dry_run: bool,
) -> dict:
    table, pk = TABLES[table_key]
    read_cursor = connection.cursor(dictionary=True)
    write_cursor = connection.cursor()
    last_id = 0
    scanned = 0
    updated = 0

    while True:
        rows = fetch_batch(read_cursor, table, pk, last_id, batch_size)

        if not rows:
            break

        updates = []

        for row in rows:
            scanned += 1
            last_id = int(row[pk])
            candidate = build_candidate_tuple(row)

            if candidate is None:
                continue

            updates.append((*candidate, last_id))

        if updates and not dry_run:
            write_cursor.executemany(
                f"""
                UPDATE {table}
                SET
                    NA_ERROR_DOMAIN = %s,
                    NA_ERROR_STAGE = %s,
                    NA_ERROR_CODE = %s,
                    NA_ERROR_SUMMARY = %s,
                    NA_ERROR_DETAIL = %s,
                    NU_ERROR_CLASSIFIER_VERSION = %s
                WHERE {pk} = %s
                """,
                updates,
            )
            connection.commit()

        updated += len(updates)

    read_cursor.close()
    write_cursor.close()
    return {
        "table": table,
        "scanned": scanned,
        "updated": updated,
        "dry_run": dry_run,
    }


def main() -> int:
    args = parse_args()
    targets = (
        ["file_task", "file_task_history"]
        if args.table == "all"
        else [args.table]
    )

    connection = connect()

    try:
        for table_key in targets:
            summary = backfill_table(
                connection,
                table_key=table_key,
                batch_size=max(1, args.batch_size),
                dry_run=args.dry_run,
            )
            print(
                f"{summary['table']}: scanned={summary['scanned']} "
                f"updated={summary['updated']} dry_run={summary['dry_run']}"
            )
    finally:
        connection.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
