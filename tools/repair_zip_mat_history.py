"""
Repair script: close spurious FILE_TASK_HISTORY rows that were created by a
re-discovery of CelPlan .zip files whose VL_FILE_SIZE_KB / DT_FILE_CREATED had
been overwritten with the server-side .mat values.

The symptom: rows where
    NA_HOST_FILE_NAME LIKE '%.zip'
    NA_SERVER_FILE_NAME LIKE '%.mat'
    NU_STATUS_BACKUP = PENDING (1)          -- reset by UPSERT on re-discovery
    NU_STATUS_PROCESSING = PENDING (1)      -- same

These rows got a matching FILE_TASK created by the discovery worker because the
corrupted identity key (mat size / mat timestamp) no longer matched the original
zip key, so filter_existing_file_batch let them through.

Repair steps (atomic per row, inside a transaction):
    1. Mark FILE_TASK_HISTORY as fully done:
           NU_STATUS_BACKUP    = DONE (0)
           NU_STATUS_PROCESSING = DONE (0)
           NA_MESSAGE          = 'Repaired: zip already converted to mat'
    2. Delete the orphan FILE_TASK (backup type, same host + path + name)
       if one exists.
    3. Commit.

Run with --dry-run to preview without writing.

Usage:
    python repair_zip_mat_history.py [--dry-run] [--batch-size N]
"""

import argparse
import sys
import os

# Make sure appCataloga config is importable when run directly.
_CONFIG_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__),
    "..",
    "src",
    "appCataloga",
    "server_volume",
    "etc",
    "appCataloga",
))
if _CONFIG_DIR not in sys.path:
    sys.path.insert(0, _CONFIG_DIR)

import mysql.connector
import config as k


def get_connection():
    return mysql.connector.connect(
        host=k.SERVER_NAME,
        port=k.DB_PORT,
        user=k.DB_USER_NAME,
        password=k.DB_PASSWORD,
        database=k.BKP_DATABASE_NAME,
        autocommit=False,
    )


FIND_SQL = """
    SELECT
        h.ID_HISTORY,
        h.FK_HOST,
        h.NA_HOST_FILE_PATH,
        h.NA_HOST_FILE_NAME,
        h.NA_SERVER_FILE_PATH,
        h.NA_SERVER_FILE_NAME,
        h.VL_FILE_SIZE_KB,
        h.DT_FILE_CREATED,
        h.NU_STATUS_BACKUP,
        h.NU_STATUS_PROCESSING
    FROM FILE_TASK_HISTORY h
    WHERE h.NA_HOST_FILE_NAME LIKE '%.zip'
      AND h.NA_SERVER_FILE_NAME LIKE '%.mat'
      AND h.NU_STATUS_PROCESSING = 1
    ORDER BY h.ID_HISTORY
    LIMIT %s
"""

UPDATE_HISTORY_SQL = """
    UPDATE FILE_TASK_HISTORY
    SET
        NU_STATUS_BACKUP     = 0,
        NU_STATUS_PROCESSING = 0,
        NA_MESSAGE           = 'Repaired: zip re-discovery duplicate — mat artifact already existed'
    WHERE ID_HISTORY = %s
      AND NU_STATUS_PROCESSING = 1
"""

DELETE_FILE_TASK_SQL = """
    DELETE FROM FILE_TASK
    WHERE FK_HOST          = %s
      AND NA_HOST_FILE_PATH = %s
      AND NA_HOST_FILE_NAME = %s
      AND NU_STATUS IN (1, 2)   -- PENDING or RUNNING only; leave ERROR/DONE alone
"""

# Step 3: FILE_TASK backup rows SUSPENDED for zip files whose history is
# already fully DONE (both backup and processing). These were created before
# the fix and got suspended when the host went offline; the history was then
# repaired by steps 1/2 above.  On host recovery the worker would wrongly
# resume backup of an already-processed file, so we delete them.
FIND_SUSPENDED_SQL = """
    SELECT ft.ID_FILE_TASK, ft.FK_HOST, ft.NA_HOST_FILE_PATH, ft.NA_HOST_FILE_NAME
    FROM FILE_TASK ft
    JOIN FILE_TASK_HISTORY h
        ON h.FK_HOST           = ft.FK_HOST
       AND h.NA_HOST_FILE_PATH  = ft.NA_HOST_FILE_PATH
       AND h.NA_HOST_FILE_NAME  = ft.NA_HOST_FILE_NAME
    WHERE ft.NA_HOST_FILE_NAME  LIKE '%.zip'
      AND h.NA_SERVER_FILE_NAME LIKE '%.mat'
      AND ft.NU_STATUS          = -2
      AND h.NU_STATUS_BACKUP    = 0
      AND h.NU_STATUS_PROCESSING = 0
    ORDER BY ft.ID_FILE_TASK
    LIMIT %s
"""

DELETE_SUSPENDED_FILE_TASK_SQL = """
    DELETE FROM FILE_TASK
    WHERE ID_FILE_TASK = %s
      AND NU_STATUS    = -2
"""


def repair(dry_run: bool, batch_size: int) -> None:
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute(FIND_SQL, (batch_size,))
    rows = cursor.fetchall()
    # Close the implicit read-only transaction started by SELECT so that
    # the per-row explicit commit/rollback calls below work correctly.
    conn.commit()

    if not rows:
        print("No affected FILE_TASK_HISTORY rows found. Skipping steps 1/2.")

    print(f"Found {len(rows)} affected FILE_TASK_HISTORY row(s).")
    if dry_run:
        print("DRY-RUN — no changes will be written.\n")

    fixed_history  = 0
    fixed_tasks    = 0
    errors         = 0

    for row in rows:
        hid       = row["ID_HISTORY"]
        host_id   = row["FK_HOST"]
        host_path = row["NA_HOST_FILE_PATH"]
        host_name = row["NA_HOST_FILE_NAME"]
        srv_name  = row["NA_SERVER_FILE_NAME"]

        print(
            f"  ID_HISTORY={hid}  host={host_id}  "
            f"zip={host_name}  mat={srv_name}"
        )

        if dry_run:
            continue

        try:
            cursor.execute(UPDATE_HISTORY_SQL, (hid,))
            history_affected = cursor.rowcount

            cursor.execute(DELETE_FILE_TASK_SQL, (host_id, host_path, host_name))
            tasks_deleted = cursor.rowcount

            conn.commit()

            fixed_history += history_affected
            fixed_tasks   += tasks_deleted
            print(
                f"    → history updated={history_affected}, "
                f"FILE_TASK deleted={tasks_deleted}"
            )

        except Exception as exc:
            conn.rollback()
            errors += 1
            print(f"    ERROR for ID_HISTORY={hid}: {exc}", file=sys.stderr)

    # ------------------------------------------------------------------
    # Step 3: delete orphan SUSPENDED backup tasks for already-done zips
    # ------------------------------------------------------------------
    print("\n--- Step 3: orphan SUSPENDED FILE_TASK cleanup ---")
    cursor.execute(FIND_SUSPENDED_SQL, (batch_size,))
    suspended_rows = cursor.fetchall()
    conn.commit()

    if not suspended_rows:
        print("No orphan SUSPENDED FILE_TASK rows found.")
    else:
        print(f"Found {len(suspended_rows)} orphan SUSPENDED FILE_TASK row(s).")
        if dry_run:
            for r in suspended_rows:
                print(
                    f"  [DRY] Would delete FILE_TASK id={r['ID_FILE_TASK']} "
                    f"host={r['FK_HOST']}  {r['NA_HOST_FILE_NAME']}"
                )
        else:
            deleted_suspended = 0
            for r in suspended_rows:
                try:
                    cursor.execute(DELETE_SUSPENDED_FILE_TASK_SQL, (r["ID_FILE_TASK"],))
                    conn.commit()
                    deleted_suspended += cursor.rowcount
                except Exception as exc:
                    conn.rollback()
                    errors += 1
                    print(
                        f"  ERROR deleting FILE_TASK id={r['ID_FILE_TASK']}: {exc}",
                        file=sys.stderr,
                    )
            fixed_tasks += deleted_suspended
            print(f"  Deleted {deleted_suspended} SUSPENDED FILE_TASK row(s).")

    cursor.close()
    conn.close()

    if not dry_run:
        print(
            f"\nDone. History rows fixed: {fixed_history}, "
            f"FILE_TASKs deleted: {fixed_tasks}, errors: {errors}."
        )
        cap_warning = (len(rows) == batch_size or
                       len(suspended_rows) == batch_size)
        if cap_warning:
            print(
                f"WARNING: one or more result sets were capped at "
                f"batch_size={batch_size}. Re-run to process remaining rows."
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview affected rows without writing any changes.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        metavar="N",
        help="Max rows to process in one run (default: 10000).",
    )
    args = parser.parse_args()
    repair(dry_run=args.dry_run, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
