#!/usr/bin/python3
"""
Metadata publication worker for appCataloga.

This service exports the latest processed metadata to Parquet when the database
has changed since the last published snapshot. The loop stays intentionally
small because it is operational glue, not a data-processing pipeline.
"""

import inspect
import os
import random
import signal
import sys
import time


# =================================================
# PROJECT ROOT (shared/, db/, stations/)
# =================================================
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


# Import appCataloga modules
import config as k
from db.dbHandlerRFM import dbHandlerRFM
from shared import errors, logging_utils


# ============================================================
# Globals
# ============================================================
log = logging_utils.log()
process_status = {"running": True}


# ============================================================
# Signal handling
# ============================================================
def _signal_handler(signal_name: str) -> None:
    """
    Register shutdown intent for the publication loop.
    """
    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"event=signal_received signal={signal_name} handler={current_function}")
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


# Register the signal handler function, to handle system kill commands
signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


def ensure_parent_directory(file_name: str) -> None:
    """
    Ensure the parent directory of the publication file exists.
    """

    path = os.path.dirname(file_name)
    if not os.path.exists(path):
        os.makedirs(path)


def get_latest_parquet_time(path: str) -> float:
    """
    Return the most recent mtime among Parquet files under `path`.
    """

    latest_export = 0.0
    try:
        for file in os.listdir(path):
            if file.endswith(".parquet"):
                file_path = os.path.join(path, file)
                file_time = os.path.getmtime(file_path)
                if file_time > latest_export:
                    latest_export = file_time
    except Exception as e:
        log.error(f"event=parquet_scan_failed path={path} error={e}")
        pass

    return latest_export


def wait_random_time(message: str) -> int:
    """
    Sleep for a bounded random interval to desynchronize concurrent publishers.
    """

    wait_time = int(
        (k.MAX_HOST_TASK_WAIT_TIME + k.MAX_HOST_TASK_WAIT_TIME * random.random()) / 2
    )

    log.entry(f"event=publish_wait seconds={wait_time} detail=\"{message}\"")
    time.sleep(wait_time)


def main():
    """
    Run the publication polling loop until shutdown is requested.
    """

    log.entry("event=service_start service=appCataloga_pub_metadata")
    err = errors.ErrorHandler(log)

    try:
        rfdb = dbHandlerRFM(database=k.RFM_DATABASE_NAME, log=log)
    except Exception as e:
        err.capture(
            reason="Database initialization failed",
            stage="INIT",
            exc=e,
            service="appCataloga_pub_metadata",
        )
        err.log_error(service="appCataloga_pub_metadata")
        exit(1)

    ensure_parent_directory(file_name=k.PUBLISH_FILE)

    while process_status["running"]:
        try:
            latest_export = get_latest_parquet_time(
                path=os.path.dirname(k.PUBLISH_FILE)
            )

            latest_db_update = rfdb.get_latest_processing_time()

            latest_export_str = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(latest_export)
            )
            latest_db_update_str = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(latest_db_update)
            )

            message = f"Latest parquet export time: {latest_export_str}, Latest database update time: {latest_db_update_str}"
            if latest_export < latest_db_update:
                rfdb.export_parquet(file_name=k.PUBLISH_FILE)

                log.entry(
                    f"event=parquet_export_completed file={k.PUBLISH_FILE} "
                    f"detail=\"{message}\""
                )
            else:
                wait_random_time(message=message)

        except Exception as e:
            err = errors.ErrorHandler(log)
            err.capture(
                reason="Unexpected metadata publication loop failure",
                stage="PUBLISH",
                exc=e,
            )
            err.log_error()
            process_status["running"] = False

    log.entry("event=service_stop service=appCataloga_pub_metadata")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal metadata publication worker crash",
            stage="MAIN",
            exc=e,
        )
        err.log_error()
        raise
