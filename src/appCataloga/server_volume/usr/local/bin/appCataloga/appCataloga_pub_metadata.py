#!/usr/bin/python3
"""
Metadata publication worker for appCataloga.

This service exports the latest processed metadata to Parquet when the database
has changed since the last published snapshot. The loop stays intentionally
small because it is operational glue, not a data-processing pipeline.
"""

import os
import random
import sys
import time

from bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)


# Import appCataloga modules
import config as k
from db.dbHandlerRFM import dbHandlerRFM
from server_handler import signal_runtime
from shared import errors, logging_utils


# ============================================================
# Globals
# ============================================================
log = logging_utils.log()
process_status = {"running": True}


signal_runtime.install_shutdown_handlers(
    process_status=process_status,
    logger=log,
)


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

    log.event("publish_wait", seconds=wait_time, detail=message)
    time.sleep(wait_time)


def main():
    """
    Run the publication polling loop until shutdown is requested.
    """

    log.service_start("appCataloga_pub_metadata")
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

                log.event(
                    "parquet_export_completed",
                    file=k.PUBLISH_FILE,
                    detail=message,
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

    log.service_stop("appCataloga_pub_metadata")


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
