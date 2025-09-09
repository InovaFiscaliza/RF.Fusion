#!/usr/bin/python3
"""
This module publishes measurement metadata using parket file format
The created file is stored in the /etc/appCataloga/config.py PUBLISH_FILE location

Usage:
    python appCataloga_pub_metadata.py

Returns:    metadata file
            log entries
"""

import sys

sys.path.append("/etc/appCataloga")

# Import appCataloga modules
import config as k
import shared as sh
import db_handler as dbh

import signal
import inspect
import os
import time
import random

# define global variables for log and general use
log = sh.log()

process_status = {"running": True}


# Define a signal handler for SIGTERM (kill command )
def sigterm_handler(signal=None, frame=None) -> None:
    """Signal handler for SIGTERM (Kill) to stop the process."""

    global process_status
    global log

    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"Kill signal received at: {current_function}()")
    process_status["running"] = False


# Define a signal handler for SIGINT (Ctrl+C)
def sigint_handler(signal=None, frame=None) -> None:
    """Signal handler for SIGINT (Ctrl+C) to stop the process."""

    global process_status
    global log

    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"Ctrl+C received at: {current_function}()")
    process_status["running"] = False


# Register the signal handler function, to handle system kill commands
signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


def test_path(file_name: str) -> None:
    """Test if the path to the file exists, if not create it.

    Args:
        file_name (str): File name with path
    """

    path = os.path.dirname(file_name)
    if not os.path.exists(path):
        os.makedirs(path)


def get_latest_parquet_time(path: str) -> float:
    """Get the latest update time for the parquet files in the path.

    Args:
        path (str): Path to the parquet files

    Returns:
        float: Latest update time
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
        log.error(f"Error getting latest parquet time: {e}")
        pass

    return latest_export


def wait_random_time(message: str) -> int:
    """Wait for a random time between within limits defined in config.py."""

    wait_time = int(
        (k.MAX_HOST_TASK_WAIT_TIME + k.MAX_HOST_TASK_WAIT_TIME * random.random()) / 2
    )

    log.entry(f"Waiting for {wait_time} seconds. {message}")
    time.sleep(wait_time)


def main():
    """Main function to start the host check process."""

    global process_status
    global log

    log.entry("Starting....")

    try:
        # create db object using databaseHandler class for the backup and processing database
        rfdb = dbh.dbHandler(database=k.RFM_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        exit(1)

    # test if path to k.PUBLISH_FILE exists and create it if it does not
    test_path(file_name=k.PUBLISH_FILE)

    while process_status["running"]:
        try:
            # get the latest update time for the parquet files in the path k.PUBLISH_FILE
            latest_export = get_latest_parquet_time(
                path=os.path.dirname(k.PUBLISH_FILE)
            )

            # get latest update of metadata in the database
            latest_db_update = rfdb.get_latest_processing_time()

            # convert timstamp to string
            latest_export_str = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(latest_export)
            )
            latest_db_update_str = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(latest_db_update)
            )

            message = f"Latest parquet export time: {latest_export_str}, Latest database update time: {latest_db_update_str}"
            if latest_export < latest_db_update:
                rfdb.export_parquet(file_name=k.PUBLISH_FILE)

                log.entry(f"Published new set of parquet files. {message}.")
            else:
                wait_random_time(message=message)

        except Exception as e:
            log.error(f"Unmapped error occurred: {str(e)}")
            process_status["running"] = False
            pass

    log.entry("Shutting down....")


if __name__ == "__main__":
    main()
