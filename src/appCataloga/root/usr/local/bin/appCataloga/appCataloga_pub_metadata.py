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

# define global variables for log and general use
log = sh.log()

process_status = {"conn": None, "halt_flag": None, "running": True}


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

    while process_status["running"]:
        try:
            # publish the metadata to the parquet file
            rfdb.publish_parquet(file_name=k.PUBLISH_FILE)

        except Exception as e:
            log.error(f"Unmapped error occurred: {str(e)}")
            raise ValueError(log.dump_error())

    log.entry("Shutting down....")


if __name__ == "__main__":
    main()
