#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""This module perform the following tasks:
- stop appCataloga_host_check.service using systemd and wait for it to finish
- list host tasks that have status 'error' (HOST_TASK.NU_STATUS=-1) or Under execution (HOST_TASK.NU_STATUS=2)
- set host tasks status to 'pending' (HOST_TASK.NU_STATUS=1)
- Decrease HOST.NU_HOST_CHECK_ERROR in the same number of tasks that were previously set to 'error'
- Decrease HOST.NU_PENDING_HOST_TASK in the same number of tasks that were previously set to 'Under execution'
- start appCataloga_host_check.service

- stop all appCataloga_file_bkp worker services using systemd and wait for it to finish
- list file backup tasks (FILE_TASK.NU_TYPE=1) that have status 'error' (FILE_TASK.NU_STATUS=-1) or Under execution (HOST_TASK.NU_STATUS=2)
- set file backup tasks (FILE_TASK.NU_TYPE=1) status to 'pending' (FILE_TASK.NU_STATUS=1)
- Decrease HOST.NU_BACKUP_ERROR in the same number of tasks that were previously set to 'error' status
- Decrease HOST.NU_PENDING_BACKUP in the same number of tasks that were previously set to 'Under execution'
- start appCataloga_file_bkp@0.service using systemd

- stop appCataloga_file_bin_proces.service worker using systemd and wait for it to finish
- list file processing tasks (FILE_TASK.NU_TYPE=2) that have status 'error' (FILE_TASK.NU_STATUS=-1) or Under execution (HOST_TASK.NU_STATUS=2)
- set file processing tasks (FILE_TASK.NU_TYPE=1) status to 'pending' (FILE_TASK.NU_STATUS=1)
- Decrease HOST.NU_PROCESSING_ERROR in the same number of tasks that were previously set to 'error' status
- Decrease HOST.NU_PENDING_PROCESSING in the same number of tasks that were previously set to 'Under execution'
- start appCataloga_file_bin_proces.service using systemd
"""

import sys

# sys.path.append('Y:\\RF.Fusion\\src\\appCataloga\\root\\etc\\appCataloga\\')
sys.path.append("/etc/appCataloga")

# Import appCataloga modules
import config as k
import shared as sh
import db_handler as dbh

from pathlib import Path
import subprocess


class system_service:
    def __init__(self, service: str, log: sh.log, multi_worker=False) -> None:
        """Class to handle system services

        Args:
            service (str): Service name
            log (sh.log): Log object
            multi_worker (bool, optional): True if service is a multi-worker service. Defaults to False.
        """
        self.service = service
        self.log = log
        self.multi_worker = multi_worker

    def stop(self) -> None:
        """Stop service using systemd"""
        if self.multi_worker:
            command = f"systemctl stop {self.service}@*"
        else:
            command = f"systemctl stop {self.service}"

        result = subprocess.run(command, capture_output=True, shell=True, text=True)
        if result.returncode != 0:
            self.log.error(f"Error stopping {self.service}: {result.stderr}")
        else:
            self.log.entry(f"{self.service} stopped by reset tool.")

    def start(self) -> None:
        command = f"systemctl start {self.service}"
        result = subprocess.run(command, capture_output=True, shell=True, text=True)
        if result.returncode != 0:
            self.log.error(f"Error starting {self.service}: {result.stderr}")
        else:
            self.log.entry(f"{self.service} started by reset tool.")

    def status(self) -> None:
        command = f"systemctl status {self.service}"
        result = subprocess.run(command, capture_output=True, shell=True, text=True)
        if result.returncode != 0:
            self.log.error(f"Error checking {self.service} status: {result.stderr}")
        else:
            self.log.entry(f"{self.service} status: {result.stdout}")


def reset_host_check(dbp: dbh.dbHandler, log: sh.log) -> None:
    """Reset host check tasks

    Args:
        dbp (dbh.dbHandler): Database handler
        log (sh.log): Log object
    """
    log.entry("Resetting host check tasks.")
    # Stop host check service
    host_check = system_service("appCataloga_host_check.service", log)
    host_check.stop()

    # List host tasks that have status 'error'
    failed_host_tasks = dbp.list_host_tasks(status=dbp.TASK_ERROR)

    # Iterate over all failed tasks and reset them
    for host_id, failed_task_list in failed_host_tasks.items():
        for failed_task in failed_task_list:
            dbp.update_host_task_status(task_id=failed_task, status=dbp.TASK_PENDING)

        dbp.update_host_status(
            host_id=host_id,
            nu_host_check_error=-failed_task_list.__len__(),
            nu_pending_host_task=failed_task_list.__len__(),
        )

        log.entry(
            f"{failed_task_list.__len__()} HOST TASKS that FAILED for host {host_id} were reset."
        )

    # Set running host tasks status to 'pending' and update host status counters. (since service is stopped, any running task is a broken task)
    running_host_tasks = dbp.list_host_tasks(status=dbp.TASK_RUNNING)

    for host_id, running_task_list in running_host_tasks.items():
        for running_task in running_task_list:
            dbp.update_host_task_status(task_id=running_task, status=dbp.TASK_PENDING)

        log.entry(
            f"{running_task_list.__len__()} HOST TASKS that were running for host {host_id} were reset."
        )

    # Start host check service
    host_check.start()


def reset_file_bkp(dbp: dbh.dbHandler, log: sh.log) -> None:
    """Reset file backup tasks

    Args:
        dbp (dbh.dbHandler): Database handler
        log (sh.log): Log object
    """
    log.entry("Resetting file backup tasks.")
    # Stop file backup service
    file_bkp = system_service("appCataloga_file_bkp", log, multi_worker=True)
    file_bkp.stop()

    # List file backup tasks that have status 'error'
    failed_file_bkp_tasks = dbp.list_file_tasks(
        task_type=dbp.BACKUP_TASK_TYPE, task_status=dbp.TASK_ERROR
    )

    # Iterate over all failed tasks and reset them
    for host_id, failed_task_list in failed_file_bkp_tasks.items():
        for failed_task in failed_task_list:
            dbp.file_task_update(task_id=failed_task, status=dbp.TASK_PENDING)

        dbp.update_host_status(
            host_id=host_id,
            nu_backup_error=-failed_task_list.__len__(),
            nu_pending_backup=failed_task_list.__len__(),
        )

        log.entry(
            f"{failed_task_list.__len__()} FILE BACKUP TASKS that FAILED for host {host_id} were reset."
        )

    running_file_bkp_tasks = dbp.list_file_tasks(
        task_type=dbp.BACKUP_TASK_TYPE, task_status=dbp.TASK_RUNNING
    )

    for host_id, running_task_list in running_file_bkp_tasks.items():
        for running_task in running_task_list:
            dbp.file_task_update(task_id=running_task, status=dbp.TASK_PENDING)

        log.entry(
            f"{running_task_list.__len__()} FILE BACKUP TASKS that were running for host {host_id} were reset."
        )

        log.entry(
            f"{running_task_list.__len__()} FILE BACKUP TASKS that were running for host {host_id} were reset."
        )

    # Start file backup service
    file_bkp.start()


def reset_file_bin_proces(dbp: dbh.dbHandler, log: sh.log) -> None:
    """Reset file processing tasks

    Args:
        dbp (dbh.dbHandler): Database handler
        log (sh.log): Log object
    """
    log.entry("Resetting file processing tasks.")
    # Stop file processing service
    file_bin_proces = system_service("appCataloga_file_bin_proces", log)
    file_bin_proces.stop()

    # List file processing tasks that have status 'error'
    failed_file_bin_proces_tasks = dbp.list_file_tasks(
        task_type=dbp.PROCESSING_TASK_TYPE, task_status=dbp.TASK_ERROR
    )

    # Iterate over all failed tasks and reset them
    for host_id, failed_task_list in failed_file_bin_proces_tasks.items():
        for failed_task in failed_task_list:
            dbp.file_task_update(task_id=failed_task, status=dbp.TASK_PENDING)

        dbp.update_host_status(
            host_id=host_id,
            nu_processing_error=-failed_task_list.__len__(),
            nu_pending_processing=failed_task_list.__len__(),
        )

        log.entry(
            f"{failed_task_list.__len__()} FILE PROCESSING TASKS that FAILED for host {host_id} were reset."
        )

    running_file_bin_proces_tasks = dbp.list_file_tasks(
        task_type=dbp.PROCESSING_TASK_TYPE, task_status=dbp.TASK_RUNNING
    )

    for host_id, running_task_list in running_file_bin_proces_tasks.items():
        for running_task in running_task_list:
            dbp.file_task_update(task_id=running_task, status=dbp.TASK_PENDING)

        log.entry(
            f"{running_task_list.__len__()} FILE PROCESSING TASKS that were running for host {host_id} were reset."
        )

    # Start file processing service
    file_bin_proces.start()


def main():
    try:  # create a warning message object
        log = sh.log(target_screen=True, target_file=False)
    except Exception as e:
        print(f"Error creating log object: {e}")
        exit(1)

    log.entry("Starting server refresh.")

    dbp = dbh.dbHandler(database=k.BKP_DATABASE_NAME, log=log)

    reset_host_check(dbp, log)

    reset_file_bkp(dbp, log)

    reset_file_bin_proces(dbp, log)

    log.entry(
        "Finish server DB and files refreshing. You may need to manually perform additional tasks. Check the log for details."
    )


if __name__ == "__main__":
    main()
