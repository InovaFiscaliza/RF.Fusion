#!/usr/bin/python3
"""Get list of files to backup from remote host and create file tasks in the control database for backup to the central repository.

Args (stdin): ctrl+c will soft stop the process similar to kill command or systemd stop <service>. kill -9 will hard stop.

Returns (stdout): As log messages, if target_screen in log is set to True.

Raises:
    Exception: If any error occurs, the exception is raised with a message describing the error.
"""

# Set system path to include modules from /etc/appCataloga
import sys

sys.path.append("/etc/appCataloga")

# Import standard libraries.
import paramiko
import signal
import inspect
import time
import random

# Import modules for file processing
import config as k
import shared as sh
import db_handler as dbh

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


def process_due_backup(
    sftp_conn: sh.sftpConnection, daemon_cfg: dict, task: dict, db_bp: dbh.dbHandler
) -> None:
    """Process the list of files to backup from the DUE_BACKUP file.

    Args:
        sftp_conn (sftpConnection): The SFTP connection object.
        daemon_cfg (dict): The daemon configuration dictionary.
        task (dict): The task dictionary containing host information.
        db_bp (dbh.dbHandler): The database handler object.

    Returns:
        None
    """
    global log
    global process_status

    due_backup_str = sftp_conn.read(filename=daemon_cfg["DUE_BACKUP"], mode="r")

    if due_backup_str:
        # Clean the string and split it into a list of files
        due_backup_str = due_backup_str.decode(encoding="utf-8")
        due_backup_str = "".join(due_backup_str.split("\x00"))
        due_backup_list = due_backup_str.splitlines()

        # Create file tasks for later handling by the file task process
        db_bp.file_task_create(
            host_id=task["host_id"],
            task_type=db_bp.FILE_TASK_BACKUP_TYPE,
            volume=task["host_uid"],
            files=due_backup_list,
        )


def main():
    """Main function to start the host check process."""

    global process_status
    global log

    log.entry("Starting....")

    try:
        # create db object using databaseHandler class for the backup and processing database
        db_bp = dbh.dbHandler(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        exit(1)

    while process_status["running"]:
        try:
            task = db_bp.host_task_read()
            """	{   "task_id": (int),
                    "host_id": (int),
                    "host_uid": (str),
                    "host_add": (str),
                    "port": (int),
                    "user": (str),
                    "password": (str)}"""

            if not task:
                # wait before trying again
                time_to_wait = int(
                    (
                        k.MAX_HOST_TASK_WAIT_TIME
                        + k.MAX_HOST_TASK_WAIT_TIME * random.random()
                    )
                    / 2
                )
                log.entry(f"Waiting {time_to_wait} seconds for new tasks.")
                time.sleep(time_to_wait)
                continue

            # set task status to running
            db_bp.host_task_update(task_id=task["task_id"], status=db_bp.TASK_RUNNING)

            # Create a SSH client and SFTP connection to the remote host
            sftp_conn = sh.sftpConnection(
                host_uid=task["host_uid"],
                host_add=task["host_add"],
                port=task["port"],
                user=task["user"],
                password=task["password"],
                log=log,
            )

            daemon = sh.hostDaemon(
                sftp_conn=sftp_conn,
                db_bp=db_bp,
                host_id=task["host_id"],
                log=log,
                task_id=task["task_id"],
            )

            # Get the remote host configuration file
            if not daemon.get_config(
                task_type=db_bp.HOST_TASK_TYPE, remove_failed_task=True
            ):
                continue

            # Set halt flag
            if not daemon.get_halt_flag(
                task_type=db_bp.HOST_TASK_TYPE, remove_failed_task=True
            ):
                continue

            # Get the list of files to backup from DUE_BACKUP file and create file tasks
            process_due_backup(
                sftp_conn=sftp_conn, daemon_cfg=daemon.config, task=task, db_bp=db_bp
            )

            daemon.close_host(remove_due_backup=True)

        except paramiko.AuthenticationException as e:
            log.error(f"Authentication failed. Please check your credentials. {str(e)}")
            raise ValueError(log.dump_error())

        except paramiko.SSHException as e:
            log.error(f"SSH error: {str(e)}")
            raise ValueError(log.dump_error())

        except Exception as e:
            log.error(f"Unmapped error occurred: {str(e)}")
            raise ValueError(log.dump_error())

    log.entry("Shutting down....")


if __name__ == "__main__":
    main()
