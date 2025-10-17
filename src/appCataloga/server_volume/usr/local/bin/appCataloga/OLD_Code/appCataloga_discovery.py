#!/usr/bin/python3
"""Get list of files to backup from remote host and create file tasks in the control database for backup to the central repository.

Args (stdin): ctrl+c will soft stop the process similar to kill command or systemd stop <service>. kill -9 will hard stop.

Returns (stdout): As log messages, if target_screen in log is set to True.

Raises:
    Exception: If any error occurs, the exception is raised with a message describing the error.
"""

# Set system path to include modules from /etc/appCataloga
import sys,os

# Import standard libraries.
import paramiko
import signal
import inspect
import time
import random

# load Config and Database folders
_CFG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga"))
if _CFG_DIR not in sys.path and os.path.isdir(_CFG_DIR):
    sys.path.append(_CFG_DIR)
_DB_DIR = os.path.join(os.path.dirname(__file__), "db")
if _DB_DIR not in sys.path and os.path.isdir(_DB_DIR):
    sys.path.append(_DB_DIR)
    
# Import customized libraries
from db.dbHandlerBKP import dbHandlerBKP
import shared as sh
import config as k

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
    # Added because dagemon.get_halt_flag cold 
    sys.exit(0)


# Register the signal handler function, to handle system kill commands
signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


def process_due_backup(
    sftp_conn: sh.sftpConnection, daemon_cfg: dict, task: dict, db_bp: dbHandlerBKP
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

    # Read .files.changed.list in remote node and check new available files discovered by Agent
    due_backup_str = sftp_conn.read(filename=daemon_cfg["DUE_BACKUP"], mode="r")

    if due_backup_str:
        # Clean the string and split it into a list of files
        due_backup_str = due_backup_str.decode(encoding="utf-8")
        due_backup_str = "".join(due_backup_str.split("\x00"))
        due_backup_list = due_backup_str.splitlines()
        
        # Get metadata from files mapped
        due_backup_metadata = sftp_conn.get_metadata_files(due_backup_list)

        # Create a Discovery NU_TYPE=DISCOVERY in FILE_TASK Table
        db_bp.file_task_create(
            host_id=task["host_id"],
            volume=task["host_uid"],
            backup_type=k.FILE_TASK_BACKUP_TYPE,
            backup_status=k.TASK_PENDING,
            discovery_type=k.FILE_TASK_DISCOVERY,
            discovery_status=k.TASK_DONE,
            files=due_backup_list,
            file_metadata=due_backup_metadata,
            task_filter=task["host_filter"],
            log=log,
        )
            
        
def main():
    """Main function to start the host check process."""

    global process_status
    global log

    log.entry("Starting....")

    try:
        # create db object using databaseHandler class for the backup and processing database
        db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        exit(1)

    while process_status["running"]:
        try:
            # Check HOST_TASK tablet and find peding HOST_TASK
            custom_read = db_bp.host_task_read()
            
            if not custom_read:
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

            # Set task status to running
            db_bp.host_task_update(task_id=custom_read["task_id"], status=k.TASK_RUNNING)

            # Create a SSH client and SFTP connection to the remote host
            sftp_conn, daemon = sh.init_host_context(host=custom_read,log=log)

            # Get the remote host configuration file
            # Read indexerD ang get information from which file have to evaluated
            if not daemon.get_config(
                task_type=k.HOST_TASK_TYPE, remove_failed_task=True
            ):
                continue

            # Read remote halt flag
            if not daemon.get_halt_flag(
                task_type=k.HOST_TASK_TYPE, remove_failed_task=True
            ):
                continue
            
            # Criar uma função de atualizar o backlog das FILE_TASKS
            db_bp.update_backlog_by_filter(host_id=custom_read["host_id"], 
                                           task_filter=custom_read["host_filter"],
                                           backup_type=k.FILE_TASK_BACKUP_TYPE,
                                            backup_status=k.TASK_PENDING,
                                            discovery_type=k.FILE_TASK_DISCOVERY,
                                            discovery_status=k.TASK_DONE,
                                            log=log)

            # Get the list of files to backup from DUE_BACKUP file and create file tasks
            process_due_backup(
                sftp_conn=sftp_conn, daemon_cfg=daemon.config, task=custom_read, db_bp=db_bp
            )
            
            # Close daemon connection and remove HOST_TASK
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
