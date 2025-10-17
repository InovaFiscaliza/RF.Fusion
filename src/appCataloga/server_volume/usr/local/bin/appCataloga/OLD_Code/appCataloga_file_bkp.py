#!/usr/bin/python3
"""Get file tasks in the control database and perform backup to the central repository.

Args:
    Arguments passed from the command line should present in the format: "key=value"

    Where the possible keys are:
        "worker": int, Serial index of the worker process. Default is 0.

(stdin): ctrl+c will soft stop the process similar to kill or systemd stop <service>. kill -9 will hard stop.

Returns (stdout): As log messages, if target_screen in log is set to True.

Raises:
    Exception: If any error occurs, the exception is raised with a message describing the error.
"""

# ======================================================================
# Imports
# ======================================================================
import sys, os
import paramiko
import time
import random
import signal
import inspect

# load appCataloga path
CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga"))
sys.path.append(CONFIG_PATH)

# Import project libs
import config as k
import shared as sh
import db_handler as dbh

# ======================================================================
# Globals
# ======================================================================
log = sh.log()

process_status = {"worker": None, "running": True}

DEFAULT_WORKER = 0
ARGUMENTS = {
    "worker": {
        "set": False,
        "value": DEFAULT_WORKER,
        "warning": "Using default worker zero",
    }
}


# ======================================================================
# Signal handlers
# ======================================================================
def sigterm_handler(signal=None, frame=None) -> None:
    global process_status, log
    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"Kill signal received at: {current_function}()")
    process_status["running"] = False


def sigint_handler(signal=None, frame=None) -> None:
    global process_status, log
    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"Ctrl+C received at: {current_function}()")
    process_status["running"] = False


signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


class HaltFlagError(Exception):
    pass


# ======================================================================
# Argument parsing
# ======================================================================
def parse_arguments():
    global ARGUMENTS, process_status, log
    for arg in sys.argv[1:]:
        if "=" in arg:
            key, value = arg.split("=", 1)
            if key in ARGUMENTS:
                try:
                    ARGUMENTS[key]["value"] = int(value)
                    ARGUMENTS[key]["set"] = True
                except ValueError:
                    log.warning(f"Invalid value for {key}, using default {ARGUMENTS[key]['value']}")
            else:
                log.warning(f"Unknown argument: {arg}")

    process_status["worker"] = ARGUMENTS["worker"]["value"]
    if not ARGUMENTS["worker"]["set"]:
        log.warning(ARGUMENTS["worker"]["warning"])


# ======================================================================
# Worker management
# ======================================================================
def spawn_file_task_worker(worker_list: list) -> None:
    global log
    new_worker = 0
    for i in range(len(worker_list)):
        if new_worker == worker_list[i]:
            new_worker += 1
        elif new_worker < worker_list[i]:
            break

    log.entry(f"Spawning file backup task process worker {new_worker}")
    os.system(f"systemctl start {k.BKP_TASK_WORKER_SERVICE}{new_worker}.service")


def worker_counter(process_filename: str) -> list:
    """Count the number of running file task processes."""
    global log
    worker_list = []

    try:
        running_processes = os.popen(f"pgrep -f {process_filename}").read().splitlines()
    except Exception as e:
        log.error(f"Error counting running workers: {e}")
        return []

    for worker in running_processes:
        try:
            proc_path = f"/proc/{worker}/cmdline"
            if not os.path.exists(proc_path):
                log.warning(f"Process {worker} terminated before inspection.")
                continue

            worker_args = open(proc_path).read().split("\x00")
            for i in range(len(worker_args)):
                if process_filename in worker_args[i]:
                    try:
                        if "=" in worker_args[i + 1]:
                            worker_index = int(worker_args[i + 1].split("=")[1])
                        else:
                            worker_index = int(worker_args[i + 1])
                    except (IndexError, ValueError, TypeError):
                        worker_index = DEFAULT_WORKER
                    worker_list.append(worker_index)
                    break
        except Exception as e:
            log.warning(f"Could not inspect process {worker}: {e}")
            continue

    worker_list = sorted(list(dict.fromkeys(worker_list)))
    log.entry(f"Running workers: {worker_list}")
    return worker_list



# ======================================================================
# Main loop
# ======================================================================
def main():
    global process_status, log
    
    parse_arguments()
    log.entry(f"Starting worker {process_status['worker']}...")

    worker_list = worker_counter(process_filename=os.path.basename(__file__))
    
    
    try:
        db_bp = dbh.dbHandler(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        exit(1)

    while process_status["running"]:
        try:
            # Get Tasks in database with NU_TYPE = FILE_TASK_BACKUP_TYPE and NU_STATUS = 1
            tasks = db_bp.file_task_read_list_one_host(task_type=db_bp.FILE_TASK_BACKUP_TYPE)
            if not tasks:
                if worker_list.__len__() > 2:
                    log.entry(f"No host found with pending backup. Exiting worker {process_status['worker']}")
                    os.system(f"systemctl stop {k.BKP_TASK_WORKER_SERVICE}{process_status['worker']}.service")
                    continue
                else:
                    time_to_wait = int((k.MAX_FILE_TASK_WAIT_TIME + k.MAX_FILE_TASK_WAIT_TIME * random.random()) / 2)
                    log.entry(f"Waiting {time_to_wait} seconds for new tasks.")
                    time.sleep(time_to_wait)
                    continue

            host = db_bp.host_read_access(tasks["host_id"])

            sftp_conn = sh.sftpConnection(
                host_uid=host["host_uid"],
                host_add=host["host_add"],
                port=host["port"],
                user=host["user"],
                password=host["password"],
                log=log,
            )

            daemon = sh.hostDaemon(
                sftp_conn=sftp_conn,
                db_bp=db_bp,
                host_id=host["host_id"],
                task_dict=tasks["file_tasks"],
                log=log,
            )

            if not daemon.get_config(task_type=db_bp.FILE_TASK_BACKUP_TYPE, remove_failed_task=False):
                continue
            if not daemon.get_halt_flag(task_type=db_bp.FILE_TASK_BACKUP_TYPE, remove_failed_task=False):
                continue

            if worker_list.__len__() < k.BKP_TASK_MAX_WORKERS:
                spawn_file_task_worker(worker_list=worker_list)

            local_path = f"{k.REPO_FOLDER}/{k.TMP_FOLDER}/{host['host_add']}"
            if not os.path.exists(local_path):
                os.makedirs(local_path)

            for task_id, file_list in tasks["file_tasks"].items():
                filename = file_list[1]
                file_list[2] = local_path
                file_list[3] = filename

                remote_file = f"{file_list[0]}/{filename}"

                if not sftp_conn.test(remote_file):
                    message = f"File '{remote_file}' not found in remote host {host['host_add']}"
                    db_bp.file_task_update(task_id=task_id, status=db_bp.TASK_ERROR, message=message)
                    db_bp.host_update(host_id=host["host_id"], pending_backup=-1, backup_error=1)
                    log.warning(message)
                    continue

                local_file = f"{local_path}/{filename}"

                try:                  
                    # Transfer file
                    sftp_conn.transfer(remote_file, local_file)
                    
                    # Update NU_STATUS = 1 (Pending), NU_TYPE = 2 (Processing) to next step - File Processing
                    db_bp.file_task_update(
                        task_id=task_id,
                        server_file=filename,
                        server_path=local_path,
                        task_type=db_bp.FILE_TASK_PROCESS_TYPE,
                        status=db_bp.TASK_PENDING,
                        message=f"File '{remote_file}' copied to '{local_file}'",
                    )
                    db_bp.host_update(host_id=host["host_id"], pending_backup=-1, pending_processing=1)
                    daemon.set_backup_done(remote_file)
                    log.entry(f"File '{filename}' copied to '{local_file}'")
                except Exception as e:
                    message = f"Error copying '{remote_file}' from host {host['host_add']}. {str(e)}"
                    db_bp.file_task_update(task_id=task_id, status=db_bp.TASK_ERROR, message=message)
                    db_bp.host_update(host_id=host["host_id"], pending_backup=-1, backup_error=1)
                    continue

            daemon.close_host()

        except paramiko.AuthenticationException as e:
            log.error(f"Authentication failed. Please check your credentials. {str(e)}")
            raise ValueError(log.dump_error())
        except paramiko.SSHException as e:
            log.error(f"SSH error: {str(e)}")
            raise ValueError(log.dump_error())
        except HaltFlagError:
            pass
        except Exception as e:
            log.error(f"Unmapped error occurred: {str(e)}")
            raise ValueError(log.dump_error())

    log.entry("Shutting down....")


if __name__ == "__main__":
    main()
