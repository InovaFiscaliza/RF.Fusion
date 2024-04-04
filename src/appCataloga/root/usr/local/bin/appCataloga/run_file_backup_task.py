#!/usr/bin/python3
"""Get file tasks in the control database and perform backup to the central repository.
    
    Args:   Arguments passed from the command line should present in the format: "key=value"
    
            Where the possible keys are:
            
                "worker": int, Serial index of the worker process. Default is 0.
                
            (stdin): ctrl+c will soft stop the process similar to kill or systemd stop <service>. kill -9 will hard stop.

    Returns (stdout): As log messages, if target_screen in log is set to True.            
    
    Raises:
        Exception: If any error occurs, the exception is raised with a message describing the error.
"""

# Set system path to include modules from /etc/appCataloga
import sys
sys.path.append('/etc/appCataloga')

# Import standard libraries.
# Import modules for file processing 
import config as k
import shared as sh
import db_handler as dbh

import paramiko
import os
import time
import random
import signal
import inspect

# define global variables for log and general use
log = sh.log()

process_status = {  "worker": None,
                    "conn": False,
                    "halt_flag": False,
                    "running": True}

DEFAULT_WORKER = 0
# define arguments as dictionary to associate each argumenbt key to a default value and associated warning messages
ARGUMENTS = {
    "worker": {
        "set": False,
        "value": DEFAULT_WORKER,
        "warning": "Using default worker zero"
        }
    }

# Define a signal handler for SIGTERM (kill command )
def sigterm_handler(signal=None, frame=None) -> None:
    global process_status
    global log
      
    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"Kill signal received at: {current_function}()")
    process_status["running"] = False

# Define a signal handler for SIGINT (Ctrl+C)
def sigint_handler(signal=None, frame=None) -> None:
    global process_status
    global log
    
    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"Ctrl+C received at: {current_function}()")
    process_status['running'] = False

# Register the signal handler function, to handle system kill commands
signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)

class HaltFlagError(Exception):
    pass

def spawn_file_task_worker(worker_list: list) -> None:
    """Spawn a new file task process.
    
    Args:
        worker (int): The new worker serial index.
    
    Returns:
        None
    """
    global process_status
    global log
    
    # find the lowest available worker index for the new process
    new_worker = 0
    for i in range(worker_list.__len__()):
        if new_worker == worker_list[i]:
            new_worker += 1
        elif new_worker < worker_list[i]:
            break

    log.entry(f"Spawning file backup task process worker {new_worker}")
    
    # Use systemd to start a new file task process
    # Comment this line for testing if there is no systemd service available
    os.system(f"systemctl start {k.FILE_TASK_SERVICE_NAME}{new_worker}")

def worker_counter(process_filename:str) -> list:
    """Count the number of running file task processes.
    
    Args:
        None
    
    Returns:
        list: A list with indexes of the running file task processes.
    """
    global log
    worker_list = []
    
    try:
        # get the list of running processes
        runnin_processes = os.popen(f"pgrep -f {process_filename}").read().splitlines()
    except Exception as e:
        log.error(f"Error counting running workers: {e}")
        raise ValueError(log.dump_error())
    
    # loop through the list of running processes and get the worker serial index from the command line arguments of each process
    for worker in runnin_processes:
        try:
            worker_args = os.popen(f"cat /proc/{worker}/cmdline").read().split('\x00')
            
            for i in range(worker_args.__len__()):
                if worker_args[i] == process_filename:
                    try:
                        worker_index = int(worker_args[i+1])
                    except (IndexError, ValueError, TypeError):
                        worker_index = DEFAULT_WORKER
                        pass
                    worker_list.append(int(worker_index))
                    break
        except Exception as e:
            log.error(f"Error getting worker serial index from process {worker}: {e}")
            raise ValueError(log.dump_error())
    
    # reorder worker_args in numerical ascending order
    worker_list = sorted(worker_list)
    
    # remove duplicates from worker_list
    worker_list = list(dict.fromkeys(worker_list))
    
    log.entry(f"Running workers: {worker_list}")
    
    return worker_list

def main():
    global process_status
    global log

    log.entry("Starting....")
    
    worker_list = worker_counter(process_filename=__file__)
    
    try:
        # create db object using databaseHandler class for the backup and processing database
        db_bp = dbh.dbHandler(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        exit(1)

    while process_status["running"]:
        
        try:
            
            # Get the list of files to backup from the database
            tasks = db_bp.next_file_task_list(task_type=db_bp.BACKUP_TASK_TYPE)
            """ tasks ={   "host_id": (int) host_id,
                            task_id: [
                                        host_file_path,
                                        host_file_name,
                                        server_file_path,
                                        server_file_name]}
            """

            if not tasks:
                # if it is not the worker with serial zero, terminate process.
                if worker_list.__len__() > 2:
                    log.entry(f"No host found with pending backup. Exiting worker {process_status['worker']}")
                    # stop systemd service for this worker.
                    os.system(f"systemctl stop {k.FILE_TASK_SERVICE_NAME}{process_status['worker']}")
                    continue
                # if it is the zero worker, wait and try again later.
                else:
                    time_to_wait = int((k.MAX_FILE_TASK_WAIT_TIME+k.MAX_FILE_TASK_WAIT_TIME*random.random())/2)
                    log.entry(f"Waiting {time_to_wait} seconds for new tasks.")
                    time.sleep(time_to_wait)
                    continue

            # Using task["host_id"] to get the host configuration from the database
            host = db_bp.get_host(tasks["host_id"])
            """{"host_uid": str,
                "host_add": str,
                "port": int,
                "user": str,
                "password": str}"""

            # Create a SSH client and SFTP connection to the remote host
            sftp_conn = sh.sftpConnection(  host_uid=host["host_uid"],
                                            host_add=host["host_add"],
                                            port=host["port"],
                                            user=host["user"],
                                            password=host["password"],
                                            log=log)
        
            process_status["conn"] = sftp_conn
            
            daemon = sh.hostDaemon( sftp_conn=sftp_conn,
                                    db_bp=db_bp,
                                    host_id=host["host_id"],
                                    task_dict=tasks["file_tasks"],
                                    log=log)
            
            # Get the remote host configuration file
            daemon.get_config(remove_failed_task=False)

            # Set halt flag if not already set (first run)
            process_status["halt_flag"] = daemon.get_halt_flag(remove_failed_task=False)
            
            # After trying to set or reset the halt flag, if it was not set
                # move to attempt the next task. (current task was susppended bt the get_halt_flag method)
            if not process_status["halt_flag"]:
                continue
                        
            # Before processing the current task, spawn another file backup task process to look for other hosts with pending backup
            if worker_list.__len__() < k.BKP_TASK_MAX_WORKERS:
                spawn_file_task_worker(worker_list=worker_list)
            
            # * Peform the backup
            # Loop through all tasks in the task_dict['file_tasks'], geting for each its task_id and index
            local_path = f"{k.REPO_FOLDER}/{k.TMP_FOLDER}/{host['host_add']}"
            
            # make sure that the target folder do exist
            if not os.path.exists(local_path):
                os.makedirs(local_path)
            
            for task_id, file_list in tasks["file_tasks"].items():
                filename = file_list[1]
                file_list[2] = local_path
                file_list[3] = filename
                
                remote_file = f"{file_list[0]}/{filename}"
            
                # test if remote_file does not exist, update the database and skip to the next file
                if not sftp_conn.test(remote_file):
                    message=f"File '{remote_file}' not found in remote host {host['host_add']}"
                    db_bp.file_task_error(  task_id=task_id,
                                            message=message)
                    
                    db_bp.update_host_status(   host_id=host["host_id"],
                                                pending_backup=-1,
                                                backup_error=1)
                    
                    log.warning(message)
                    continue
                
                # Compose target file name by adding the remote file name to the target folder
                local_file = f"{local_path}/{filename}"
                
                # Transfer the file from the remote host to the local host
                try:
                    sftp_conn.transfer(remote_file, local_file)
                    
                    # Change file task from running backup  to pending processing
                    db_bp.file_task_update( task_id=task_id,
                                            server_file=filename,
                                            server_path=local_path,
                                            task_type=db_bp.PROCESS_TASK_TYPE,
                                            status=db_bp.TASK_PENDING,
                                            message=f"File '{remote_file}' copied to '{local_file}'")
                    
                    # update host status
                    db_bp.update_host_status(   host_id=host["host_id"],
                                                pending_backup=-1,
                                                pending_processing=1)
                    
                    # update host backup done file
                    daemon.set_backup_done(remote_file)
                    
                    log.entry(f"File '{filename}' copied to '{local_file}'")
                    
                except Exception as e:
                    message=f"Error copying '{remote_file}' from host {host['host_add']}.{str(e)}"
                    db_bp.file_task_error(  task_id=task_id,
                                            message=message)
                    
                    db_bp.update_host_status(   host_id=host["host_id"],
                                                pending_backup=-1,
                                                backup_error=1)
                    continue
        
            # Remove HALT FLAG, close the SSH client and SFTP connection
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
    
    # TODO: #37 Check if task was completted or a kill request was received, and update the database accordingly to move back tasks to pending status
    
    log.entry("Shutting down....")
    
if __name__ == "__main__":
    main()

