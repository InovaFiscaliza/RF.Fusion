#!/usr/bin/python3
"""Access the control database and get files associated with a host to be backed up to the central repository.
    
    Args: None
    
    Returns:
        stdout: As log messages, if target_screen in log is set to True.
            
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
import json
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
    log.entry(f"\nKill signal received at: {current_function}()")
    process_status["running"] = False

# Define a signal handler for SIGINT (Ctrl+C)
def sigint_handler(signal=None, frame=None) -> None:
    global process_status
    global log
    
    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"\nCtrl+C received at: {current_function}()")
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
    for i in range(worker_list.__len__() - 1):
        if new_worker == worker_list[i]:
            new_worker += 1

    log.entry(f"Spawning file backup task process worker {new_worker}")
    
    # Use systemd to start a new file task process
    command = f"systemctl start {k.FILE_TASK_SERVICE_NAME}{new_worker}"
    os.system(command)

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
        worker_list = os.popen(f"pgrep -f {process_filename}").read().splitlines()
    except Exception as e:
        log.error(f"Error counting running workers: {e}")
        raise ValueError(log.dump_error())
    
    # loop through the list of running processes and get the worker serial index from the command line arguments of each process
    for worker in worker_list:
        try:
            worker_args = os.popen(f"cat /proc/{worker}/cmdline").read().split('\x00')
            worker_args = [arg for arg in worker_args if arg]
            worker_args = worker_args[1:]
            worker_args = [arg.split('=') for arg in worker_args]
            worker_args = {arg[0]:arg[1] for arg in worker_args}
        except Exception as e:
            log.error(f"Error getting worker serial index from process {worker}: {e}")
            raise ValueError(log.dump_error())
    
    # reorder worker_args in numerical ascending order
    worker_list = sorted(worker_args, key=lambda x: int(x))
    
    return worker_list

def main():
    global process_status
    global log

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
            task = db_bp.next_file_tasks(task_type=db_bp.BACKUP_TASK_TYPE)
            """ task = {"host_id": (int) host_id,
                        "task_ids": (dict){ task_id: [
                                                        host_file_path,
                                                        host_file_name,
                                                        server_file_path,
                                                        server_file_name]}
            """
            
            if not task:
                # if it is not the worker with serial zero, terminate process.
                if worker_list.__len__() > 2:
                    log.entry(f"No host found with pending backup. Exiting worker {process_status['worker']}")
                    # stop systemd service for this worker.
                    os.system(f"systemctl stop {k.FILE_TASK_SERVICE_NAME}{process_status['worker']}")
                    continue
                # if it is the zero worker, wait and try again later.
                else:
                    time_to_wait = k.FILE_TASK_EXECUTION_WAIT_TIME+k.FILE_TASK_EXECUTION_WAIT_TIME*random.random()
                    log.entry(f"No host found with pending backup. Waiting {time_to_wait} seconds")
                    time.sleep(time_to_wait)
                    continue

            # Using task["host_id"] to get the host configuration from the database
            host = db_bp.get_host(task["host_id"])

            # Create a SSH client and SFTP connection to the remote host
            sftp_conn = sh.sftp_connection( hostname=host["host_add"],
                                            port=host["port"],
                                            username=host["user"],
                                            password=host["password"],
                                            log=log)
        
            process_status["conn"] = sftp_conn
            
            daemon = sh.hostDaemon( sftp_conn=sftp_conn,
                                    db_bp=db_bp,
                                    task=task,
                                    log=log)
            
            # Get the remote host configuration file
            daemon.get_config()

            # Set halt flag 
            process_status["halt_flag"] = daemon.get_halt_flag()
            
            if not process_status["halt_flag"]:
                continue
            
            # Before processing the current task, spawn another file backup task process to look for other hosts with pending backup
            if worker_list.__len__() < k.FILE_TASK_MAX_WORKERS:
                spawn_file_task_worker(worker_list=worker_list)
            
            # * Peform the backup
            # Loop through all tasks in the task_dict['task_ids'], geting for each its task_id and index
            local_path = f"{k.REPO_FOLDER}/{k.TMP_FOLDER}/{host['host_add']}"
            
            # make sure that the target folder do exist
            if not os.path.exists(local_path):
                os.makedirs(local_path)
            
            server_files = []
            for task_id, index in task_dict["task_ids"]:
                
                remote_file = task_dict["host_files"][index]
            
                # test if remote_file does not exist, update the database and skip to the next file
                if not sftp_conn.test(remote_file):
                    message=f"File '{remote_file}' not found in remote host {host['host_add']}"
                    db_bp.file_task_error(  task_id=task_id,
                                            message=message)
                    
                    db_bp.update_host_status(   host_id=task_dict["host_id"],
                                                pending_backup=-1,
                                                backup_error=1)
                    
                    log.warning(message)
                    server_files.append("File not found")
                    continue
                
                # Compose target file name by adding the remote file name to the target folder
                local_file = os.path.basename(remote_file)
                
                full_local_file = os.path.join(local_path, local_file)
                
                # Transfer the file from the remote host to the local host
                try:
                    sftp_conn.transfer(remote_file, full_local_file)
                    
                    # Change file task from backup to processing. (assume every file needs processing)
                    db_bp.file_task_update( task_id=task_id,
                                            server_file=local_file,
                                            server_path=local_path,
                                            task_type=db_bp.PROCESS_TASK_TYPE,
                                            status=db_bp.TASK_PENDING,
                                            message=f"File '{remote_file}' copied to '{local_file}'")
                    
                    db_bp.update_host_status(   host_id=task_dict["host_id"],
                                                pending_backup=-1,
                                                pending_processing=1)
                    
                    log.entry(f"File '{os.path.basename(remote_file)}' copied to '{local_file}'")
                    
                    server_files.append(local_file)
                    
                    # refresh the HALT_FLAG timeout control
                    time_since_start = time.time()-halt_flag_time
                    
                    if time_since_start > time_limit:
                        try:
                            halt_flag_file_handle = sftp_conn.sftp.open(daemon_cfg['HALT_FLAG'], 'w')
                            halt_flag_file_handle.write(f'running backup for {time_since_start/60} minutes\n')
                            halt_flag_file_handle.close()
                        except Exception as e:
                            log.warning(f"Could not raise halt_flag for host {host['host_id']['value']}.{str(e)}")
                            pass
                    
                except Exception as e:
                    message=f"Error copying '{remote_file}' from host {host['host_add']}.{str(e)}"
                    db_bp.file_task_error(  task_id=task_id,
                                            message=message)
                    
                    db_bp.update_host_status(   host_id=task_dict["host_id"],
                                                pending_backup=-1,
                                                backup_error=1)
                    
                    log.warning(f"Error copying '{remote_file}' from host {host['host_add']}.{str(e)}")
                    
                    server_files.append("Error copying")
                    continue
        
            # ! PARADO AQUI PRECISA PEGAR A LISTA DE ARQUIVOS NO REMOTE HOST E ATUALIZAR COM OS QUE FORAM COPIADOS. USAR SET DROP PARA REMOVER E ADICIONAR AO BACKUP_DONE
            # * Get the list of files to backup from DUE_BACKUP file
            # due_backup_file = sftp.open(daemon_cfg['DUE_BACKUP'], 'r')
            due_backup_str = sftp_conn.read(daemon_cfg['DUE_BACKUP'], 'r')
            
            if due_backup_str == "":
                nu_host_files = 0
                due_backup_list = []
            else:
                # Clean the string and split the into a list of files
                due_backup_str = due_backup_str.decode(encoding='utf-8')
                due_backup_str = ''.join(due_backup_str.split('\x00'))
                
                # create a set of filenames from the due_backup_str, where each line correspond to a filename
                due_backup_set = set(due_backup_str.splitlines())
                nu_host_files = len(due_backup_set)

            # initializa backup control variables
            nu_backup_error = 0
            done_backup_list = []
            done_backup_list_remote = []

            # Test if there are files to backup. Done before the loop to avoid unecessary creation of the target folder
            if nu_host_files > 0:
                
                
                

                # use bkp_list_index to control item in the list that is under backup, skipping the ones that failed
                bkp_list_index = 0
                while len(due_backup_list) > bkp_list_index:
                    
                    # get the first element in the due_backup_list
                    remote_file = due_backup_list[bkp_list_index]
                    
                        
                        # Compose target file name by adding the remote file name to the target folder
                        local_file = os.path.join(target_folder, os.path.basename(remote_file))
                                            
                        try:
                            sftp.get(remote_file, local_file)

                            # Remove the element from the due_backup_list if the backup was successfull
                            due_backup_list.pop(bkp_list_index)
                            
                            # Add the file name to the done_backup remote and local lists
                            done_backup_list.append({"remote":remote_file,"local":local_file})
                            done_backup_list_remote.append(remote_file)
                            
                            log.entry(f"File '{os.path.basename(remote_file)}' copied to '{local_file}'")
                        except Exception as e:
                            log.warning(f"Error copying '{remote_file}' from host {task.data['host_add']['value']}.{str(e)}")
                            # skip to the next item for backup
                            bkp_list_index += 1
                            pass
                    else:
                        # If file does not exixt, remove the element from the due_backup_list if the backup was successfull
                        due_backup_list.pop(bkp_list_index)

                # Test if there is a BACKUP_DONE file in the remote host
                if not _check_remote_file(sftp, daemon_cfg['BACKUP_DONE'], task):
                    # Create a BACKUP_DONE file in the remote host with the list of files in done_backup_list_remote
                    backup_done_file = sftp.open(daemon_cfg['BACKUP_DONE'], 'w')
                else:
                    # Append the list of files in done_backup_list_remote to the BACKUP_DONE file in the remote host
                    backup_done_file = sftp.open(daemon_cfg['BACKUP_DONE'], 'a')
                    
                backup_done_file.write("\n".join(done_backup_list_remote) + "\n")
                backup_done_file.close()
                    
                # Overwrite the DUE_BACKUP file in the remote host with the list of files in due_backup_list
                if len(due_backup_list)>0:
                    due_backup_file = sftp.open(daemon_cfg['DUE_BACKUP'], 'w')
                    due_backup_file.write("\n".join(due_backup_list) + "\n")
                    due_backup_file.close()
                else:
                    # Remove the DUE_BACKUP file in the remote host if there are no more files to backup
                    sftp.remove(daemon_cfg['DUE_BACKUP'])
                
                nu_backup_error = len(due_backup_list)/nu_host_files
            
            # Remove the HALT_FLAG file from the remote host
            sftp.remove(daemon_cfg['HALT_FLAG'])

            # Close the SSH client and SFTP connection
            sftp.close()
            ssh_client.close()
            
            output = { 'host_id': task.data["host_id"]["value"],
                    'nu_host_files': nu_host_files, 
                    'nu_pending_backup': len(due_backup_list), 
                    'nu_backup_error': nu_backup_error,
                    'done_backup_list':done_backup_list}

            print(json.dumps(output))
        
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
    
if __name__ == "__main__":
    main()
