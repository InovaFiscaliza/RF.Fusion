#!/usr/bin/env python
"""
Access the backup list from BKPDATA database and starts the independent backup process.
    
    Usage:
        backup_control
            
    Returns:
        (json) =  { 'Total Files': (int),
                    'Files to backup': (int),
                    'Last Backup data': (str)
                    'Days since last backup': (int),
                    'Status': (int), 
                    'Message': (str)}

        Status may be 1=valid data or 0=error in the script
        All keys except "Message" are suppresed when Status=0
        Message describe the error or warning information
        
        
"""
# Set system path to include modules from /etc/appCataloga
import sys
sys.path.append('/etc/appCataloga')

# Import modules for file processing 
import config as k
import db_handler as dbh
import shared as sh

import subprocess
import json

import time

def main():
    
    # create a warning message object
    log = sh.log()
    
    failed_task = { 'host_id': 0,
                    'nu_host_files': 0, 
                    'nu_pending_backup': 0, 
                    'nu_backup_error': 1}

    # create db object using databaseHandler class for the backup and processing database
    db = dbh.dbHandler(database=k.BKP_DATABASE_NAME)

    # create a list to hold the handles to the backup processes
    tasks = []
    task_counter = 0
 
    while True:
        # Get one backup task from the queue in the database
        task = db.next_host_task()

        """	
            task={  "task_id": str,
                    "host_id": str,
                    "host_add": str,
                    "port": str,
                    "user": str,
                    "password": str}"""        
        
        # if there is a task in the database
        if task:
            # check if there is a task already running for the same host and remove it if it is the case, avoiding the creation of multiple tasks for the same host
            new_task = True
            for running_task in tasks:
                if running_task["host_id"] == task["host_id"]:
                    db.remove_host_task(task)
                    new_task = False
            
            # if it really is a new task and the total number of tasks running didn't top the capacity alloted
            if new_task and (task_counter < k.BKP_MAX_PROCESS):
                log.entry(f"Adding backup task for {task['host_add']}.")

                # TODO: #35 Remove credentials from module call to improve security
                command = ( f'bash -c '
                            f'"source {k.MINICONDA_PATH}; '
                            f'conda activate appdata; '
                            f'python3 {k.BACKUP_SINGLE_HOST_MODULE} '
                            f'task_id={task["task_id"]} ')
                
                # add task to tasks list
                task["process_handle"] = subprocess.Popen([command],
                                                        stdout=subprocess.PIPE,
                                                        stderr=subprocess.PIPE,
                                                        text=True,
                                                        shell=True)
                
                task["birth_time"] = time.time()
                task["nu_backup_error"] = 0
                tasks.append(task)                
                task_counter += 1 
                
                # TODO #33 Use status control to report running tasks instead of deleting them from the database before completion
                # remove task from database
                db.remove_host_task(task)

        # if there are tasks running
        if len(tasks) > 0:
            # loop through tasks list and remove completed tasks
            for running_task in tasks:
                # Get output from the running task, wainting 5 seconds for timeout
                try:
                    task_output, task_error = running_task["process_handle"].communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    task_output = False
                    task_error = False
                    # in case of timeout, check if the task is running for more than the alotted time and remove it if it is the case, otherwise, pass
                    if time.time() - running_task["birth_time"] > k.HOST_TASK_EXECUTION_TIMEOUT:
                        running_task["process_handle"].kill()
                        tasks.remove(running_task)
                        log.warning(f"Backup task canceled due to timeout {task['host_add']}")
                    pass
                
                # if there is output from the task, process it
                if task_output:
                    try:
                        task_dict_output = json.loads(task_output)
                        
                        # remove task from tasks list
                        tasks.remove(running_task)
                            
                        # add the list of files to the processing task list TODO: if len(task_dict_output['done_backup_list']) > 0:
                        db.add_file_task(host_id=task_dict_output['host_id'],
                                            files_list=task_dict_output['done_backup_list'])
            
                        # update backup summary status for the host_id in case of previous errors
                        task_dict_output['nu_backup_error'] = task_dict_output['nu_backup_error'] + running_task['nu_backup_error']
                        
                        # update backup summary status for the host_id
                        # ! may need fix
                        db.update_host_status(  host_id=task_dict_output['host_id'],
                                                pending_host_check=-1,
                                                pending_processing=len(task_dict_output['done_backup_list']))
                        
                        log.entry(f"Completed backup from {running_task['host_add']}")
                        
                    except json.JSONDecodeError as e:
                        log.error(f"Malformed JSON received. Dumped\n***Output: {task_output}\n***Error: {e}")
                    
                elif task_error:
                    running_task["nu_backup_error"] += 1

                    log.entry(f"Error in backup from {tasks['host_add']}. Will try again later. Error: {task_error}")
                    
                # if birth time is more than alowed time, remove task from tasks list
                execution_time = time.time() - running_task["birth_time"]
                if  execution_time > k.HOST_TASK_EXECUTION_TIMEOUT:
                    
                    # kill the process
                    running_task["process_handle"].kill()
                    
                    # remove task from tasks list
                    tasks.remove(running_task)
                        
                    # remove task from database. If there are pending backup, it will be consider in the next cycle.
                    db.remove_host_task(running_task)

                    failed_task["nu_backup_error"] = running_task["nu_backup_error"] + 1

                    # update backup summary status for the host_id
                    # ! many need fix
                    db.update_host_status(  host_id=task_dict_output['host_id'],
                                            pending_host_check=-1,
                                            host_check_error=1)
                                                
                    
                    log.warning(f"Backup task killed due to timeout for host {task['host_add']} after {execution_time/60} minutes.")
            
            log.entry(f"Wainting for {len(tasks)} backup tasks to finish. Next check in {k.HOST_TASK_EXECUTION_WAIT_TIME} seconds.")
            # wait for some task to finish or be posted
            time.sleep(k.HOST_TASK_EXECUTION_WAIT_TIME)
            
        else:
            log.entry(f"No backup task. Waiting for {k.HOST_TASK_REQUEST_WAIT_TIME/k.SECONDS_IN_MINUTE} minutes.")
            # wait for a task to be posted
            time.sleep(k.HOST_TASK_REQUEST_WAIT_TIME)

if __name__ == "__main__":
    main()