#!/usr/bin/env python
"""
Access the backup list from BKPDATA database and starts the backup process threads.
    
    Usage:
        runBkpThreads
            
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

# Import standard libraries.
from selectors import DefaultSelector, EVENT_READ

# Import modules for file processing 
import config as k
import db_handler as dbh
import shared as sh

import subprocess
import json

import time

def main():
    
    DECREMENT_BACKUP_TASK = {   'host_id': 0,
                                'nu_host_files': 0, 
                                'nu_pending_backup': -1, 
                                'nu_backup_error': 0}

    FAILED_TASK = { 'host_id': 0,
                    'nu_host_files': 0, 
                    'nu_pending_backup': 0, 
                    'nu_backup_error': 1}

    # create db object using databaseHandler class for the backup and processing database
    db = dbh.dbHandler(database=k.BKP_DATABASE_NAME)

    # create a list to hold the handles to the backup processes
    tasks = []
 
    while True:
        # Get one backup task from the queue in the database
        task = db.next_backup_task()

        """	
            task={  "task_id": str,
                    "host_id": str,
                    "host_add": str,
                    "port": str,
                    "user": str,
                    "password": str}"""        
        # if there is a task, add it to the executor and task list
        if task:
            
            # check if there is a task already running for the same host and remove it if it is the case, avoiding the creation of multiple tasks for the same host
            new_task = True
            for running_task in tasks:
                if running_task["host_id"] == task["host_id"]:
                    db.remove_backup_task(task)
                    new_task = False
            
            if new_task:
                print(f"Adding backup task for {task['host_add']}.")

                command = ( f'bash -c '
                            f'"source ~/miniconda3/etc/profile.d/conda.sh; '
                            f'conda activate appdata; '
                            f'python3 {k.BACKUP_SINGLE_HOST_MODULE} '
                            f'host_id={task["host_id"]} '
                            f'host_add={task["host_add"]} '
                            f'port={task["port"]} '
                            f'user={task["user"]} '
                            f'pass={task["password"]}"')
                
                # add task to tasks list
                task["process_handle"] = subprocess.Popen([command],
                                                        stdout=subprocess.PIPE,
                                                        stderr=subprocess.PIPE,
                                                        text=True,
                                                        shell=True)
                
                task["birth_time"] = time.time()
                task["nu_backup_error"] = 0
                tasks.append(task)
                
                # remove task from database
                db.remove_backup_task(task)

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
                    if time.time() - running_task["birth_time"] > k.BKP_TASK_EXECUTION_TIMEOUT:
                        running_task["process_handle"].kill()
                        tasks.remove(running_task)
                        print(f"Backup task canceled due to timeout {task['host_add']}")
                    pass
                
                # if there is output from the task, process it
                if task_output:
                    try:
                        task_dict_output = json.loads(task_output)
                        
                        # remove task from tasks list
                        tasks.remove(running_task)
                            
                        # add the list of files to the processing task list TODO: if len(task_dict_output['done_backup_list']) > 0:
                        db.add_processing_task(hostid=task_dict_output['host_id'],
                                            done_backup_list=task_dict_output['done_backup_list'])
            
                        # update backup summary status for the host_id in case of previous errors
                        task_dict_output['nu_backup_error'] = task_dict_output['nu_backup_error'] + running_task['nu_backup_error']
                        
                        # update backup summary status for the host_id
                        db.update_backup_status(task_dict_output)
                        
                        print(f"Completed backup from {running_task['host_add']}")
                        
                    except json.JSONDecodeError as e:
                        print(f'{"Status":0,"Message":"Error: Malformed JSON received. Dumped: {task_output}"}')
                    
                elif task_error:
                    running_task["nu_backup_error"] = running_task["nu_backup_error"] + 1

                    print(f"Error in backup from {task['host_add']}. Will try again later. Error: {task_error}")
                    
                    # if birth time is more than 5 minutes, remove task from tasks list
                    if time.time() - running_task["birth_time"] > k.BKP_TASK_EXECUTION_TIMEOUT:
                        
                        # remove task from tasks list
                        tasks.remove(running_task)
                            
                        # remove task from database. If there are pending backup, it will be consider in the next cycle.
                        db.remove_backup_task(running_task)

                        FAILED_TASK['nu_backup_error'] = running_task["nu_backup_error"] + 1

                        # update backup summary status for the host_id
                        db.update_backup_status(FAILED_TASK)
                        
                        print(f"Backup task canceled due to timeout {task['host_add']}")
            
            print(f"Wainting for {len(tasks)} backup tasks to finish. Next check in {k.BKP_TASK_EXECUTION_WAIT_TIME} seconds.")
            # wait for some task to finish or be posted
            time.sleep(k.BKP_TASK_EXECUTION_WAIT_TIME)
            
        else:
            print("No backup task. Waiting for {k.BKP_TASK_REQUEST_WAIT_TIME/k.SECONDS_IN_MINUTE} minutes.")
            # wait for a task to be posted
            time.sleep(k.BKP_TASK_REQUEST_WAIT_TIME)

if __name__ == "__main__":
    main()