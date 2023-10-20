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
import paramiko
import os
import time

def main():
    
    failed_task = { 'host_id': 0,
                    'nu_host_files': 0, 
                    'nu_pending_backup': 0, 
                    'nu_backup_error': 1}

    # create db object using databaseHandler class for the backup and processing database
    db = dbh.dbHandler(database=k.BKP_DATABASE_NAME)

    # create a list to hold the handles to the backup processes
    tasks = []
 
    while True:
        # Get one backup task
        task = db.next_backup_task()
        
        # if there is a task, add it to the executor and task list
        if task:
            print(f"Adding backup task for {task['host']}.")

#! FIX CALL TO BACKUP SINGLE HOST
            # add task to tasks list
            _host_backup(task)
            task["process_handle"] = backup_process = subprocess.Popen(["_host_backup", "task"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            tasks.append(task)

        # if there are tasks running
        if len(tasks) > 0:
            # loop through tasks list and remove completed tasks
            for running_task in tasks:
                # test if the runnning_task is completed
                if running_task["process_handle"].done():

                    try:
                        # get the result from the process_handle
                        task_status = running_task["process_handle"].result()
                        
                        # remove task from tasks list
                        tasks.remove(running_task)
                        
                        # If running task was successful (result not empty or False)
                        if task_status:
                            
                            # remove task from database. If there are pending backup, it will be consider in the next cycle.
                            db.remove_backup_task(task)
                                                            
                            # add the list of files to the processing task list
                            db.add_processing(hostid=task_status['host_id'],
                                                done_backup_list=task_status['done_backup_list'])
                            
                            print(f"Completed backup from {task['host']}")
                        else:
                            failed_task['host_id'] = task_status['host_id']
                            task_status = failed_task

                            print(f"Error in backup from {task['host']}. Will try again later.")

                    # except error in running_task
                    except running_task["process_handle"].exception() as e:
                        # if the running task has an error, set task_status to False
                        failed_task['host_id'] = task_status['host_id']
                        task_status = failed_task
                        print(f"Error in backup from {task['host']}. Will try again later. {str(e)}")
                    
                    # any other exception
                    except Exception as e:
                        # if the running task has an error, set task_status to False
                        failed_task['host_id'] = task_status['host_id']
                        task_status = failed_task
                        print(f"Error in backup from {task['host']}. Will try again later. {str(e)}")
                    finally:
                        # update backup summary status for the host_id
                        db.update_host_backup_status(task_status)
            
            print(f"Wainting for {len(tasks)} backup tasks to finish. Next check in {k.BKP_TASK_EXECUTION_WAIT_TIME} seconds.")
            # wait for some task to finish or be posted
            time.sleep(k.BKP_TASK_EXECUTION_WAIT_TIME)
            
        else:
            print("No backup task. Waiting for {k.BKP_TASK_REQUEST_WAIT_TIME/k.SECONDS_IN_MINUTE} minutes.")
            # wait for a task to be posted
            time.sleep(k.BKP_TASK_REQUEST_WAIT_TIME)

if __name__ == "__main__":
    main()