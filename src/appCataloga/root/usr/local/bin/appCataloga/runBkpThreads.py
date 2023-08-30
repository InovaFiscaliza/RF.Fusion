#!/usr/bin/env python
"""
Access the backup list from BKPDATA database and starts the backup process threads.
    
    Usage:
        runBackup 
            
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
import dbHandler as dbh
import appCShared as csh

import concurrent.futures
import paramiko
import os

"""
                    "task_id": task[0],
                    "host_id": task[1],
                    "host": task[2],
                    "user": task[3],
                    "password": task[4]}
"""

def ssh_copy_files(task,source_file=k.DUE_BACKUP_FILE, target_folder=k.TARGET_FOLDER):
    # Create an SSH client
    ssh_client = paramiko.SSHClient()

    try:
        # Automatically add the server's host key (this is insecure, see note below)
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the remote host
        ssh_client.connect(task('host'), username=task('user'), password=task('user'))

        # SFTP (Secure FTP) connection
        sftp = ssh_client.open_sftp()
    
        # Create target file adding the remote file name to the target folder
        target_file = os.path.join(target_folder, os.path.basename(source_file))
        
        try:
            sftp.get(source_file, target_file)
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            
        print(f"File '{os.path.basename(remote_file)}' copied to '{local_file}'")

    except paramiko.AuthenticationException:
        print("Authentication failed. Please check your credentials.")
    except paramiko.SSHException as e:
        print(f"SSH error: {str(e)}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Close the SSH client and SFTP connection
        sftp.close()
        ssh_client.close()

def main():
    # Connect to the database
    # create db object using databaseHandler class
    db = dbh.dbHandler(database=k.BKP_DATABASE_NAME)

    # create a list to hold the future objects
    tasks = []

    # Use ThreadPoolExecutor to limit the number of concurrent threads
    with concurrent.futures.ThreadPoolExecutor(k.MAX_THREADS) as executor:
        
        # Get the oldest backup task
        task = db.nextBackup()
        
        # Loop until there are no more backup tasks
        while task:
            print(f"Starting backup for {task['host']}.")
            
            # test if len(tasks) < k.MAX_THREADS
            # if true, add task to tasks list
            # else, wait for a task to finish and remove it from the list
            if len(tasks) < k.MAX_THREADS:
                # add task to tasks list
                future = executor.map(ssh_copy_files, task)
                
                task["future"] = future
                
                tasks.append(task)
                
                task = db.nextBackup()
            else:
                # loop through tasks list and remove completed tasks
                for task_end in tasks:
                    # test if furure is completed
                    if task_end["future"].done():
                        # remove task from tasks list
                        tasks.remove(task_end)
                        # remove task from database
                        db.remove_backup_task(task)
                
                        try:
                            # Get the result of the completed future
                            result = task_end["future"].result()
                            print(f"Completed backup from {task['host']}: {result}")
                        except Exception as e:
                            print(f"Error in backup from {task['host']}: {e}")
                
                        task = db.nextBackup()

if __name__ == "__main__":
    main()