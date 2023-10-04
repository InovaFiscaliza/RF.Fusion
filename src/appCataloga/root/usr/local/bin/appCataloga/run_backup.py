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

import concurrent.futures
import paramiko
import os
import time

def host_backup(task):
    """Get list of files to backup from remote host and copy them to central repository mapped to local folder, updating lists of files in the remote host and in the reference database.

    Args:
        task (_dict_): {"task_id": str,
                        "host_id": int,
                        "host": str,
                        "port": int,
                        "user": str,
                        "password": str}

    Returns:
        _Bol_: True: Backup completed successfully
               False: Backup failed
    """
    
    # Create an SSH client
    ssh_client = paramiko.SSHClient()

    try:
        # Automatically add the server's host key (this is insecure)
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the remote host
        ssh_client.connect(hostname=task['host'], port=task['port'] ,username=task['user'], password=task['password'])

        # SFTP (Secure FTP) connection
        sftp = ssh_client.open_sftp()
    
        # Get the remote host configuration file
        daemon_cfg_file = sftp.open(k.DAEMON_CFG_FILE, 'r')
        daemon_cfg_str = daemon_cfg_file.read()
        daemon_cfg_file.close()
        
        # Parse the configuration file
        daemon_cfg = sh.parse_cfg(daemon_cfg_str)
        
        loop_count = 0
        # Check if exist the HALT_FLAG file in the remote host
        # If exists wait and retry each 5 minutes for 30 minutes
        while not sftp.exists(daemon_cfg['HALT_FLAG']):
            # If HALT_FLAG exists, wait for 5 minutes and test again
            time.sleep(k.FIVE_MINUTES)
            
            loop_count += 1
            
            if loop_count > 6:
                output = False
                raise Exception("Timeout waiting for HALT_FLAG file")
        
        # Create a HALT_FLAG file in the remote host
        sftp.open(daemon_cfg['HALT_FLAG'], 'w').close()
        
        # Get the list of files to backup from DUE_BACKUP file
        due_backup_file = sftp.open(k.DUE_BACKUP_FILE, 'r')
        due_backup_str = due_backup_file.read()
        due_backup_file.close()
        
        # Split the file list into a list of files
        due_backup_list = due_backup_str.splitlines()
        
        nu_host_files = len(due_backup_list)
        
        done_backup_list = []
        target_folder = f"k.TARGET_FOLDER/{task['host']}"
        # loop through the list of files to backup
        for remote_file in due_backup_list:
            # Create target file name by adding the remote file name to the target folder
            local_file = os.path.join(target_folder, os.path.basename(remote_file))
            
            try:
                sftp.get(remote_file, local_file)

                # Remove the file name from the due_backup_list
                due_backup_list.remove(remote_file)
                
                # Add the file name to the done_backup_list
                done_backup_list.append(remote_file)
                
                print(f"File '{os.path.basename(remote_file)}' copied to '{local_file}'")
            except Exception as e:
                print(f"Error copying {remote_file} from host {task['host']}.{str(e)}")

        # Test if there is a BACKUP_DONE file in the remote host
        if not sftp.exists(daemon_cfg['BACKUP_DONE']):
            # Create a BACKUP_DONE file in the remote host with the list of files in done_backup_list
            backup_done_file = sftp.open(daemon_cfg['BACKUP_DONE'], 'w')
        else:
            # Append the list of files in done_backup_list to the BACKUP_DONE file in the remote host
            backup_done_file = sftp.open(daemon_cfg['BACKUP_DONE'], 'a')
        
        backup_done_file.write('\n'.join(done_backup_list))
        backup_done_file.close()
            
        # Overwrite the DUE_BACKUP file in the remote host with the list of files in due_backup_list
        due_backup_file = sftp.open(k.DUE_BACKUP_FILE, 'w')
        due_backup_file.write('\n'.join(due_backup_list))
        due_backup_file.close()
            
        # Remove the HALT_FLAG file from the remote host
        sftp.remove(daemon_cfg['HALT_FLAG'])
        
        output = { 'host_id': task['host_id'],
                   'nu_host_files': nu_host_files, 
                   'nu_pending_backup': len(due_backup_list), 
                   'nu_backup_error': len(due_backup_list)/nu_host_files}

    except paramiko.AuthenticationException:
        print("Authentication failed. Please check your credentials.")
        output = False
    except paramiko.SSHException as e:
        print(f"SSH error: {str(e)}")
        output = False
    except Exception as e:
        print(f"Unmapped error occurred: {str(e)}")
        output = False
    finally:
        # Close the SSH client and SFTP connection
        sftp.close()
        ssh_client.close()
        return output

def main():
    
    FAILED_TASK = { 'host_id': running_task['host_id'],
                    'nu_host_files': 0, 
                    'nu_pending_backup': 0, 
                    'nu_backup_error': 1}

    # Connect to the database
    # create db object using databaseHandler class
    db = dbh.dbHandler(database=k.BKP_DATABASE_NAME)

    # Get one backup task to start
    task = db.nextBackup()

    # create a list to hold the future objects
    tasks = []

    # Use ThreadPoolExecutor to limit the number of concurrent threads
    with concurrent.futures.ThreadPoolExecutor(k.MAX_THREADS) as executor:
        
        while True:
            
            print(f"Starting backup for {task['host']}.")
            
            # test if len(tasks) < k.MAX_THREADS
            # if true, add task to tasks list
            # else, wait for a task to finish and remove it from the list
            if len(tasks) < k.MAX_THREADS:
                # add task to tasks list
                task["map_itarator"] = executor.map(host_backup, task)
                
                tasks.append(task)
                
            else:
                # loop through tasks list and remove completed tasks
                for running_task in tasks:
                    # test if the runnning_task is completed
                    if running_task["map_itarator"].done():

                        try:
                            # get the result from the map_itarator
                            task_status = running_task["map_itarator"].result()
                            
                            # remove task from tasks list
                            tasks.remove(running_task)
                            
                            # If running task was successful (result not empty or False)
                            if task_status:
                                
                                # remove task from database
                                db.remove_backup_task(task)
                                                                
                                print(f"Completed backup from {task['host']}")
                            else:
                                task_status = FAILED_TASK

                                print(f"Error in backup from {task['host']}. Will try again later.")

                        # except error in running_task
                        except running_task["map_itarator"].exception() as e:
                            # if the running task has an error, set task_status to False
                            task_status = FAILED_TASK
                            print(f"Error in backup from {task['host']}. Will try again later. {str(e)}")
                        
                        # any other exception
                        except Exception as e:
                            # if the running task has an error, set task_status to False
                            task_status = FAILED_TASK
                            print(f"Error in backup from {task['host']}. Will try again later. {str(e)}")
                        finally:
                            # update backup summary status for the host_id
                            db.update_host_backup_status(task_status)
            
            # Get the next backup task
            task = db.nextBackup()
            
            while not task:
                print("No backup task. Waiting for 5 minutes.")
                # wait for 5 minutes
                time.sleep(k.FIVE_MINUTES)
                
                # try again to get a task
                task = db.nextBackup()

if __name__ == "__main__":
    main()