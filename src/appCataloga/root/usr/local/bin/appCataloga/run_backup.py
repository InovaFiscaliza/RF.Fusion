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

from concurrent.futures import ProcessPoolExecutor
import paramiko
import os
import time

def _host_backup(task):
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
    try:
        # Create an SSH client
        ssh_client = paramiko.SSHClient()
        
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

        def _check_remote_file(sftp, file_name, task):
            try: 
                sftp.lstat(file_name)
                return True
            except FileNotFoundError:
                return False
            except Exception as e:
                message = f"Error checking {file_name} in remote host {task['host']}. {str(e)}"
                raise Exception(message)

        # Check if exist the HALT_FLAG file in the remote host
        # If exists wait and retry each 5 minutes for 30 minutes        
        while _check_remote_file(sftp, daemon_cfg['HALT_FLAG'], task):
            # If HALT_FLAG exists, wait for 5 minutes and test again
            time.sleep(k.BKP_TASK_REQUEST_WAIT_TIME)
            
            loop_count += 1
            
            if loop_count > 6:
                output = False
                raise Exception("Timeout waiting for HALT_FLAG file")

        # store current time for HALT_FLAG timeout control
        halt_flag_time = time.time()
        
        # Create a HALT_FLAG file in the remote host
        sftp.open(daemon_cfg['HALT_FLAG'], 'w').close()
                
        try:
            # Get the list of files to backup from DUE_BACKUP file
            due_backup_file = sftp.open(daemon_cfg['DUE_BACKUP'], 'r')
            due_backup_str = due_backup_file.read()
            due_backup_file.close()
            
            # Clean the string and split the into a list of files
            due_backup_str = due_backup_str.decode(encoding='utf-8')
            due_backup_str = ''.join(due_backup_str.split('\x00'))
            due_backup_list = due_backup_str.splitlines()
            nu_host_files = len(due_backup_list)
            
            # initializa backup control variables
            nu_backup_error = 0
            done_backup_list = []
            done_backup_list_remote = []
            
        except Exception as e:
            print(f"Error reading {daemon_cfg['DUE_BACKUP']} in remote host {task['host']}. {str(e)}")
            nu_backup_error = 1
            nu_host_files = 0        

        # Test if there are files to backup                
        if nu_host_files > 0:        
            target_folder = f"{k.TARGET_FOLDER}/{task['host']}"
            
            # make sure that the target folder do exist
            if not os.path.exists(target_folder):
                os.makedirs(target_folder)

            # loop through the list of files to backup
            for remote_file in due_backup_list:
                
                # refresh the HALT_FLAG timeout control
                time_since_start = time.time()-halt_flag_time
                time_limit = daemon_cfg['HALT_TIMEOUT']*k.SECONDS_IN_MINUTE*k.ALLOTED_TIME_WINDOW
                if time_since_start > time_limit:
                    try:
                        halt_flag_file_handle = sftp.open(daemon_cfg['HALT_FLAG'], 'w')
                        halt_flag_file_handle.write(f'running backup for {time_since_start} seconds\n')
                        halt_flag_file_handle.close()
                    except:
                        print(f"Error reseting halt_flag for host {task['host']}.{str(e)}")
                        pass
                
                # Compose target file name by adding the remote file name to the target folder
                local_file = os.path.join(target_folder, os.path.basename(remote_file))
                                    
                try:
                    sftp.get(remote_file, local_file)

                    # Remove the file name from the due_backup_list
                    due_backup_list.remove(remote_file)
                    
                    # Add the file name to the done_backup remote and local lists
                    done_backup_list.append({"remote":remote_file,"local":local_file})
                    done_backup_list_remote.append(remote_file)
                    
                    print(f"File '{os.path.basename(remote_file)}' copied to '{local_file}'")
                except Exception as e:
                    print(f"Error copying {remote_file} from host {task['host']}.{str(e)}")
                    pass

            # Test if there is a BACKUP_DONE file in the remote host
            if not _check_remote_file(sftp, daemon_cfg['BACKUP_DONE'], task):
                # Create a BACKUP_DONE file in the remote host with the list of files in done_backup_list_remote
                backup_done_file = sftp.open(daemon_cfg['BACKUP_DONE'], 'w')
            else:
                # Append the list of files in done_backup_list_remote to the BACKUP_DONE file in the remote host
                backup_done_file = sftp.open(daemon_cfg['BACKUP_DONE'], 'a')
                
            backup_done_file.write('\n'.join(done_backup_list_remote))
            backup_done_file.close()
                
            # Overwrite the DUE_BACKUP file in the remote host with the list of files in due_backup_list
            due_backup_file = sftp.open(daemon_cfg['DUE_BACKUP'], 'w')
            due_backup_file.write('\n'.join(due_backup_list))
            due_backup_file.close()
            
            nu_backup_error = len(due_backup_list)/nu_host_files
        
        # Remove the HALT_FLAG file from the remote host
        sftp.remove(daemon_cfg['HALT_FLAG'])
        
        output = { 'host_id': task['host_id'],
                   'nu_host_files': nu_host_files, 
                   'nu_pending_backup': len(due_backup_list), 
                   'nu_backup_error': nu_backup_error,
                   'done_backup_list':done_backup_list}

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

def control():
    
    failed_task = { 'host_id': 0,
                    'nu_host_files': 0, 
                    'nu_pending_backup': 0, 
                    'nu_backup_error': 1}

    # create db object using databaseHandler class for the backup and processing database
    db = dbh.dbHandler(database=k.BKP_DATABASE_NAME)

    # create a list to hold the handles to the backup processes
    tasks = []

    # Use ThreadPoolExecutor to limit the number of concurrent threads
    with ProcessPoolExecutor(k.MAX_PROCESS) as executor:
        
        while True:
            # Get one backup task
            task = db.next_backup_task()
            
            # if there is a task, add it to the executor and task list
            if task:
                print(f"Adding backup task for {task['host']}.")
                
                # add task to tasks list
                _host_backup(task)
                task["process_handle"] = executor.submit(_host_backup, task)
                
            
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
                            #TODO: Test error in the result. Test why the result is not a dict
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
