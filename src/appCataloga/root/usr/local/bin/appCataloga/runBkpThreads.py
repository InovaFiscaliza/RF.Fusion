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
import dbHandler as dbh
import appCShared as csh

import concurrent.futures
import paramiko
import os
import time

import configparser

def parse_daemon_cfg(daemon_cfg=""):
    """Parse the daemon configuration file

    Args:
        daemon_cfg (str): Content from the indexerD.cfg file. Defaults to "".

    Returns:
        _dict_: _description_
    """    
    config = configparser.ConfigParser()
    config.read_srting(daemon_cfg)

    properties_dict = {}
    for section in config.sections():
        for key, value in config.items(section):
            properties_dict[key] = value

    return properties_dict

def host_backup(task):
    """Get list of files to backup from remote host and copy them to central repository mapped to local folder

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
        daemon_cfg = parse_daemon_cfg(daemon_cfg_str)
        
        loop_count = 0
        # Test if HALT_FLAG file exists in the remote host each 5 minutes for 30 minutes
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
        
        done_backup_list = []
        
        target_folder = f"k.TARGET_FOLDER/{task['host']}"
        
        # loop through the list of files to backup
        for remote_file in due_backup_list:
            # Create target file adding the remote file name to the target folder
            local_file = os.path.join(target_folder, os.path.basename(remote_file))
            
            try:
                sftp.get(remote_file, local_file)
                
                # Remove the file name from the due_backup_list
                due_backup_list.remove(remote_file)
                
                # Add the file name to the done_backup_list
                done_backup_list.append(remote_file)
                
            except Exception as e:
                print(f"Error copying file from host {task['host']}.{str(e)}")
                
            print(f"File '{os.path.basename(remote_file)}' copied to '{local_file}'")
            
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
        
        output = True

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
        return output

def main():
    # Connect to the database
    # create db object using databaseHandler class
    db = dbh.dbHandler(database=k.BKP_DATABASE_NAME)

    # create a list to hold the future objects
    tasks = []

    # Use ThreadPoolExecutor to limit the number of concurrent threads
    with concurrent.futures.ThreadPoolExecutor(k.MAX_THREADS) as executor:
        
        while True:
            
            # Get the oldest backup task
            task = db.nextBackup()

            print(f"Starting backup for {task['host']}.")
            
            # test if len(tasks) < k.MAX_THREADS
            # if true, add task to tasks list
            # else, wait for a task to finish and remove it from the list
            if len(tasks) < k.MAX_THREADS:
                # add task to tasks list
                future = executor.map(host_backup, task)
                
                task["future"] = future
                
                tasks.append(task)
            else:
                # loop through tasks list and remove completed tasks
                for task_end in tasks:
                    # test if furure is completed
                    if task_end["future"].done():

                        # remove task from tasks list
                        tasks.remove(task_end)
                        
                        # test if future is not successful or task returned False
                        if not task_end["future"].result():
                            # remove task from database
                            db.remove_backup_task(task)
                            
                            print(f"Completed backup from {task['host']}")
                        else:
                            print(f"Error in backup from {task['host']}. Will try again later.")

            
            # Get the oldest backup task                
            task = db.nextBackup()
            
            while not task:
                print("No backup task. Waiting for 5 minutes.")
                # wait for 5 minutes
                time.sleep(k.FIVE_MINUTES)
                
                task = db.nextBackup()

if __name__ == "__main__":
    main()