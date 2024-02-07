#!/usr/bin/python3
"""Get list of files to backup from remote host and copy them to central repository mapped to local folder, updating lists of files in the remote host and in the reference database.

    Args: Arguments passed from the command line should present in the format: "key=value"
    
    Where the possible keys are:
    
        "host_id": int, Host id in the reference database
        "host_add": str, Host address or DNS valid name
        "port": int, SSH port
        "user": str, SSH user name
        "password": str, SSH user password

    Returns:
        JSON object with the following keys:
            'host_id': task.data["host_id"]["value"],
            'nu_host_files': nu_host_files, 
            'nu_pending_backup': len(due_backup_list), 
            'nu_backup_error': nu_backup_error,
            'done_backup_list':done_backup_list}
            
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

# Define default arguments
DEFAULT_TASK_ID = 1

# define arguments as dictionary to associate each argumenbt key to a default value and associated warning messages
ARGUMENTS = {
    "task_id": {
        "set": False,
        "value": DEFAULT_TASK_ID,
        "warning": "Using default task id"
        }
    }

class HaltFlagError(Exception):
    pass

class sftp_connection():
    
    def __init__(self, host_add:str, port:str, user:str, password:str, log:sh.log) -> None:
        """Initialize the SSH client and SFTP connection to a remote host with log support."""
        
        try:
            self.log = log
            self.host_add = host_add
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(hostname=host_add, port=port, username=user, password=password)
            self.sftp = self.ssh_client.open_sftp()
        except Exception as e:
            self.log.error(f"Error initializing SSH to '{host_add}'. {str(e)}")
            raise
                
    def test(self, filename:str) -> bool:
        """Test if a file exists in the remote host

        Args:
            file (str): File name to be tested

        Returns:
            bool: True if the file exists, False otherwise
        """
        
        try:
            self.sftp.lstat(filename)
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            self.log.error(f"Error checking '{filename}' in '{self.host_add}'. {str(e)}")
            raise
    
    def touch(self, filename:str) -> None:
        """Create a file in the remote host

        Args:
            file (str): File name to be created
        """
        
        try:
            self.sftp.open(filename, 'w').close()
        except Exception as e:
            self.log.error(f"Error creating '{filename}' in '{self.host_add}'. {str(e)}")
            raise
        
    def read(self, filename, mode):
        try:
            remote_file_handle = self.sftp.open(filename, mode)
            file_content = remote_file_handle.read()
            remote_file_handle.close()
            return file_content
        except FileNotFoundError:
            self.log.error(f"File '{filename}' not found in '{self.host_add}'")
            return ""
        except Exception as e:
            self.log.error(f"Error reading '{filename}' in '{self.host_add}'. {str(e)}")
            raise
    
    def transfer(self, remote_file, local_file):
        return self.sftp.get(remote_file, local_file)
    
    def remove(self, filename):
        return self.sftp.remove(filename)
    
    def close(self):
        self.sftp.close()
        self.ssh_client.close()

def main():
    # create a warning message object
    log = sh.log()
    
    # create an argument object
    call_argument = sh.argument(log, ARGUMENTS)
    
    # parse the command line arguments
    call_argument.parse(sys.argv)
    task_id = call_argument.data["host_add"]["value"]
    
    try:
        # create db object using databaseHandler class for the backup and processing database
        db_bp = dbh.dbHandler(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        exit(1)
    
    """	
        task={  "task_id": str,
                "host_id": str,
                "host_add": str,
                "port": str,
                "user": str,
                "password": str}"""  
    
    task = db_bp.next_host_task(task_id=task_id)

    # Create a SSH client and SFTP connection to the remote host
    sftp_conn = sftp_connection(   hostname=task["host_add"],
                                port=task["port"],
                                username=task["user"],
                                password=task["passWORD"],
                                log=log)
    
    try:
        
        # * Get the remote host configuration file
        daemon_cfg_str = sftp_conn.read(k.DAEMON_CFG_FILE, 'r')
        
        # Parse the configuration file
        daemon_cfg = sh.parse_cfg(daemon_cfg_str)

        # Set the time limit for HALT_FLAG timeout control according to the HALT_TIMEOUT parameter in the remote host
        time_limit = daemon_cfg['HALT_TIMEOUT']*k.SECONDS_IN_MINUTE*k.BKP_HOST_ALLOTED_TIME_FRACTION
        
        # * Check if exist the HALT_FLAG file in the remote host
        loop_count = 0
        # If exists wait and retry each 5 minutes for 30 minutes  
        while sftp_conn.test(daemon_cfg['HALT_FLAG']):
            # If HALT_FLAG exists, wait for 5 minutes and test again
            time.sleep(k.HOST_TASK_REQUEST_WAIT_TIME)
            
            loop_count += 1
            
            if loop_count > 6:
                output = False
                message = f"HALT_FLAG file found in remote host {task.data['host_add']['value']}. Backup aborted."
                log.error(message)
                raise HaltFlagError(message)

        # store current time for HALT_FLAG timeout control
        halt_flag_time = time.time()
        
        # Create a HALT_FLAG file in the remote host
        sftp_conn.touch(daemon_cfg['HALT_FLAG'])
                        
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
            due_backup_list = due_backup_str.splitlines()
            nu_host_files = len(due_backup_list)

        # update database information
        # ! FIX AT THIS POINT
        db_bp.update_host_status(host_id
            
            task_id=task_id, nu_host_files=nu_host_files, nu_pending_backup=nu_host_files)
        db_bp.add_file_task(host_id=task["host_id"], files_list=due_backup_list)
        
        # * Peform the backup
        # initializa backup control variables
        nu_backup_error = 0
        done_backup_list = []
        done_backup_list_remote = []

        # Test if there are files to backup. Done before the loop to avoid unecessary creation of the target folder
        if nu_host_files > 0:
            target_folder = f"{k.TMP_FOLDER}/{task.data['host_add']['value']}"
            
            # make sure that the target folder do exist
            if not os.path.exists(target_folder):
                os.makedirs(target_folder)

            # use bkp_list_index to control item in the list that is under backup, skipping the ones that failed
            bkp_list_index = 0
            while len(due_backup_list) > bkp_list_index:
                
                # get the first element in the due_backup_list
                remote_file = due_backup_list[bkp_list_index]
                
                # test if remote_file exists before attempting to copy
                if _check_remote_file(sftp, remote_file, task):
                    # refresh the HALT_FLAG timeout control
                    time_since_start = time.time()-halt_flag_time
                    
                    if time_since_start > time_limit:
                        try:
                            halt_flag_file_handle = sftp.open(daemon_cfg['HALT_FLAG'], 'w')
                            halt_flag_file_handle.write(f'running backup for {time_since_start} seconds\n')
                            halt_flag_file_handle.close()
                        except Exception as e:
                            log.warning(f"Could not raise halt_flag for host {task.data['host_add']['value']}.{str(e)}")
                            pass
                    
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
