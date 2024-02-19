#!/usr/bin/python3
"""Get list of files to backup from remote host and create file tasks in the control database for backup to the central repository.

    Args: Arguments passed from the command line should present in the format: "key=value"
    
    Where the possible keys are:
    
        "task_id": int, Task id in the control database

    Returns:
        None
        
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
import time

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
    sftp_conn = sftp_connection(hostname=task["host_add"],
                                port=task["port"],
                                username=task["user"],
                                password=task["passWORD"],
                                log=log)
    
    try:
        
        # * Get the remote host configuration file
        daemon_cfg_str = sftp_conn.read(k.DAEMON_CFG_FILE, 'r')
        
        # Parse the configuration file
        daemon_cfg = sh.parse_cfg(daemon_cfg_str)

        # * Check if exist the HALT_FLAG file in the remote host
        # * Wait for HALT_FLAG release
        loop_count = 0
        # If exists wait and retry each 5 minutes for 30 minutes  
        while sftp_conn.test(daemon_cfg['HALT_FLAG']):
            # If HALT_FLAG exists, wait for 5 minutes and test again
            time.sleep(k.HOST_TASK_REQUEST_WAIT_TIME/k.HALT_FLAG_CHECK_CYCLES)
            log.warning(f"HALT_FLAG file found in remote host {task.data['host_add']['value']}. Waiting {(k.HOST_TASK_REQUEST_WAIT_TIME/(k.HALT_FLAG_CHECK_CYCLES*60))} minutes.")
            loop_count += 1
            
            if loop_count > k.HALT_FLAG_CHECK_CYCLES:
                message = f"HALT_FLAG file found in remote host {task.data['host_add']['value']}. Host task aborted."
                log.error(message)
                raise HaltFlagError(message)
        
        # Create a HALT_FLAG file in the remote host
        sftp_conn.touch(daemon_cfg['HALT_FLAG'])

        # * Get the list of files to backup from DUE_BACKUP file
        due_backup_str = sftp_conn.read(filename=daemon_cfg['DUE_BACKUP'], mode='r')
        
        if due_backup_str == "":
            exit
        else:
            # Clean the string and split the into a list of files
            due_backup_str = due_backup_str.decode(encoding='utf-8')
            due_backup_str = ''.join(due_backup_str.split('\x00'))
            due_backup_list = due_backup_str.splitlines()
            
            db_bp.add_file_task(host_id=task["host_id"], task_type=db_bp.BACKUP, files_list=due_backup_list)
        
        # * Close task 
        sftp_conn.remove(filename=daemon_cfg['DUE_BACKUP'])
        
        sftp_conn.remove(filename=daemon_cfg['HALT_FLAG'])
        
        db_bp.remove_host_task(task_id=task_id)
    
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
