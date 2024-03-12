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
import paramiko
import time
import signal
import inspect

# Import modules for file processing 
import config as k
import shared as sh
import db_handler as dbh

# define global variables for log and general use
log = sh.log()

process_status = {  "conn": False,
                    "halt_flag": False,
                    "running": True,}

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
            return False
        except Exception as e:
            self.log.error(f"Error reading '{filename}' in '{self.host_add}'. {str(e)}")
            raise
    
    def transfer(self, remote_file, local_file):
        try:
            return self.sftp.get(remote_file, local_file)
        except Exception as e:
            self.log.error(f"Error transferring '{remote_file}' from '{self.host_add}' to '{local_file}'. {str(e)}")
            raise
    
    def remove(self, filename):
        try:
            return self.sftp.remove(filename)
        except FileNotFoundError:
            self.log.error(f"File '{filename}' not found in '{self.host_add}'")
            return ""
        except Exception as e:
            self.log.error(f"Error removing '{filename}' in '{self.host_add}'. {str(e)}")
            raise
    
    def close(self):
        try:
            self.sftp.close()
            self.ssh_client.close()
        except Exception as e:
            self.log.error(f"Error closing connection to '{self.host_add}'. {str(e)}")
            raise
        
        
def get_daemon_config(sftp_conn,
                      task,
                      db_bp):
    global log
    
    try:
        daemon_cfg_str = sftp_conn.read(k.DAEMON_CFG_FILE, 'r')
    except FileNotFoundError:
        log.error(f"Configuration file '{k.DAEMON_CFG_FILE}' not found in remote host {task['host_uid']}({task['host_add']})")
        db_bp.update_host_status(host_id=task['host_id'], status=db_bp.HOST_WITHOUT_DAEMON)
        sftp_conn.close()
        db_bp.remove_host_task(task_id=task)
        return None
    
    # Parse the configuration file
    daemon_cfg = sh.parse_cfg(daemon_cfg_str)
    return daemon_cfg

def check_halt_flag(sftp_conn: sftp_connection, daemon_cfg: dict, task: dict, db_bp: dbh.dbHandler) -> None:
    """Check if the HALT_FLAG file exists in the remote host and wait for its release.
    
    Args:
        sftp_conn (sftp_connection): The SFTP connection object.
        daemon_cfg (dict): The daemon configuration dictionary.
        task (dict): The task dictionary containing host information.
        db_bp (dbh.dbHandler): The database handler object.
    
    Returns:
        None
    
    Raises:
        HaltFlagError: If the HALT_FLAG file is found and the maximum wait time is exceeded.
    """
    global log
    global process_status
    
    loop_count = 0
    # If HALT_FLAG exists, wait and retry each 5 minutes for 30 minutes
    while sftp_conn.test(daemon_cfg['HALT_FLAG']):
        # If HALT_FLAG exists, wait for 5 minutes and test again
        time.sleep(k.HOST_TASK_REQUEST_WAIT_TIME / k.HALT_FLAG_CHECK_CYCLES)
        log.warning(f"HALT_FLAG file found in remote host {task['host_uid']}({task['host_add']}). Waiting {(k.HOST_TASK_REQUEST_WAIT_TIME / (k.HALT_FLAG_CHECK_CYCLES * 60))} minutes.")
        loop_count += 1

        if loop_count > k.HALT_FLAG_CHECK_CYCLES:
            message = f"HALT_FLAG file found in remote host {task['host_uid']}({task['host_add']}). Host task aborted."
            log.error(message)
            sftp_conn.remove(filename=daemon_cfg['HALT_FLAG'])
            db_bp.update_host_status(host_id=task["host_id"], status=db_bp.HOST_WITH_HALT_FLAG)
            sftp_conn.close()
            db_bp.remove_host_task(task_id=task["task_id"])
            raise HaltFlagError(message)

    # Create a HALT_FLAG file in the remote host
    sftp_conn.touch(daemon_cfg['HALT_FLAG'])

    process_status["halt_flag"] = daemon_cfg['HALT_FLAG']

def process_due_backup(sftp_conn: sftp_connection, daemon_cfg: dict, task: dict, db_bp: dbh.dbHandler) -> None:
    """Process the list of files to backup from the DUE_BACKUP file.
    
    Args:
        sftp_conn (sftp_connection): The SFTP connection object.
        daemon_cfg (dict): The daemon configuration dictionary.
        task (dict): The task dictionary containing host information.
        db_bp (dbh.dbHandler): The database handler object.
    
    Returns:
        None
    """
    global log
    global process_status
    
    due_backup_str = sftp_conn.read(filename=daemon_cfg['DUE_BACKUP'], mode='r')
    
    if due_backup_str:
        # Clean the string and split it into a list of files
        due_backup_str = due_backup_str.decode(encoding='utf-8')
        due_backup_str = ''.join(due_backup_str.split('\x00'))
        due_backup_list = due_backup_str.splitlines()
        
        # Create file tasks for later handling by the file task process
        db_bp.add_file_task(host_id=task["host_id"], task_type=db_bp.BACKUP_TASK_TYPE, volume=task["host_uid"], files=due_backup_list)


def main():
    global process_status
    global log

    try:
        # create db object using databaseHandler class for the backup and processing database
        db_bp = dbh.dbHandler(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Error initializing database: {e}")
        exit(1)

    while process_status["running"]:
        
        try:
            task = db_bp.next_host_task()
            """	{   "task_id": (int),
                    "host_id": (int),
                    "host_uid": (str),
                    "host_add": (str),
                    "port": (int),
                    "user": (str),
                    "password": (str)}"""  

            # Create a SSH client and SFTP connection to the remote host
            sftp_conn = sftp_connection(hostname=task["host_add"],
                                        port=task["port"],
                                        username=task["user"],
                                        password=task["passWORD"],
                                        log=log)
            
            process_status["conn"] = sftp_conn
            
            # Get the remote host configuration file
            daemon_cfg = get_daemon_config( sftp_conn=sftp_conn,
                                            task=task,
                                            db_bp=db_bp)

            # Check and set halt flag 
            check_halt_flag(sftp_conn=sftp_conn,
                            daemon_cfg=daemon_cfg,
                            task=task,
                            db_bp=db_bp)
            
            # Get the list of files to backup from DUE_BACKUP file and create file tasks
            process_due_backup(sftp_conn=sftp_conn,
                               daemon_cfg=daemon_cfg,
                               task=task,
                               db_bp=db_bp)
            
            sftp_conn.remove(filename=daemon_cfg['DUE_BACKUP'])
            sftp_conn.remove(filename=daemon_cfg['HALT_FLAG'])
            db_bp.remove_host_task(task_id=task["task_id"])
    
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
