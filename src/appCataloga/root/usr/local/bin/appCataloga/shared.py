#!/usr/bin/env python
"""
Shared functions for appCataloga scripts
"""
import sys
sys.path.append('/etc/appCataloga')
import os
import paramiko
from datetime import datetime
import time

import db_handler as dbh
import config as k

class font:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    
# Class to compose warning messages
NO_MSG = "none"

class log:
    """Class to compose warning messages

    Returns:
        void: Variable warning_msg is updated with the new warning message
    """
    
    def __init__(self,verbose=k.LOG_VERBOSE,
                 target_screen=k.LOG_TARGET_SCREEN, 
                 target_file=k.LOG_TARGET_FILE, 
                 log_file_name = k.LOG_FILE):
        
        """Initialize log object

        Args:
            verbose (bool, optional): Set verbose level for debug, warning and error. Defaults to LOG_VERBOSE in config file.
            target_screen (bool, optional): Set the output target to screen. Defaults to False.
            target_file (bool, optional): Set the output target to file. Defaults to False.
            log_file_name (str, optional): Set the output file name. Defaults to LOG_FILE in config file.
        """        
        
        self.target_screen = target_screen
        self.target_file = target_file
        self.log_file_name = log_file_name
        
        self.log_msg = []
        self.warning_msg = []
        self.error_msg = []
        
        self.pid =  os.getpid()
                
        if isinstance(verbose,dict):
            self.verbose = verbose
            
        elif isinstance(verbose,bool):
            self.verbose = {"log":verbose,
                            "warning":verbose,
                            "error":verbose}
        else:
            self.verbose = {"log":False,
                            "warning":False,
                            "error":False}
            self.warning(f"Invalid verbose value '{verbose}'. Using default 'False'")
        
        if target_file:
            try:
                now = datetime.now()
                date_time = now.strftime("%Y/%m/%d %H:%M:%S")
                message = f"{date_time} | p.{self.pid} | Log started\n"
                
                self.log_file = open(log_file_name, "a")
                self.log_file.write(message)
                self.log_file.close()
                self.target_file = True
            except Exception as e:
                self.target_file = False
                self.warning(f"Invalid log_file_name value '{log_file_name}'. Disabling file logging. Error: {str(e)}")
        
    def entry(self, new_entry):
        
        now = datetime.now()
        self.log_msg.append((now,self.pid,new_entry))
        
        if self.verbose["log"]:
            date_time = now.strftime("%Y/%m/%d %H:%M:%S")
            if self.target_file:
                message = f"{date_time} | p.{self.pid} | {new_entry}\n"
                self.log_file = open(self.log_file_name, "a")
                self.log_file.write(message)
                self.log_file.close()
        
            if self.target_screen:
                message = f"{font.OKGREEN}{date_time} | p.{self.pid} | {font.ENDC}{new_entry}"
                print(message)

    def warning(self, new_entry):
        
        now = datetime.now()
        self.warning_msg.append((now,self.pid,new_entry))
        
        if self.verbose["warning"]:
            date_time = now.strftime("%Y/%m/%d %H:%M:%S")
            if self.target_file:
                message = f"{date_time} | p.{self.pid} | {new_entry}\n"
                self.log_file = open(self.log_file_name, "a")
                self.log_file.write(message)
                self.log_file.close()
        
            if self.target_screen:
                message = f"{font.WARNING}{date_time} | p.{self.pid} | {font.ENDC}{new_entry}"
                print(new_entry)
                            
    def error(self, new_entry):
        
        now = datetime.now()
        self.error_msg.append((now,self.pid,new_entry))
        
        if self.verbose["error"]:
            date_time = now.strftime("%Y/%m/%d %H:%M:%S")
            if self.target_file:
                message = f"{date_time} | p.{self.pid} | {new_entry}\n"
                self.log_file = open(self.log_file_name, "a")
                self.log_file.write(message)
                self.log_file.close()
        
            if self.target_screen:
                message = f"{font.FAIL}{date_time} | p.{self.pid} | {font.ENDC}{new_entry}"
                print(message)

    def dump_log(self):
        message = ', '.join([str(elem[1]) for elem in self.log_msg])
        
        return message

    def dump_warning(self):
        message = ', '.join([str(elem[1]) for elem in self.warning_msg])
        
        return message
        
    def dump_error(self):
        message = ', '.join([str(elem[1]) for elem in self.error_msg])
        return message
    
def parse_cfg(cfg_data="", root_level=True, line_number=0):
    """Parse shell like configuration file into dictionary


    Args:
        cfg_data (str): Content from the configuration file.
        e.g. indexerD.cfg
        Defaults to "".
        root_level (bool, optional): Flag to indicate if the call is in the root level. Defaults to True.
        line_number (int, optional): Line number where the parsing should start. Defaults to 0.

    Returns:
        dict: shell variables returned as pairs of key and value
        int: line number where the parsing stopped if call was not in the root_level
    """    
    
    config_str=cfg_data.decode(encoding='utf-8')
    
    config_list=config_str.splitlines()
    
    config_dict = {}
    while line_number<len(config_list):
        line = config_list[line_number]
        line_number += 1
        # handle standard lines with variable value assignation
        try:
            key, value = line.split("=")
            try:
                # try to convert value to float
                config_dict[key] = float(value)
            except ValueError:
                # if not possible to use float, keep value as string
                config_dict[key] = value
        # handle section lines, where there is no "=" sign and split will fail
        except ValueError:
            
            try:
                if line[0]=="[" and line[-1]=="]":
                    key = line[1:-1]
                    if root_level:
                        config_dict[key], line_number = parse_cfg(cfg_data=cfg_data, root_level=False, line_number=line_number)
                    else:
                        return (config_dict, line_number-1)
                else:
                    # ignore lines that do not assign values or define sections
                    pass
            except IndexError:
                # ignore empty lines
                pass
    
    # return according to the call level
    if root_level:
        return config_dict
    else:
        return (config_dict, line_number)

class argument:
    """Class to parse and store command-line arguments"""
    
    def __init__(self, log_input=log(), arg_input={}) -> None:
        self.log = log_input
        self.data = arg_input
        
    def parse(self, sys_arg=[]):
        """Get command-line arguments and parse into a request to the server"""
        
        # loop through the arguments list and set the value of the argument if it is present in the command line
        for i in range(1, len(sys_arg)):
            arg_in = sys_arg[i].split("=")
            if arg_in[0] in self.data.keys():
                # Get the data type from sef.data value
                data_type = type(self.data[arg_in[0]]["value"])
                
                # Set the argument value and set the "set" flag to True
                self.data[arg_in[0]]["value"] = data_type(arg_in[1])
                self.data[arg_in[0]]["set"] = True
            else:
                self.log.warning(f"Argument '{arg_in[0]}' not recognized, ignoring it")
            
        # loop through the arguments list and compose a warning message for each argument that was not set
        for arg in self.data.keys():
            if not self.data[arg]["set"]:
                self.log.warning(self.data[arg]["warning"])
                
class sftpConnection():
    
    def __init__(   self,
                    host_uid:str,
                    host_add:str,
                    port:str,
                    user:str,
                    password:str,
                    log:log) -> None:
        """Initialize the SSH client and SFTP connection to a remote host with log support."""
        
        try:
            self.log = log
            self.host_uid = host_uid
            self.host_add = host_add
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(hostname=host_add, port=port, username=user, password=password)
            self.sftp = self.ssh_client.open_sftp()
        except Exception as e:
            self.log.error(f"Error initializing SSH to '{self.host_uid}'({self.host_add}). {str(e)}")
            raise
                
    def test(   self,
                filename:str) -> bool:
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
            self.log.error(f"Error checking '{filename}' in '{self.host_uid}'({self.host_add}). {str(e)}")
            raise
    
    def touch(  self,
                filename:str) -> None:
        """Create a file in the remote host

        Args:
            file (str): File name to be created
        """
        
        try:
            self.sftp.open(filename, 'w').close()
        except Exception as e:
            self.log.error(f"Error creating '{filename}' in '{self.host_uid}'({self.host_add}). {str(e)}")
            raise
        
    def read(   self,
                filename:str,
                mode:str) -> str:
        try:
            remote_file_handle = self.sftp.open(filename, mode)
            file_content = remote_file_handle.read()
            remote_file_handle.close()
            return file_content
        except FileNotFoundError:
            self.log.error(f"File '{filename}' not found in '{self.host_uid}'({self.host_add})")
            return False
        except Exception as e:
            self.log.error(f"Error reading '{filename}' in '{self.host_uid}'({self.host_add}). {str(e)}")
            raise
    
    def transfer(   self,
                    remote_file:str,
                    local_file:str) -> None:
        try:
            return self.sftp.get(remote_file, local_file)
        except Exception as e:
            self.log.error(f"Error transferring '{remote_file}' from '{self.host_uid}'({self.host_add}) to '{local_file}'. {str(e)}")
            raise
    
    def remove( self,
                filename:str) -> None:
        try:
            return self.sftp.remove(filename)
        except FileNotFoundError:
            self.log.error(f"File '{filename}' not found in '{self.host_uid}'({self.host_add})")
            return ""
        except Exception as e:
            self.log.error(f"Error removing '{filename}' in '{self.host_uid}'({self.host_add}). {str(e)}")
            raise
    
    def close(self) -> None:
        try:
            self.sftp.close()
            self.ssh_client.close()
        except Exception as e:
            self.log.error(f"Error closing connection to '{self.host_uid}'({self.host_add}). {str(e)}")
            raise

class hostDaemon():
    """Class to handle the remote host daemon tasks
    """
    
    def __init__(   self,
                    sftp_conn:sftpConnection,
                    db_bp: dbh.dbHandler,
                    host_id: int,
                    log:log,
                    task_id: int = None,
                    task_dict:dict = None) -> None:
        
        self.sftp_conn = sftp_conn
        self.db_bp = db_bp
        self.log = log
        
        self.task_id = task_id
        self.task_dict = task_dict
        
        self.config = None
        self.time_limit = None
        self.halt_flag_set_time = None
        
        self.host = db_bp.get_host(host_id)

    def _handle_failed_task(self, task_id:int,
                            remove_failed_task:bool,
                            message:str = None) -> None:
        
        if remove_failed_task:
            self.db_bp.remove_host_task(task_id=task_id)
        else:
            self.db_bp.file_task_update(task_id=task_id, status=self.db_bp.TASK_FAILED, message=message)
    
    def get_config( self,
                    remove_failed_task:bool = False) -> dict:
        """Get the remote host configuration file into config class variable

        Args:
            remove_failed_task (bool, optional): Remove the task from the database if the halt_flag is set. Defaults to False, suspend task.

        Raises:
            FileNotFoundError: If the configuration file is not found in the remote host
        """
                
        try:
            daemon_cfg_str = self.sftp_conn.read(k.DAEMON_CFG_FILE, 'r')
            
            self.config = parse_cfg(daemon_cfg_str)
            
            # Set the time limit for HALT_FLAG timeout control according to the HALT_TIMEOUT parameter in the remote host
            self.time_limit = self.config['HALT_TIMEOUT']*k.SECONDS_IN_MINUTE*k.BKP_HOST_ALLOTED_TIME_FRACTION

        except FileNotFoundError:
            self.log.error(f"Configuration file '{k.DAEMON_CFG_FILE}' not found in remote host with id {self.host['host_id']}")
            
            self.db_bp.update_host_status(host_id=self.host["host_id"], status=self.db_bp.HOST_WITHOUT_DAEMON)
            self.sftp_conn.close()
            
            task_handle_arguments = {   "remove_failed_task":remove_failed_task,
                                        "message":"Configuration file not found in remote host"}
            if self.task_id:
                self._handle_failed_task(task_id=self.task_id, **task_handle_arguments)
            if self.task_dict:
                for task_id in self.task_dict.keys():
                    self._handle_failed_task(task_id=task_id, **task_handle_arguments)
            
            raise FileNotFoundError

    def get_halt_flag(  self,
                        remove_failed_task:bool = False) -> bool:
        """Set the halt_flag in the remote host if it is not previously set by another process. 
            Wait for release before continuing using config parameters
            Remove or suspend the task if the halt_flag can not be set.
        
        Args:
            remove_failed_task (bool, optional): Remove the task if True. Defaults to False, suspend task.
        
        Returns:
            status (bool): True if the HALT_FLAG file raised, False otherwise.
        """
        
        loop_count = 0
        # If HALT_FLAG exists, wait and retry each 5 minutes for 30 minutes
        while self.sftp_conn.test(self.config['HALT_FLAG']):
            # If HALT_FLAG exists, wait for 5 minutes and test again
            time.sleep(k.HOST_TASK_REQUEST_WAIT_TIME / k.HALT_FLAG_CHECK_CYCLES)
            self.log.warning(f"HALT_FLAG file found in remote host {self.host['host_uid']}({self.host['host_add']}). Waiting {(k.HOST_TASK_REQUEST_WAIT_TIME / (k.HALT_FLAG_CHECK_CYCLES * 60))} minutes.")
            loop_count += 1

            if loop_count > k.HALT_FLAG_CHECK_CYCLES:
                message = f"HALT_FLAG file found in remote host {self.host['host_uid']}({self.host['host_add']}). Task aborted."
                self.log.error(message)
                self.sftp_conn.close()
                self.db_bp.update_host_status(host_id=self.host["host_id"], status=self.db_bp.HOST_WITH_HALT_FLAG)
                
                task_handle_arguments = {   "remove_failed_task":remove_failed_task,
                                            "message":"Halt flag set in remote host"}
                if self.task_id:
                    self._handle_failed_task(task_id=self.task_id, **task_handle_arguments)
                    
                if self.task_dict:
                    for task_id in self.task_dict.keys():
                        self._handle_failed_task(task_id=task_id, **task_handle_arguments)

                return False

        # Create a HALT_FLAG file in the remote host
        self.sftp_conn.touch(self.config['HALT_FLAG'])
        
        self.halt_flag_set_time = time.time()

        return True


    def reset_halt_flag(self) -> None:
        """Reset the halt_flag in the remote host if reached the time limit.

        Returns:
            None
        """
        # refresh the HALT_FLAG timeout control
        time_since_start = time.time()-self.halt_flag_set_time
        
        if time_since_start > self.time_limit:
            try:
                halt_flag_file_handle = self.sftp_conn.sftp.open(self.config['HALT_FLAG'], 'w')
                halt_flag_file_handle.write(f'running backup for {time_since_start/60} minutes\n')
                halt_flag_file_handle.close()
            except Exception as e:
                self.log.warning(f"Could not raise halt_flag for host {self.host['host_id']}.{str(e)}")
                pass

    def set_backup_done(self,
                        filename:str) -> None:
        """Insert into BACKUP_DONE file the name of the file that was backed up.

        Args:
            filename (str): file name that was backed up
        """
        
        try:
            backup_done_handle = self.sftp_conn.sftp.open(self.config['BACKUP_DONE'], 'a')
            backup_done_handle.write(f'{filename}\n')
            backup_done_handle.close()
        except Exception as e:
            self.log.warning(f"Could not write to BACKUP_DONE file for host {self.host['host_id']}.{str(e)}")
            pass

    def close_host(self, remove_due_backup:bool = False) -> None:
        """Reset the halt_flag in the remote host if it is set by this process.
        
        Args:
            remove_due_backup (bool, optional): Remove the DUE_BACKUP file. Defaults to False.
        
        Returns:
            None
        """
        
        if remove_due_backup:
            self.sftp_conn.remove(filename=self.config['DUE_BACKUP'])
            
        self.sftp_conn.remove(filename=self.config['HALT_FLAG'])
        self.sftp_conn.close()
        
        if self.task_id:
            self.db_bp.remove_host_task(task_id=self.task_id)
