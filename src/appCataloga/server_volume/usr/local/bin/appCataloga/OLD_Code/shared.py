#!/usr/bin/env python
"""
Shared functions for appCataloga scripts
"""

import sys,os

CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga"))
sys.path.append(CONFIG_PATH)

import os
import paramiko
from datetime import datetime
from typing import Tuple
import time
import stat
import json
from db.dbHandlerBKP import dbHandlerBKP
import config as k


class font:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


# Class to compose warning messages
NO_MSG = "none"


class log:
    """Class to compose warning messages

    Returns:
        void: Variable warning_msg is updated with the new warning message
    """

    def __init__(
        self,
        verbose=k.LOG_VERBOSE,
        target_screen=k.LOG_TARGET_SCREEN,
        target_file=k.LOG_TARGET_FILE,
        log_file_name=k.LOG_FILE,
    ):
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

        self.pid = os.getpid()
        self.pname = os.path.basename(sys.argv[0]).split(".")[0]
        self.last_msg = ""

        if isinstance(verbose, dict):
            self.verbose = verbose

        elif isinstance(verbose, bool):
            self.verbose = {"log": verbose, "warning": verbose, "error": verbose}
        else:
            self.verbose = {"log": False, "warning": False, "error": False}
            self.warning(f"Invalid verbose value '{verbose}'. Using default 'False'")

        if target_file:
            try:
                now = datetime.now()
                date_time = now.strftime("%Y/%m/%d %H:%M:%S")
                message = f"{date_time} | p.{self.pid} | p.{self.pname} | Log started\n"

                self.log_file = open(log_file_name, "a")
                self.log_file.write(message)
                self.log_file.close()
                self.target_file = True
            except Exception as e:
                self.target_file = False
                self.warning(
                    f"Invalid log_file_name value '{log_file_name}'. Disabling file logging. Error: {str(e)}"
                )

    def _verbose_output(self):
        datetime = self.last_update.strftime("%Y/%m/%d %H:%M:%S")

        if self.target_file:
            message = f"{datetime} | p.{self.pid} | p.{self.pname} | {self.last_msg}\n"
            self.log_file = open(self.log_file_name, "a")
            self.log_file.write(message)
            self.log_file.close()

        if self.target_screen:
            message = f"{font.OKGREEN}{datetime} | p.{self.pid} | {self.pname} |{font.ENDC} {self.last_msg}"
            print(message)

    def entry(self, new_entry):
        self.last_update = datetime.now()
        self.last_msg = new_entry
        self.log_msg.append((self.last_update, self.pid, self.pname, self.last_msg))

        if self.verbose["log"]:
            self._verbose_output()

    def warning(self, new_entry):
        self.last_update = datetime.now()
        self.last_msg = new_entry
        self.warning_msg.append((self.last_update, self.pid, self.pname, self.last_msg))

        if self.verbose["warning"]:
            self._verbose_output()

    def error(self, new_entry):
        self.last_update = datetime.now()
        self.last_msg = new_entry
        self.error_msg.append((self.last_update, self.pid, self.pname, self.last_msg))

        if self.verbose["error"]:
            self._verbose_output()

    def dump_log(self):
        message = ", ".join([str(elem[1]) for elem in self.log_msg])

        return message

    def dump_warning(self):
        message = ", ".join([str(elem[1]) for elem in self.warning_msg])

        return message

    def dump_error(self):
        message = ", ".join([str(elem[1]) for elem in self.error_msg])
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

    config_str = cfg_data.decode(encoding="utf-8")

    config_list = config_str.splitlines()

    config_dict = {}
    while line_number < len(config_list):
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
                if line[0] == "[" and line[-1] == "]":
                    key = line[1:-1]
                    if root_level:
                        config_dict[key], line_number = parse_cfg(
                            cfg_data=cfg_data, root_level=False, line_number=line_number
                        )
                    else:
                        return (config_dict, line_number - 1)
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


class sftpConnection:
    def __init__(
        self,
        host_uid: str,
        host_add: str,
        port: str,
        user: str,
        password: str,
        log: log,
    ) -> None:
        """Initialize the SSH client and SFTP connection to a remote host with log support."""

        try:
            self.log = log
            self.host_uid = host_uid
            self.host_add = host_add
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(
                hostname=host_add, port=port, username=user, password=password
            )
            self.sftp = self.ssh_client.open_sftp()
        except Exception as e:
            self.log.error(
                f"Error initializing SSH to '{self.host_uid}'({self.host_add}). {str(e)}"
            )
            raise

    def test(self, filename: str) -> bool:
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
            self.log.error(
                f"Error checking '{filename}' in '{self.host_uid}'({self.host_add}). {str(e)}"
            )
            raise

    def touch(self, filename: str) -> None:
        """Create a file in the remote host

        Args:
            file (str): File name to be created
        """

        try:
            self.sftp.open(filename, "w").close()
        except Exception as e:
            self.log.error(
                f"Error creating '{filename}' in '{self.host_uid}'({self.host_add}). {str(e)}"
            )
            raise

    def read(self, filename: str, mode: str) -> str:
        try:
            remote_file_handle = self.sftp.open(filename, mode)
            file_content = remote_file_handle.read()
            remote_file_handle.close()
            return file_content
        except FileNotFoundError:
            self.log.error(
                f"File '{filename}' not found in '{self.host_uid}'({self.host_add})"
            )
            return False
        except Exception as e:
            self.log.error(
                f"Error reading '{filename}' in '{self.host_uid}'({self.host_add}). {str(e)}"
            )
            raise

    def transfer(self, remote_file: str, local_file: str) -> None:
        try:
            return self.sftp.get(remote_file, local_file)
        except Exception as e:
            self.log.error(
                f"Error transferring '{remote_file}' from '{self.host_uid}'({self.host_add}) to '{local_file}'. {str(e)}"
            )
            raise

    def remove(self, filename: str) -> None:
        try:
            return self.sftp.remove(filename)
        except FileNotFoundError:
            self.log.error(
                f"File '{filename}' not found in '{self.host_uid}'({self.host_add})"
            )
            return ""
        except Exception as e:
            self.log.error(
                f"Error removing '{filename}' in '{self.host_uid}'({self.host_add}). {str(e)}"
            )
            raise

    def close(self) -> None:
        try:
            self.sftp.close()
            self.ssh_client.close()
        except Exception as e:
            self.log.error(
                f"Error closing connection to '{self.host_uid}'({self.host_add}). {str(e)}"
            )
            raise
    
    def get_metadata(self, filename: str) -> dict:
        """Get metadata of a remote file via SFTP.

        Args:
            filename (str): Remote file path

        Returns:
            dict: {NA_FILE, NA_PATH, NA_EXTENSION, VL_FILE_SIZE_KB, DT_FILE_CREATED, DT_FILE_MODIFIED, DT_FILE_ACCESSED, NA_OWNER, NA_GROUP, NA_PERMISSIONS}
        """
        try:
            attrs = self.sftp.stat(filename)

            # Size em KB
            size_kb = attrs.st_size // 1024 if attrs.st_size else 0

            # Datetime
            dt_modified = datetime.fromtimestamp(attrs.st_mtime)
            dt_accessed = datetime.fromtimestamp(attrs.st_atime)

            # Creation date obtained from remote node
            dt_created = None
            try:
                _, stdout, _ = self.ssh_client.exec_command(f"stat -c %W {filename}")
                created_ts = int(stdout.read().decode().strip())
                
                # If Unix supports date created in file system
                if created_ts > 0:
                    dt_created = datetime.fromtimestamp(created_ts)
                else:
                    dt_created = dt_modified
            except Exception:
                pass  # Ignore - not supported

            # Permissions in format rwx
            permissions = stat.filemode(attrs.st_mode)

            # File extension
            _, ext = os.path.splitext(filename)

            return {
                "NA_FILE": os.path.basename(filename),
                "NA_PATH": os.path.dirname(filename),
                "NA_EXTENSION": ext,
                "VL_FILE_SIZE_KB": size_kb,
                "DT_FILE_CREATED": dt_created,
                "DT_FILE_MODIFIED": dt_modified,
                "DT_FILE_ACCESSED": dt_accessed,
                "NA_OWNER": attrs.st_uid,
                "NA_GROUP": attrs.st_gid,
                "NA_PERMISSIONS": permissions,
            }

        except FileNotFoundError:
            self.log.error(
                f"File '{filename}' not found in '{self.host_uid}'({self.host_add})"
            )
            return {}
        except Exception as e:
            self.log.error(
                f"Error retrieving metadata for '{filename}' in '{self.host_uid}'({self.host_add}). {str(e)}"
            )
            raise
        
    def get_metadata_files(self, file_list: list) -> dict:
        """Get metadata of a remote file via SFTP.

        Args:
            filename (list): List of remote files mapped

        Returns:
            list with dict: {NA_FILE, NA_PATH, NA_EXTENSION, VL_FILE_SIZE_KB, DT_FILE_CREATED, DT_FILE_MODIFIED, DT_FILE_ACCESSED, NA_OWNER, NA_GROUP, NA_PERMISSIONS}
        """
        try:
            output_list = []
            
            for filename in file_list:
                attrs = self.sftp.stat(filename)

                # Size em KB
                size_kb = attrs.st_size // 1024 if attrs.st_size else 0

                # Datetime
                dt_modified = datetime.fromtimestamp(attrs.st_mtime)
                dt_accessed = datetime.fromtimestamp(attrs.st_atime)

                # Creation date obtained from remote node
                dt_created = None
                try:
                    _, stdout, _ = self.ssh_client.exec_command(f"stat -c %W {filename}")
                    created_ts = int(stdout.read().decode().strip())
                    
                    # If Unix supports date created in file system
                    if created_ts > 0:
                        dt_created = datetime.fromtimestamp(created_ts)
                    else:
                        dt_created = dt_modified
                except Exception:
                    pass  # Ignore - not supported

                # Permissions in format rwx
                permissions = stat.filemode(attrs.st_mode)

                # File extension
                _, ext = os.path.splitext(filename)

                file_metadata = {
                    "NA_FILE": os.path.basename(filename),
                    "NA_PATH": os.path.dirname(filename),
                    "NA_EXTENSION": ext,
                    "VL_FILE_SIZE_KB": size_kb,
                    "DT_FILE_CREATED": dt_created,
                    "DT_FILE_MODIFIED": dt_modified,
                    "DT_FILE_ACCESSED": dt_accessed,
                    "NA_OWNER": attrs.st_uid,
                    "NA_GROUP": attrs.st_gid,
                    "NA_PERMISSIONS": permissions,
                }
                
                # Append metadata information for each file in a List
                output_list.append(file_metadata)
            
            return output_list

        except FileNotFoundError:
            self.log.error(
                f"File '{filename}' not found in '{self.host_uid}'({self.host_add})"
            )
            return {}
        except Exception as e:
            self.log.error(
                f"Error retrieving metadata for '{filename}' in '{self.host_uid}'({self.host_add}). {str(e)}"
            )
            raise

class hostDaemon:
    """Class to handle the remote host daemon tasks"""

    def __init__(
        self,
        sftp_conn: sftpConnection,
        db_bp: dbHandlerBKP,
        host_id: int,
        log: log,
        task_id: int = None,
        task_dict: dict = None,
    ) -> None:
        self.sftp_conn = sftp_conn
        self.db_bp = db_bp
        self.log = log

        self.task_id = task_id
        self.task_dict = task_dict

        self.config = None
        self.time_limit = None
        self.halt_flag_set_time = None

        self.host = db_bp.host_read_access(host_id)

    def _handle_failed_task(
        self,
        task_id: int,
        task_type: int,
        remove_failed_task: bool,
        message: str = None,
    ) -> None:
        match task_type:
            case k.HOST_TASK_TYPE:
                if remove_failed_task:
                    self.db_bp.host_task_delete(task_id=task_id)
                else:
                    self.db_bp.host_task_update(
                        task_id=task_id, status=self.db_bp.TASK_ERROR, message=message
                    )

                self.db_bp.host_update(host_id=self.host["host_id"], host_check_error=1)

            case k.FILE_TASK_BACKUP_TYPE:
                if remove_failed_task:
                    self.db_bp.file_task_delete(task_id=task_id)
                else:
                    self.db_bp.file_task_update(
                        task_id=task_id, status=self.db_bp.TASK_ERROR, message=message
                    )

                self.db_bp.host_update(
                    host_id=self.host["host_id"], pending_backup=-1, backup_error=1
                )

            case k.FILE_TASK_PROCESS_TYPE:
                if remove_failed_task:
                    self.db_bp.file_task_delete(task_id=task_id)
                else:
                    self.db_bp.file_task_update(
                        task_id=task_id, status=self.db_bp.TASK_ERROR, message=message
                    )

                self.db_bp.host_update(
                    host_id=self.host["host_id"],
                    pending_processing=-1,
                    processing_error=1,
                )

            case _:
                self.log.error(f"Invalid task type '{task_type}'")

    def get_config(self, task_type: int, remove_failed_task: bool = False) -> dict:
        """Get the remote host configuration file into config class variable

        Args:
            remove_failed_task (bool, optional): Remove the task from the database if the halt_flag is set. Defaults to False, suspend task.

        Raises:
            FileNotFoundError: If the configuration file is not found in the remote host
        """

        try:
            daemon_cfg_str = self.sftp_conn.read(k.DAEMON_CFG_FILE, "r")

            if not daemon_cfg_str:
                raise FileNotFoundError

            self.config = parse_cfg(daemon_cfg_str)

            # Set the time limit for HALT_FLAG timeout control according to the HALT_TIMEOUT parameter in the remote host
            self.time_limit = (
                self.config["HALT_TIMEOUT"]
                * k.SECONDS_IN_MINUTE
                * k.BKP_HOST_ALLOTED_TIME_FRACTION
            )

            return True
        except FileNotFoundError:
            self.log.error(
                f"Configuration file '{k.DAEMON_CFG_FILE}' not found in remote host with id {self.host['host_id']}"
            )

            self.db_bp.host_update(
                host_id=self.host["host_id"], status=self.db_bp.HOST_WITHOUT_DAEMON
            )
            self.sftp_conn.close()

            task_handle_arguments = {
                "task_type": task_type,
                "remove_failed_task": remove_failed_task,
                "message": "Configuration file not found in remote host",
            }

            if self.task_id:
                self._handle_failed_task(task_id=self.task_id, **task_handle_arguments)
            if self.task_dict:
                for task_id in self.task_dict.keys():
                    self._handle_failed_task(task_id=task_id, **task_handle_arguments)

            return False

    def get_halt_flag(self, task_type: int, remove_failed_task: bool = False) -> bool:
        """Set the halt_flag in the remote host if it is not previously set by another process.
            Wait for release before continuing using config parameters
            Remove or suspend the task if the halt_flag can not be set.

        Args:
            remove_failed_task (bool, optional): Remove the task if True. Defaults to False, suspend task.

        Returns:
            status (bool): True if the HALT_FLAG file raised, False otherwise.
        """

        loop_count = 0
        time_to_wait = k.HOST_TASK_REQUEST_WAIT_TIME / k.HALT_FLAG_CHECK_CYCLES
        # If HALT_FLAG exists, wait and retry each 5 minutes for 30 minutes
        while self.sftp_conn.test(self.config["HALT_FLAG"]):
            # If HALT_FLAG exists, wait for 5 minutes and test again
            time.sleep(time_to_wait)
            self.log.warning(
                f"HALT_FLAG file found in remote host {self.host['host_uid']}({self.host['host_add']}). Waiting {(time_to_wait / 60.0)} minutes."
            )
            loop_count += 1

            if loop_count > k.HALT_FLAG_CHECK_CYCLES:
                message = f"HALT_FLAG file found in remote host {self.host['host_uid']}({self.host['host_add']}). Task aborted."
                self.log.error(message)
                self.sftp_conn.close()
                self.db_bp.host_update(
                    host_id=self.host["host_id"],
                    reset=True,
                    status=self.db_bp.HOST_WITH_HALT_FLAG,
                )

                task_handle_arguments = {
                    "task_type": task_type,
                    "remove_failed_task": remove_failed_task,
                    "message": "Halt flag set in remote host",
                }
                if self.task_id:
                    self._handle_failed_task(
                        task_id=self.task_id, **task_handle_arguments
                    )

                if self.task_dict:
                    for task_id in self.task_dict.keys():
                        self._handle_failed_task(
                            task_id=task_id, **task_handle_arguments
                        )

                return False

        # Create a HALT_FLAG file in the remote host
        self.sftp_conn.touch(self.config["HALT_FLAG"])

        self.halt_flag_set_time = time.time()

        return True

    def reset_halt_flag(self) -> None:
        """Reset the halt_flag in the remote host if reached the time limit.

        Returns:
            None
        """
        # refresh the HALT_FLAG timeout control
        time_since_start = time.time() - self.halt_flag_set_time

        if time_since_start > self.time_limit:
            try:
                halt_flag_file_handle = self.sftp_conn.sftp.open(
                    self.config["HALT_FLAG"], "w"
                )
                halt_flag_file_handle.write(
                    f"running backup for {time_since_start/60} minutes\n"
                )
                halt_flag_file_handle.close()
            except Exception as e:
                self.log.warning(
                    f"Could not raise halt_flag for host {self.host['host_id']}.{str(e)}"
                )
                pass

    def set_backup_done(self, filename: str) -> None:
        """Insert into BACKUP_DONE file the name of the file that was backed up.

        Args:
            filename (str): file name that was backed up
        """

        try:
            backup_done_handle = self.sftp_conn.sftp.open(
                self.config["BACKUP_DONE"], "a"
            )
            backup_done_handle.write(f"{filename}\n")
            backup_done_handle.close()
        except Exception as e:
            self.log.warning(
                f"Could not write to BACKUP_DONE file for host {self.host['host_id']}.{str(e)}"
            )
            pass

    def close_host(self, remove_due_backup: bool = False) -> None:
        """Reset the halt_flag in the remote host if it is set by this process.

        Args:
            remove_due_backup (bool, optional): Remove the DUE_BACKUP file. Defaults to False.

        Returns:
            None
        """

        if remove_due_backup:
            self.sftp_conn.remove(filename=self.config["DUE_BACKUP"])

        self.sftp_conn.remove(filename=self.config["HALT_FLAG"])
        self.sftp_conn.close()

        if self.task_id:
            self.db_bp.host_task_delete(task_id=self.task_id)


def parse_filter(filter_raw: str, log=None) -> dict:
    """Safely parse, validate, and normalize a filter JSON string.

    Supported modes:
        - "NONE"   : No filtering applied.
        - "ALL"    : Select all available entries.
        - "RANGE"  : Filter between start_date and end_date.
        - "LAST"   : Filter by last_n_files and optional extension.
        - "FILE"   : Select a specific file by name.

    Args:
        filter_raw (str): Raw JSON string containing filter parameters.
        log (object, optional): Logger instance with .entry() or .warning() methods.

    Returns:
        dict: Normalized and JSON-serializable filter dictionary.
    """
    def _safe_parse_date(date_str):
        """Convert string to datetime, returning None if invalid."""
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace("Z", ""))
        except Exception:
            return None

    # Default fallback
    none_filter = {
        "mode": "NONE",
        "start_date": None,
        "end_date": None,
        "last_n_files": None,
        "extension": None,
        "file_name": None,
    }

    if not filter_raw:
        return json.loads(json.dumps(none_filter))

    try:
        f = json.loads(filter_raw)
        mode = str(f.get("mode", "NONE")).upper().strip()

        # =============================
        # MODE: NONE
        # =============================
        if mode == "NONE":
            result = none_filter

        # =============================
        # MODE: ALL
        # =============================
        elif mode == "ALL":
            result = {
                "mode": "ALL",
                "start_date": None,
                "end_date": None,
                "last_n_files": None,
                "extension": None,
                "file_name": None,
            }

        # =============================
        # MODE: RANGE
        # =============================
        elif mode == "RANGE":
            start = _safe_parse_date(f.get("start_date"))
            end = _safe_parse_date(f.get("end_date"))

            # Auto-fix inverted ranges
            if start and end and start > end:
                if log:
                    log.entry(f"[parse_filter] Swapping inverted date range: {start} > {end}")
                start, end = end, start

            result = {
                "mode": "RANGE",
                "start_date": start.isoformat() if start else None,
                "end_date": end.isoformat() if end else None,
                "last_n_files": None,
                "extension": None,
                "file_name": None,
            }

        # =============================
        # MODE: LAST
        # =============================
        elif mode == "LAST":
            result = {
                "mode": "LAST",
                "start_date": None,
                "end_date": None,
                "last_n_files": int(f.get("last_n_files", 0)) or None,
                "extension": str(f.get("extension", "")).strip() or None,
                "file_name": None,
            }

        # =============================
        # MODE: FILE
        # =============================
        elif mode == "FILE":
            file_name = str(f.get("file_name", "")).strip()
            if not file_name:
                raise ValueError("Missing 'file_name' in FILE mode filter")

            # Sanitize file name to avoid directory traversal or injection
            safe_name = os.path.basename(file_name)

            result = {
                "mode": "FILE",
                "start_date": None,
                "end_date": None,
                "last_n_files": None,
                "extension": None,
                "file_name": safe_name,
            }

        # =============================
        # INVALID MODE
        # =============================
        else:
            if log:
                log.entry(f"[parse_filter] Invalid filter mode '{mode}', reverting to NONE")
            result = none_filter

    except (json.JSONDecodeError, TypeError, ValueError) as e:
        if log:
            log.entry(f"[parse_filter] Error parsing filter ({e}) - input={filter_raw}")
        result = none_filter

    # JSON round-trip ensures the dict is serializable
    return json.loads(json.dumps(result))


def parse_socket_message(data: str, peername: Tuple[str, int], log=None) -> dict:
    """Parse a socket message into structured fields, with safe fallback on errors.

    Expected format:
        <MSG_TYPE> <HOST_ID> <HOST_UID> <HOST_ADDRESS> <HOST_PORT> <USER> <PASS> <FILTER_ARGS>

    Args:
        data (str): Decoded socket payload.
        peername (Tuple[str,int]): (ip, port) from client_socket.getpeername().
        log (object, optional): Logger with .entry() method for error reporting.

    Returns:
        dict: A structured dictionary with the following keys:
              {
                "peer":      {"ip": str, "port": int},   # client connection info
                "command":   str | None,                 # message type (e.g., "backup")
                "host_id":   int | None,                 # numeric host identifier
                "host_uid":  str | None,                 # unique host name/UID
                "host_addr": str | None,                 # host IP/DNS address
                "host_port": int | None,                 # host SSH port
                "user":      str | None,                 # login username
                "password":  str | None,                 # login password
                "filter":    JSON                        # json object filter e.g '{"mode":"RANGE","start_date":"2025-09-20","end_date":"2025-09-28"}'
              }

              On parsing failure, all fields except "peer" and "filter"
              are set to None, and "filter" falls back to NONE_FILTER.
    """
    peer_ip, peer_port = peername

    try:
        parts = data.strip().split(" ", 7)

        msg_type   = parts[0]
        host_id    = int(parts[1])
        host_uid   = parts[2]
        host_addr  = parts[3]
        host_port  = int(parts[4])
        user       = parts[5]
        password   = parts[6]
        filter_raw = parts[7] if len(parts) > 7 else None

        filter_dict = parse_filter(filter_raw, log)

        return {
            "peer": {"ip": peer_ip, "port": peer_port},
            "command": msg_type,
            "host_id": host_id,
            "host_uid": host_uid,
            "host_addr": host_addr,
            "host_port": host_port,
            "user": user,
            "password": password,
            "filter": filter_dict
        }

    except Exception as e:
        if log:
            log.entry(f"Failed to parse socket message: {e} - raw data={data}")
        return {
            "peer": {"ip": peer_ip, "port": peer_port},
            "command": None,
            "host_id": None,
            "host_uid": None,
            "host_addr": None,
            "host_port": None,
            "user": None,
            "password": None,
            "filter": k.NONE_FILTER
        }


#-------------------------------------------------------------
# Public API
#-------------------------------------------------------------
def init_host_context(host: dict, log):
    """
    Initialize SFTP connection and hostDaemon context for a given host.

    This prepares the remote session for controlled backup execution.
    The caller is responsible for closing both `daemon` and `sftp_conn`
    after use (typically via try/finally).

    Args:
        host (dict): Host configuration record from database.
        tasks (dict): FILE_TASK mapping for this host.
        log: Shared logger instance.

    Returns:
        Tuple[sh.sftpConnection, sh.hostDaemon]: Active SFTP session and daemon.
    """
    sftp_conn = sftpConnection(
        host_uid=host["host_uid"],
        host_add=host["host_add"],
        port=host["port"],
        user=host["user"],
        password=host["password"],
        log=log,
    )

    daemon = hostDaemon(
        sftp_conn=sftp_conn,
        host_id=host["host_id"],
        task_dict=host["file_tasks"],
        log=log,
    )

    return sftp_conn, daemon
