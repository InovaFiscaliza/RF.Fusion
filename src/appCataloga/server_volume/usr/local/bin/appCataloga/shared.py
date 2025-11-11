#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
shared.py - Core shared module for appCataloga microservices.

This module centralizes common utilities and abstractions used by all
microservices in the appCataloga ecosystem. It provides consistent interfaces
for logging, remote file access via SFTP, host task management, and filter
parsing/validation.

Key Classes:
    - log: Lightweight logger for both console and file outputs.
    - sftpConnection: Simplified wrapper for Paramiko SSH/SFTP operations.
    - hostDaemon: Remote task orchestration helper for controlled filesystem access.
    - Filter: Unified handler for parsing and validating filter definitions.

All public names and method signatures from the legacy version are preserved.
All comments and docstrings follow Google Style and are written in English.
"""
from __future__ import annotations
import sys
import os
import stat
import time
import json
import paramiko
import posixpath
import fnmatch
import stat
import random
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional, Union
from enum import Enum



# ---------------------------------------------------------------------
# Config import path (as in original code). We keep the behavior so the
# module remains drop-in compatible with existing deployments.
# ---------------------------------------------------------------------
CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga")
)
if CONFIG_PATH not in sys.path:
    sys.path.append(CONFIG_PATH)

import config as k  # noqa: E402  (must be available at runtime)


# ---------------------------------------------------------------------
# Constants (preserved)
# ---------------------------------------------------------------------
NO_MSG = "none"


# =====================================================================
# HaltFlag class to manage objects
# =====================================================================
class HaltFlagState(Enum):
    NO_FLAG = 0
    OWN_FLAG = 1
    FOREIGN_FLAG = 2
    STALE_FLAG = 3  # flag existe, mas está vencido (tempo excedido)



# =====================================================================
# Log
# =====================================================================
class log:
    """Simple logger with optional stdout and file outputs.

    This implementation keeps a file handle open to reduce I/O overhead,
    but preserves the original public methods and behavior.

    Attributes:
        target_screen (bool): If True, prints log entries to stdout.
        target_file (bool): If True, appends log entries to a file.
        log_file_name (str): Target log file path when target_file=True.
        last_update (datetime): Timestamp of last log entry.
        last_msg (str): Last message written.
        error_msg (List[Tuple[int, str]]): Collected error messages (epoch, msg).
        pid (int): Current process ID.
        pname (str): Current process name (argv[0]).
        verbose (dict): Verbosity flags for 'log', 'warning', 'error'.
    """

    def __init__(
        self,
        verbose: Union[bool, Dict[str, bool]] = getattr(k, "LOG_VERBOSE", False),
        target_screen: bool = getattr(k, "LOG_TARGET_SCREEN", False),
        target_file: bool = getattr(k, "LOG_TARGET_FILE", False),
        log_file_name: str = getattr(k, "LOG_FILE", "/tmp/appCataloga.log"),
    ) -> None:
        """Initialize logger.

        Args:
            verbose (bool|dict): Global or per-level verbosity configuration.
                If bool, applies to all levels. If dict, supports keys
                'log', 'warning', 'error' with boolean values.
            target_screen (bool): If True, print log to stdout.
            target_file (bool): If True, append log to a file.
            log_file_name (str): Path to the log file (when target_file=True).

        Returns:
            None
        """
        self.target_screen = target_screen
        self.target_file = target_file
        self.log_file_name = log_file_name
        self.last_update = datetime.now()
        self.last_msg = ""
        self.error_msg: List[Tuple[int, str]] = []
        self.pid = os.getpid()
        self.pname = os.path.basename(sys.argv[0]) if sys.argv else "app"

        if isinstance(verbose, dict):
            self.verbose = {
                "log": bool(verbose.get("log", False)),
                "warning": bool(verbose.get("warning", False)),
                "error": bool(verbose.get("error", False)),
            }
        elif isinstance(verbose, bool):
            self.verbose = {"log": verbose, "warning": verbose, "error": verbose}
        else:
            self.verbose = {"log": False, "warning": False, "error": False}
            self.warning(f"Invalid verbose value '{verbose}'. Using False.")

        # Open the log file once, if requested
        self._fh = None
        if self.target_file:
            try:
                self._fh = open(self.log_file_name, "a", buffering=1, encoding="utf-8")
                self._write("Log started")
            except Exception as e:
                self._fh = None
                self.target_file = False
                self.warning(
                    f"Invalid log_file_name '{self.log_file_name}'. "
                    f"Disabling file logging. Error: {e}"
                )

    # ---------------------------- internal helpers ----------------------------
    def _write(self, msg: str) -> None:
        """Write a formatted message to the configured targets.

        Args:
            msg (str): Message to write (not yet formatted with timestamp).

        Returns:
            None
        """
        self.last_update = datetime.now()
        self.last_msg = msg
        timestamp = self.last_update.strftime("%Y/%m/%d %H:%M:%S")
        line = f"{timestamp} | p.{self.pid} | {self.pname} | {msg}\n"

        if self.target_file and self._fh:
            try:
                self._fh.write(line)
                self._fh.flush()
            except Exception:
                # Fallback to stdout if file write fails
                self.target_file = False
                print(line, end="")

        if self.target_screen:
            print(line, end="")

    # ------------------------------- public API --------------------------------
    def entry(self, message: str) -> None:
        """Write a standard (info) log entry when enabled by verbosity.

        Args:
            message (str): Log message.

        Returns:
            None
        """
        if self.verbose.get("log", False):
            self._write(str(message))

    def warning(self, message: str) -> None:
        """Write a warning log entry when enabled by verbosity.

        Args:
            message (str): Warning message.

        Returns:
            None
        """
        if self.verbose.get("warning", False):
            self._write(f"[WARN] {message}")

    def error(self, message: str) -> None:
        """Write an error log entry (always stored in memory; printed if enabled).

        Args:
            message (str): Error message.

        Returns:
            None
        """
        self.error_msg.append((int(time.time()), str(message)))
        if self.verbose.get("error", True):  # error defaults to True
            self._write(f"[ERROR] {message}")

    def dump_error(self) -> str:
        """Return all collected error messages as a single string.

        Args:
            None

        Returns:
            str: Concatenated error messages separated by commas.
        """
        return ", ".join([m for _, m in self.error_msg])

    def __del__(self) -> None:
        """Close the log file handle upon garbage collection (best-effort).

        Args:
            None

        Returns:
            None
        """
        try:
            if self._fh:
                self._fh.close()
        except Exception:
            pass


# =====================================================================
# Configuration parser (public name preserved)
# =====================================================================
def parse_cfg(cfg_data: Union[str, bytes] = "", root_level: bool = True, line_number: int = 0):
    """Parse a shell-like configuration content into a dictionary.

    The expected format is KEY=VALUE per line, with optional comments
    starting with '#'. Lines ending with backslashes (multiline) are
    not supported in this simplified parser and will be treated as-is.

    Args:
        cfg_data (str|bytes): Content from a configuration file
            (e.g., indexerD.cfg) either as text or bytes.
        root_level (bool): Whether this is the root-level parse call.
            When False, also returns the final line number processed.
        line_number (int): Starting line number for parsing (used in
            nested parsing scenarios).

    Returns:
        tuple[dict, int] | tuple[dict, 0]:
            - If root_level is True, returns (config_dict, 0).
            - If root_level is False, returns (config_dict, end_line_number).
    """
    # Normalize to string
    if isinstance(cfg_data, bytes):
        config_str = cfg_data.decode("utf-8", errors="ignore")
    else:
        config_str = str(cfg_data)

    config_list = config_str.splitlines()

    config_dict: Dict[str, str] = {}
    while line_number < len(config_list):
        line = config_list[line_number]
        line_number += 1

        # Strip comments and whitespace
        if "#" in line:
            line = line.split("#", 1)[0]
        line = line.strip()

        # Empty lines
        if not line:
            continue

        # Simple KEY=VALUE
        if "=" in line and not line.endswith("\\"):
            kkey, vval = line.split("=", 1)
            config_dict[kkey.strip()] = vval.strip()
            continue

        # Multiline or unsupported patterns can be extended here as needed.

    return (config_dict, line_number) if not root_level else (config_dict, 0)


# =====================================================================
# SFTP Connection
# =====================================================================
class sftpConnection:
    """Light wrapper over Paramiko SSH/SFTP with convenience methods.

    Attributes:
        log (log): Logger instance.
        host_uid (str): Host unique identifier.
        host_addr (str): Host address (hostname or IP).
        port (int): SSH port.
        user (str): SSH username.
        ssh_client (paramiko.SSHClient): Underlying SSH client.
        sftp (paramiko.SFTPClient): Underlying SFTP client.
    """

    def __init__(
        self,
        host_uid: str,
        host_addr: str,
        port: int,
        user: str,
        password: str,
        log: log,
    ) -> None:
        """Initialize SSH and SFTP connections.

        Args:
            host_uid (str): Unique host identifier (for logs).
            host_addr (str): Hostname/IP address of the remote host.
            port (int): SSH port number.
            user (str): SSH user name.
            password (str): SSH password.
            log (log): Logger instance to be used.

        Returns:
            None

        Raises:
            Exception: When connection to remote host fails.
        """
        self.log = log
        self.host_uid = host_uid
        self.host_addr = host_addr
        self.port = port
        self.user = user

        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(hostname=host_addr, port=port, username=user, password=password)
            self.sftp = self.ssh_client.open_sftp()
        except Exception as e:
            self.log.error(
                f"Error initializing SSH to '{self.host_uid}'({self.host_addr}). {e}"
            )
            raise

    # ------------------------------- file ops ---------------------------------
    def test(self, filename: str) -> bool:
        """Check remote file existence.

        Args:
            filename (str): Absolute path of the remote file.

        Returns:
            bool: True if the file exists; False if not.

        Raises:
            Exception: For errors unrelated to missing file (e.g., permissions).
        """
        try:
            self.sftp.lstat(filename)
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            self.log.error(f"Error checking '{filename}' in '{self.host_uid}'({self.host_addr}). {e}")
            raise

    def touch(self, filename: str) -> None:
        """Create a zero-length file remotely (like 'touch').

        Args:
            filename (str): Absolute path of the remote file to create.

        Returns:
            None

        Raises:
            Exception: When file creation fails for reasons other than existence.
        """
        try:
            if self.test(filename):
                return
            with self.sftp.open(filename, "w"):
                pass
        except Exception as e:
            self.log.error(f"Error touching '{filename}' in '{self.host_uid}'({self.host_addr}). {e}")
            raise

    def append(self, filename: str, content: str) -> None:
        """Append text content to a remote file.

        Args:
            filename (str): Absolute path of the remote file to append to.
            content (str): Text content to append.

        Returns:
            None

        Raises:
            Exception: On SFTP I/O errors.
        """
        try:
            with self.sftp.open(filename, "a", encoding="utf-8") as f:
                f.write(content)
        except Exception as e:
            self.log.error(f"Error appending to '{filename}' in '{self.host_uid}'({self.host_addr}). {e}")
            raise

    def write(self, filename: str, content: str) -> None:
        """Write (overwrite) text content to a remote file.

        Args:
            filename (str): Absolute path of the remote file to write to.
            content (str): Text content to write (overwrites existing data).

        Returns:
            None

        Raises:
            Exception: On SFTP I/O errors.
        """
        try:
            with self.sftp.open(filename, "w") as f:
                f.write(content)
        except Exception as e:
            self.log.error(f"Error writing '{filename}' in '{self.host_uid}'({self.host_addr}). {e}")
            raise

    def read(self, filename: str, mode: str = "r") -> Union[str, bytes]:
        """Read content from a remote file (text or binary).

        Args:
            filename (str): Absolute path of the remote file to read.
            mode (str): 'r' for text (UTF-8), 'rb' for bytes. Defaults to 'r'.

        Returns:
            str|bytes: The file content. Returns empty string/bytes if not found.

        Raises:
            Exception: On SFTP I/O errors other than FileNotFoundError.
        """
        try:
            if "b" in mode:
                with self.sftp.open(filename, "rb") as f:
                    return f.read()
            else:
                with self.sftp.open(filename, "r") as f:
                    return f.read()
        except FileNotFoundError:
            self.log.error(f"File '{filename}' not found in '{self.host_uid}'({self.host_addr})")
            return "" if "b" not in mode else b""
        except Exception as e:
            self.log.error(f"Error reading '{filename}' in '{self.host_uid}'({self.host_addr}). {e}")
            raise

    def read_cookie_list(self, filename: str) -> List[str]:
        """Read a 'list cookie' file (one item per line) from remote host.

        Args:
            filename (str): Absolute remote path of the cookie file.

        Returns:
            list[str]: List of non-empty, stripped lines. Empty list if missing.
        """
        try:
            data = self.read(filename, "r")
            if not data:
                return []
            return [ln.strip() for ln in str(data).splitlines() if ln.strip()]
        except Exception as e:
            self.log.error(f"Error reading cookie list '{filename}' from '{self.host_uid}'. {e}")
            return []

    def write_cookie_list(self, filename: str, lines: List[str]) -> None:
        """Write a 'list cookie' file (one item per line) to remote host.

        Args:
            filename (str): Absolute remote path of the cookie file.
            lines (list[str]): Lines to write; will be joined by newline.

        Returns:
            None

        Raises:
            Exception: On SFTP I/O errors.
        """
        try:
            content = "\n".join(lines) + "\n" if lines else ""
            self.write(filename, content)
        except Exception as e:
            self.log.error(f"Error writing cookie list '{filename}' to '{self.host_uid}'. {e}")
            raise

    def transfer(self, remote_file: str, local_file: str) -> None:
        """Download a remote file to a local path.

        Args:
            remote_file (str): Absolute remote path of the file.
            local_file (str): Local filesystem destination path.

        Returns:
            None

        Raises:
            Exception: On SFTP I/O errors (e.g., permissions, network).
        """
        try:
            self.sftp.get(remote_file, local_file)
        except Exception as e:
            self.log.error(
                f"Error transferring '{remote_file}' from '{self.host_uid}'({self.host_addr}) to '{local_file}'. {e}"
            )
            raise

    def remove(self, filename: str) -> None:
        """Remove a remote file if it exists.

        Args:
            filename (str): Absolute path of the remote file to delete.

        Returns:
            None

        Raises:
            Exception: On SFTP I/O errors (other than not-found).
        """
        try:
            self.sftp.remove(filename)
        except FileNotFoundError:
            self.log.error(f"File '{filename}' not found in '{self.host_uid}'({self.host_addr})")
        except Exception as e:
            self.log.error(f"Error removing '{filename}' in '{self.host_uid}'({self.host_addr}). {e}")
            raise

    def close(self) -> None:
        """Close SFTP and SSH sessions (best-effort).

        Args:
            None

        Returns:
            None
        """
        try:
            self.sftp.close()
            self.ssh_client.close()
        except Exception as e:
            self.log.error(f"Error closing SFTP/SSH for '{self.host_uid}'({self.host_add}). {e}")

    # ----------------------------- metadata helpers ---------------------------
    def _stat_to_metadata(self, attrs: Any, filename: str, created_ts: Optional[int]) -> Dict[str, Any]:
        """Build a metadata mapping from SFTPAttributes and filename.

        Args:
            attrs (paramiko.SFTPAttributes): Attributes returned by SFTP.stat().
            filename (str): Absolute remote path used for deriving name/path/ext.
            created_ts (int|None): Creation time as epoch seconds if available,
                otherwise None. When None or invalid, the modified time is reused.

        Returns:
            dict: Metadata fields expected by downstream consumers, including:
                - NA_FILE (str): Basename.
                - NA_PATH (str): Directory path.
                - NA_EXTENSION (str): File extension (with dot).
                - VL_FILE_SIZE_KB (int): Size rounded down to KB.
                - DT_FILE_CREATED (datetime): Creation timestamp.
                - DT_FILE_MODIFIED (datetime): Last modification timestamp.
                - DT_FILE_ACCESSED (datetime): Last access timestamp.
                - NA_OWNER (int): Numeric user owner.
                - NA_GROUP (int): Numeric group owner.
                - NA_PERMISSIONS (str): Symbolic permissions, e.g., '-rw-r--r--'.
        """
        size_kb = (attrs.st_size // 1024) if getattr(attrs, "st_size", 0) else 0

        dt_modified = datetime.fromtimestamp(getattr(attrs, "st_mtime", 0) or int(time.time()))
        dt_accessed = datetime.fromtimestamp(getattr(attrs, "st_atime", 0) or int(time.time()))
        dt_created = dt_modified
        try:
            if created_ts and created_ts > 0:
                dt_created = datetime.fromtimestamp(created_ts)
        except Exception:
            pass

        permissions = stat.filemode(getattr(attrs, "st_mode", 0o100644))
        _, ext = os.path.splitext(filename)

        return {
            "NA_FILE": os.path.basename(filename),
            "NA_PATH": os.path.dirname(filename),
            "NA_FULL_PATH": f"{os.path.dirname(filename)}/{os.path.basename(filename)}",
            "NA_EXTENSION": ext,
            "VL_FILE_SIZE_KB": size_kb,
            "DT_FILE_CREATED": dt_created,
            "DT_FILE_MODIFIED": dt_modified,
            "DT_FILE_ACCESSED": dt_accessed,
            "NA_OWNER": getattr(attrs, "st_uid", 0),
            "NA_GROUP": getattr(attrs, "st_gid", 0),
            "NA_PERMISSIONS": permissions,
        }

    def _get_metadata_batch(self, file_list: List[str]) -> List[Dict[str, Any]]:
        """Collect metadata for multiple files using a unified implementation.

        Args:
            file_list (list[str]): Absolute remote file paths to be inspected.

        Returns:
            list[dict]: A list of metadata dicts in the same order as inputs.
                If a file is not found, an empty dict is placed in that index.

        Raises:
            Exception: When unexpected SFTP or SSH errors occur.
        """
        results: List[Dict[str, Any]] = []
        for filename in file_list:
            try:
                attrs = self.sftp.stat(filename)

                # Try to read creation time using remote 'stat -c %W' if available
                created_ts: Optional[int] = None
                try:
                    cmd = f"stat -c %W {filename}"
                    _, stdout, _ = self.ssh_client.exec_command(cmd, get_pty=False)
                    out = stdout.read().decode("utf-8", errors="ignore").strip()
                    created_ts = int(out) if out.isdigit() else None
                except Exception:
                    created_ts = None

                results.append(self._stat_to_metadata(attrs, filename, created_ts))

            except FileNotFoundError:
                self.log.error(
                    f"File '{filename}' not found in '{self.host_uid}'({self.host_addr})"
                )
                results.append({})
            except Exception as e:
                self.log.error(
                    f"Error retrieving metadata for '{filename}' in '{self.host_uid}'({self.host_addr}). {e}"
                )
                raise
        return results

    def sftp_find_files(self, remote_path: str, pattern: str, recursive: bool = True) -> List[str]:
        """
        Search for files on a remote host via SFTP that match a given wildcard pattern.

        This method traverses the specified remote directory and returns the names of files
        that match the provided pattern (supports UNIX-style wildcards such as '*' and '?').
        When `recursive` is True, it will also explore subdirectories.

        Args:
            remote_path (str): The base remote directory path to search in.
            pattern (str): The filename pattern to match (e.g., "data*", "*.txt").
            recursive (bool, optional): Whether to search subdirectories recursively.
                Defaults to True.

        Returns:
            List[str]: A list containing the names of files that match the pattern.
                    Directory paths are excluded; only filenames are returned.
        """
        matched_files = []

        try:
            entries = self.sftp.listdir_attr(remote_path)
        except Exception as e:
            print(f"[WARN] Cannot list {remote_path}: {e}")
            return matched_files

        for entry in entries:
            full_path = os.path.join(remote_path, entry.filename).replace("\\", "/")

            # Check if entry is a directory
            if stat.S_ISDIR(entry.st_mode):
                if recursive:
                    try:
                        # Recursive search in subdirectory
                        matched_files.extend(
                            self.sftp_find_files(full_path, pattern, recursive=True)
                        )
                    except Exception as e:
                        print(f"[WARN] Skipped directory {full_path}: {e}")
            else:
                # Check if filename matches the given pattern
                if fnmatch.fnmatch(entry.filename, pattern):
                    matched_files.append(full_path)  

        return matched_files

# =====================================================================
# hostDaemon (public API preserved)
# =====================================================================
class hostDaemon:
    """High-level remote orchestration helper for controlled host access.

    This class centralizes operations involving remote hosts (via SFTP/SSH)
    and coordination of distributed tasks, but without any direct dependency
    on database handlers. Instead, it emits structured commands through a
    pluggable dispatcher callback.

    Attributes:
        sftp_conn (sftpConnection): Active SFTP/SSH connection to the remote host.
        log (log): Logger instance for diagnostics.
        host_id (int): Identifier of the remote host in the system.
        task_id (int|None): Optional task ID being processed.
        task_dict (dict|None): Optional batch of tasks indexed by ID.
        dispatcher (callable|None): Optional external callback responsible for handling
            DB operations or side effects (e.g., task updates, deletions, etc.).
        config (dict|None): Configuration parameters loaded from the remote node.
        halt_flag_set_time (int|None): Timestamp when HALT_FLAG was written.
    """

    def __init__(
        self,
        sftp_conn: "sftpConnection",
        log: "log",
    ):
        """Initialize the host daemon instance.

        Args:
            sftp_conn (sftpConnection): Connected SFTP session.
            log (log): Logger instance.
            host_id (int): Host identifier.
            task_id (int, optional): Current task being processed. Defaults to None.
            task_dict (dict, optional): Mapping of tasks for batch processing. Defaults to None.
            dispatcher (callable, optional): Callback to handle DB operations or other effects.
                Signature: dispatcher(action: str, payload: dict) -> None.

        Returns:
            None
        """
        self.sftp_conn = sftp_conn
        self.log = log
        self.config = None
        self.halt_flag_set_time = None

    # ----------------------------------------------------------------------
    # Configuration loader
    # ----------------------------------------------------------------------
    def get_config(self) -> bool:
        """Load the daemon configuration file from the remote node.

        Reads and parses the file defined in k.DAEMON_CFG_FILE on the remote
        host, storing the resulting dictionary into self.config.

        Returns:
            bool: True if configuration loaded successfully; False otherwise.
        """
        try:
            cfg_content = self.sftp_conn.read(k.DAEMON_CFG_FILE, "r")
            if not cfg_content:
                raise FileNotFoundError("Empty configuration file.")
            cfg, _ = parse_cfg(cfg_content)
            self.config = cfg
            return True
        except Exception as e:
            self.log.error(f"[HostDaemon] Failed to load config: {e}")
            return False

    # ----------------------------------------------------------------------
    # HALT flag management
    # ----------------------------------------------------------------------
    def _check_halt_flag(self, service: str, use_pid: bool = True) -> HaltFlagState:
        """
        Check current HALT_FLAG state and classify it.

        Does NOT remove or modify the file — only inspects and reports:
        - NO_FLAG: does not exist
        - OWN_FLAG: created by this service (and optionally same PID)
        - FOREIGN_FLAG: exists but owned by another service
        - STALE_FLAG: exists but too old (should be removed externally)

        Args:
            service (str): Service name/identifier.
            use_pid (bool): Whether to validate PID ownership.

        Returns:
            HaltFlagState
        """
        flag_path = self.config.get("HALT_FLAG")
        if not flag_path:
            self.log.warning("[HALT] No HALT_FLAG path configured.")
            return HaltFlagState.NO_FLAG

        try:
            raw = self.sftp_conn.read(filename=flag_path, mode="r")
            if not raw:
                return HaltFlagState.NO_FLAG

            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="ignore").strip()
            if not raw:
                return HaltFlagState.NO_FLAG

            # Try parse JSON format (fallback for plain text)
            try:
                data = json.loads(raw)
                flag_service = data.get("service")
                flag_pid = data.get("pid")
                flag_time = data.get("timestamp")
            except json.JSONDecodeError:
                flag_service, flag_pid, flag_time = raw, None, None

            # Check if flag is stale
            if flag_time:
                try:
                    flag_dt = datetime.fromisoformat(flag_time)
                    elapsed = (datetime.now() - flag_dt).total_seconds()
                    timeout = int(self.config.get("HALT_TIMEOUT", 30))
                    cycles = getattr(k, "HALT_FLAG_CHECK_CYCLES", 3)
                    max_age = timeout * cycles
                    if elapsed > max_age:
                        self.log.warning(f"[HALT] HALT_FLAG stale (age={elapsed:.1f}s > {max_age}s).")
                        return HaltFlagState.STALE_FLAG
                except Exception:
                    pass

            # Determine ownership
            same_service = (flag_service == service)
            same_pid = (flag_pid == os.getpid()) if use_pid else True

            if same_service and same_pid:
                return HaltFlagState.OWN_FLAG
            return HaltFlagState.FOREIGN_FLAG

        except FileNotFoundError:
            return HaltFlagState.NO_FLAG
        except Exception as e:
            self.log.warning(f"[HALT] Failed to read HALT_FLAG: {e}")
            return HaltFlagState.NO_FLAG

    def _write_halt_flag(self, service: str, use_pid: bool = True) -> None:
        """
        Create or overwrite the HALT_FLAG file on the remote host.

        This method writes a structured JSON payload containing:
        - 'service': Name of the service that owns the lock
        - 'pid': Current process ID (or None if not used)
        - 'timestamp': ISO 8601 timestamp of creation

        Args:
            service (str): Identifier for the calling service (e.g., "appCataloga_discovery").
            use_pid (bool): Whether to include the current process PID. If False, 'pid' is written as null.

        Raises:
            Exception: Propagates any SFTP or serialization error after logging.
        """
        flag_path = self.config.get("HALT_FLAG")
        if not flag_path:
            self.log.warning("[HALT] No HALT_FLAG path configured.")
            return

        try:
            # Build JSON payload (keep field structure stable)
            payload = {
                "service": service,
                "pid": os.getpid() if use_pid else None,   # Always include key, even if null
                "timestamp": datetime.now().isoformat()
            }

            json_data = json.dumps(payload, indent=2)

            # Write flag to remote system (overwrite if exists)
            self.sftp_conn.write(filename=flag_path, content=json_data.encode("utf-8"))

            pid_display = payload["pid"] if payload["pid"] is not None else "None"
            self.log.entry(f"[HALT] HALT_FLAG created by {service} (pid={pid_display}).")

        except Exception as e:
            self.log.error(f"[HALT] Failed to create HALT_FLAG for {service}: {e}")
            raise

    def get_halt_flag(self, service: str, use_pid: bool = True) -> None:
        """
        Acquire or create the HALT_FLAG on the remote host.

        Ensures exclusive access to the remote filesystem by creating a HALT_FLAG.
        If another service holds it, waits up to HALT_TIMEOUT * HALT_FLAG_CHECK_CYCLES.
        If the timeout expires, it forcibly removes and recreates the flag to prevent deadlock.

        Args:
            service (str): Service name (e.g., "appCataloga_discovery").
            use_pid (bool): Whether to include process PID in the flag (default: True).

        Raises:
            TimeoutError: If unable to acquire HALT_FLAG after all retries.
        """
        flag_path = self.config.get("HALT_FLAG")
        if not flag_path:
            self.log.warning("[HALT] No HALT_FLAG path configured.")
            return False

        halt_timeout = int(self.config.get("HALT_TIMEOUT", 300))
        max_cycles = getattr(k, "HALT_FLAG_CHECK_CYCLES", 6)
        sleep_interval = halt_timeout / max_cycles

        for attempt in range(1, max_cycles + 1):
            state = self._check_halt_flag(service, use_pid=use_pid)

            # Case 1: No flag → create new
            if state == HaltFlagState.NO_FLAG:
                self._write_halt_flag(service, use_pid=use_pid)
                return True

            # Case 2: Owned by this service → proceed immediately
            elif state == HaltFlagState.OWN_FLAG:
                self.log.entry(f"[HALT] HALT_FLAG already owned by {service}. Proceeding.")
                return True

            # Case 3: Stale flag → remove and recreate
            elif state == HaltFlagState.STALE_FLAG:
                self.log.entry("[HALT] Stale HALT_FLAG detected. Cleaning up...")
                self.release_halt_flag(service, use_pid=use_pid)
                time.sleep(1)
                self._write_halt_flag(service, use_pid=use_pid)
                return True

            # Case 4: Flag held by another process → wait and retry
            elif state == HaltFlagState.FOREIGN_FLAG:
                if attempt >= max_cycles:
                    self.log.warning("[HALT] Timeout reached. Forcing HALT_FLAG removal to prevent deadlock.")
                    self.release_halt_flag(service, use_pid=use_pid, force=True)
                    self._write_halt_flag(service, use_pid=use_pid)
                    return True

                self.log.entry(f"[HALT] HALT_FLAG held by another service. Waiting ({attempt}/{max_cycles})...")
                time.sleep(sleep_interval)
                continue

            # Case 5: Unknown/unexpected state — retry
            else:
                self.log.warning(f"[HALT] Unknown HALT_FLAG state '{state}'. Retrying...")
                time.sleep(sleep_interval)

        # ----------------------------------------------------------------------
        # If loop ends without acquiring — hard failure
        # ----------------------------------------------------------------------
        self.log.warning(f"[HALT] Unable to acquire HALT_FLAG after {halt_timeout}s.")
        return False


        
    def release_halt_flag(self, service: str, use_pid: bool = False, force: bool = False) -> None:
        """
        Safely remove the HALT_FLAG file from the remote host.

        The function checks flag ownership and only removes it if:
        - The flag belongs to the same service (OWN_FLAG), or
        - The flag is stale (STALE_FLAG), or
        - The `force` parameter is explicitly True.

        Args:
            service (str): Service identifier (e.g. "appCataloga_discovery").
            use_pid (bool): Whether to validate PID ownership. Default is False.
            force (bool): If True, forcibly removes the HALT_FLAG regardless of ownership.

        Returns:
            None
        """
        flag_path = self.config.get("HALT_FLAG")
        if not flag_path:
            self.log.warning("[HALT] release_halt_flag(): No HALT_FLAG path configured.")
            return

        try:
            # ------------------------------------------------------------------
            # FORCE MODE — unconditional removal attempt
            # ------------------------------------------------------------------
            if force:
                try:
                    self.sftp_conn.remove(flag_path)
                    self.log.entry("[HALT] HALT_FLAG forcibly removed.")
                except FileNotFoundError:
                    self.log.entry("[HALT] HALT_FLAG already absent (force mode).")
                except Exception as e:
                    self.log.warning(f"[HALT] Forced HALT_FLAG removal failed: {e}")
                return

            # ------------------------------------------------------------------
            # NORMAL MODE — check ownership or stale state before removal
            # ------------------------------------------------------------------
            state = self._check_halt_flag(service, use_pid=use_pid)

            if state in (HaltFlagState.OWN_FLAG, HaltFlagState.STALE_FLAG):
                try:
                    self.sftp_conn.remove(flag_path)
                    self.log.entry(f"[HALT] HALT_FLAG removed ({state.name.lower()}).")
                except FileNotFoundError:
                    self.log.entry("[HALT] HALT_FLAG already absent.")
                except Exception as e:
                    self.log.warning(f"[HALT] Failed to remove HALT_FLAG: {e}")
            else:
                self.log.entry(f"[HALT] HALT_FLAG not removed (state={state.name}).")

        except Exception as e:
            self.log.warning(f"[HALT] release_halt_flag() encountered an unexpected error: {e}")
            
    
    def _get_mapped_files(self, filter: Dict, callBackFileHistory, callBackFileTaskHistory):
        """
        Retrieve remote or local files eligible for backup according to the given filter.

        The behavior is determined by the `agent` property validated by Filter._validate():
            - agent = "remote": reads `.files.changed.list` from the remote node.
            - agent = "local" : performs recursive file searches on the repository path,
                                with behavior depending on `mode`.

        Local mode behavior:
            - FILE : wildcard-based direct search (forced backup).
            - others (ALL, RANGE, LAST, etc.): recursive search filtered by extension
            and excluding files already present in FILE_TASK_HISTORY.

        Args:
            filter (dict): Normalized filter configuration (from Filter.data).
            callBackFileHistory (Callable): Function to check existence of a filename
                in FILE_TASK_HISTORY. Signature:
                `callBackFileHistory(NA_HOST_FILE_NAME: str) -> bool`.

        Returns:
            list[str]: List of absolute file paths to be backed up.
        """
        due_backup_list: List[str] = []
        mode = (filter.get("mode") or "").upper()
        agent = (filter.get("agent") or "").upper()

        # ------------------------------------------------------------------
        # REMOTE agent → read list generated by remote node
        # ------------------------------------------------------------------
        if agent == "REMOTE":
            try:
                raw_bytes = self.sftp_conn.read(filename=self.config["DUE_BACKUP"], mode="r")
                if not raw_bytes:
                    return []
                decoded = raw_bytes.decode("utf-8", errors="ignore").replace("\x00", "").strip()
                due_backup_list = [line for line in decoded.splitlines() if line.strip()]
            except Exception as e:
                self.log.warning(f"[get_mapped_files] Failed to read remote control file: {e}")
                return []

        # ------------------------------------------------------------------
        # LOCAL agent → direct filesystem scan
        # ------------------------------------------------------------------
        elif agent == "LOCAL":
            remote_dir = self.config.get("LOCAL_REPO", "/")

            # Pattern choice
            if mode == "FILE":
                pattern = filter.get("file_name") or "*"
            else:
                extension = filter.get("extension")
                pattern = f"*{extension}" if extension else "*"

            # Single unified SFTP search
            try:
                all_files = self.sftp_conn.sftp_find_files(
                    remote_path=remote_dir,
                    pattern=pattern,
                    recursive=True,
                )
            except Exception as e:
                self.log.error(f"[get_mapped_files] Failed to list local repo files: {e}")
                return []

            if not all_files:
                return []

            # Forced backup (FILE mode)
            if mode == "FILE":
                due_backup_list = all_files

            # Filtered backup (ALL/RANGE/LAST)
            else:
                for full_path in all_files:
                    filename = os.path.basename(full_path)
                    # Check existing file in FILE_TASK_HISTORY
                    if not callBackFileHistory(NA_HOST_FILE_NAME=filename) and not callBackFileTaskHistory(NA_HOST_FILE_NAME=filename):
                        due_backup_list.append(full_path)

        return due_backup_list or []

    
    # ------------- public APIs preserved (call the unified implementation) ----
    def get_metadata(self, filename: str) -> Dict[str, Any]:
        """Get metadata for a single remote file (public API preserved).

        Args:
            filename (str): Absolute remote path to inspect.

        Returns:
            dict: Metadata mapping for the requested file, or empty dict if not found.
        """
        res = self.sftp_conn._get_metadata_batch([filename])
        return res[0] if res else {}

    def get_metadata_files(self, filter: Dict, callBackFileHistory, callBackFileTaskHistory):
        """Get metadata for multiple remote files (public API preserved).

        Args:
            file_list (list[str]): Absolute remote file paths to inspect.

        Returns:
            list[dict]: A list of metadata dicts (or empty dicts for not-found files).
        """
        
        file_list = self._get_mapped_files(filter=filter,
                                           callBackFileHistory=callBackFileHistory,
                                           callBackFileTaskHistory=callBackFileTaskHistory)
        return self.sftp_conn._get_metadata_batch(file_list=file_list)

    # ----------------------------------------------------------------------
    # Backup completion logging
    # ----------------------------------------------------------------------
    def write_backup_done(self, message: str) -> None:
        """Append a line to BACKUP_DONE file in the remote node.

        Args:
            message (str): Message or file path to append.

        Returns:
            None
        """
        if not self.config or "BACKUP_DONE" not in self.config:
            self.log.warning("[HostDaemon] BACKUP_DONE not defined in config.")
            return
        try:
            self.sftp_conn.append(self.config["BACKUP_DONE"], message + "\n")
            self.log.entry(f"[HostDaemon] Logged to BACKUP_DONE: {message}")
        except Exception as e:
            self.log.warning(f"[HostDaemon] Failed to append BACKUP_DONE: {e}")

    # ----------------------------------------------------------------------
    # Cleanup / termination
    # ----------------------------------------------------------------------
    def close_host(self, cleanup_due_backup: bool = False) -> None:
        """Clean up temporary files and release resources gracefully.

        Args:
            cleanup_due_backup (bool): If True, remove DUE_BACKUP file.

        Returns:
            None
        """
        try:
            if cleanup_due_backup and self.config:
                self.sftp_conn.remove(self.config["DUE_BACKUP"])
        except Exception as e:
            self.log.warning(f"[HostDaemon] Could not remove DUE_BACKUP: {e}")
        try:
            self.sftp_conn.close()
        except Exception as e:
            self.log.warning(f"[HostDaemon] Error closing SFTP session: {e}")



# =====================================================================
# Filter class + parse_filter wrapper
# =====================================================================
class Filter:
    """Unified handler for parsing, validating, and applying file filters.

    Provides standardized logic for applying file-level filters based on
    metadata (name, modification date, extension, etc.) and supports multiple
    modes such as RANGE, FILE, LAST, ALL, and AGENT.

    Attributes:
        raw (Union[str, dict, None]): Raw filter configuration (JSON or dict).
        data (dict): Normalized filter configuration after parsing/validation.
        log (Optional[Any]): Optional logger for diagnostic messages.
    """

    # ------------------------------------------------------------------
    # Filter mode constants
    # ------------------------------------------------------------------
    MODE_NONE = "NONE"
    MODE_ALL = "ALL"
    MODE_FILE = "FILE"
    MODE_RANGE = "RANGE"
    MODE_LAST = "LAST"

    VALID_MODES = (
        MODE_NONE,
        MODE_ALL,
        MODE_FILE,
        MODE_RANGE,
        MODE_LAST,
    )

    def __init__(self, filter_raw: Union[str, Dict[str, Any], None] = None, log: Optional[Any] = None):
        """Initialize a Filter instance.

        Args:
            filter_raw: JSON string or dict containing filter configuration.
            log: Optional logger instance for diagnostic output.
        """
        self.log = log
        self.raw = filter_raw
        self.data = self._parse_and_validate()

    # ------------------------------------------------------------------
    # Parsing & Validation
    # ------------------------------------------------------------------
    @staticmethod
    def _default_dict() -> Dict[str, Any]:
        """Return a default filter dictionary.

        Returns:
            dict: Default filter configuration with neutral values.
        """
        return dict(
            mode=Filter.MODE_NONE,
            start_date=None,
            end_date=None,
            last_n_files=None,
            extension=None,
            file_name=None,
            agent="remote",
        )

    def _parse_and_validate(self) -> Dict[str, Any]:
        """Parse and validate the filter configuration.

        Returns:
            dict: Normalized and validated filter configuration.
        """
        try:
            f = self._parse_raw()
            self._validate(f)
            return f
        except Exception as e:
            if self.log:
                self.log.warning(f"[Filter] Parse/validate failed: {e}")
            return self._default_dict()

    def _parse_raw(self) -> Dict[str, Any]:
        """Normalize raw JSON or dict input into canonical structure.

        Returns:
            dict: Parsed configuration dictionary.
        """
        if not self.raw:
            return self._default_dict()

        if isinstance(self.raw, str):
            try:
                f = json.loads(self.raw)
            except Exception as e:
                if self.log:
                    self.log.entry(f"[Filter] JSON parse error: {e}")
                return self._default_dict()
        elif isinstance(self.raw, dict):
            f = dict(self.raw)
        else:
            return self._default_dict()

        return {
            "mode": str(f.get("mode", Filter.MODE_NONE)).upper().strip(),
            "start_date": f.get("start_date"),
            "end_date": f.get("end_date"),
            "last_n_files": f.get("last_n_files"),
            "extension": f.get("extension"),
            "file_name": f.get("file_name"),
            "agent": f.get("agent"),
        }

    def _validate(self, f: Dict[str, Any]) -> None:
        """
        Validate and normalize the parsed filter dictionary.

        This method ensures consistency among filter fields, enforcing mode-specific
        constraints and type normalization. Each supported mode activates only the
        fields relevant to its semantics, while the others are nullified.

        Behavior by mode:
            - RANGE: Validates date boundaries (`start_date`, `end_date`), ensuring
            correct chronological order and ISO 8601 formatting.
            - FILE: Forces `agent = "local"`, normalizes `file_name`, and disables
            `extension` when redundant. If `file_name` is empty, reverts mode to NONE.
            - LAST: Converts `last_n_files` to an integer ≥ 1; invalid entries are nulled.
            - ALL/NONE: Retain only `extension` and `agent`.

        Additionally:
            - `extension` is normalized to lowercase and prefixed with '.' if missing.
            - `agent` is coerced to 'local' or 'remote' (default = 'remote').
            - Unused fields are explicitly set to None to simplify downstream logic.

        Args:
            f (dict): Parsed filter configuration (may be user-provided or default).

        Returns:
            None. The input dictionary is modified in-place.
    """
        mode = f["mode"]

        # --- Normalize agent ---
        agent = f.get("agent")
        if isinstance(agent, str):
            agent = agent.strip().lower()
            if agent not in ("local", "remote"):
                agent = "remote"
        else:
            agent = "remote"

        # --- RANGE mode ---
        if mode == Filter.MODE_RANGE:
            start, end = self._safe_date(f.get("start_date")), self._safe_date(f.get("end_date"))
            if start and end and start > end:
                start, end = end, start
            f["start_date"], f["end_date"] = start, end

        # --- FILE mode ---
        elif mode == Filter.MODE_FILE:
            agent = "local"  # file mode sempre local
            file_name = (f.get("file_name") or "").strip().lower()
            if not file_name:
                f["mode"] = Filter.MODE_NONE
            elif "." in os.path.basename(file_name):
                f["extension"] = None
            f["file_name"] = file_name

        # --- LAST mode ---
        elif mode == Filter.MODE_LAST:
            try:
                f["last_n_files"] = max(1, int(f["last_n_files"]))
            except Exception:
                f["last_n_files"] = None

        # --- Extension normalization (all modes) ---
        ext = f.get("extension")
        if isinstance(ext, str):
            ext = ext.strip().lower()
            if ext and not ext.startswith("."):
                ext = f".{ext}"
            f["extension"] = ext
        else:
            f["extension"] = None

        # --- Update final agent value ---
        f["agent"] = agent

        # --- Define active fields ---
        active_fields = {
            Filter.MODE_RANGE: {"start_date", "end_date", "extension", "agent"},
            Filter.MODE_FILE: {"file_name", "extension", "agent"},
            Filter.MODE_LAST: {"last_n_files", "extension", "agent"},
            Filter.MODE_ALL: {"extension", "agent"},
            Filter.MODE_NONE: {"extension", "agent"},
        }

        # --- Nullify unused fields ---
        all_fields = {"start_date", "end_date", "last_n_files", "extension", "file_name", "agent"}
        keep = active_fields.get(f["mode"], set())
        for key in all_fields - keep:
            f[key] = None



    @staticmethod
    def _safe_date(val: Any) -> Optional[str]:
        """Convert value to ISO8601 date string if valid.

        Args:
            val: Value to convert.

        Returns:
            Optional[str]: ISO8601 formatted date string, or None if invalid.
        """
        try:
            return datetime.fromisoformat(str(val).replace("Z", "")).isoformat()
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Evaluation logic (Strategy dispatch)
    # ------------------------------------------------------------------
    
    def evaluate(
        self,
        tasks: List[Dict[str, Any]],
        candidate_paths: Optional[List[Dict[str, Any]]],
    ) -> List[int]:
        """
        Evaluate FILE_TASK entries according to the active filter configuration.

        Args:
            tasks (List[Dict[str, Any]]): List of FILE_TASK dictionaries obtained from the database.
                Each task must contain at least:
                    - NA_HOST_FILE_PATH
                    - NA_HOST_FILE_NAME
                    - DT_FILE_CREATED
                    - NA_EXTENSION
            candidate_paths (Optional[List[Dict[str, Any]]]): Optional list of dictionaries,
                each containing the key 'NA_FULL_PATH'. Used only when mode == FILE.

        Returns:
            List[int]: List of binary flags where 1 = match and 0 = skip.
        """
        mode = self.data["mode"]
        flags: List[int] = []

        # Extract full paths from provided candidates
        candidate_set = [f['NA_FULL_PATH'] for f in candidate_paths] if candidate_paths else []

        # --- RANGE mode: prepare date boundaries ---
        start_dt = end_dt = None
        if mode == Filter.MODE_RANGE:
            try:
                start = self.data.get("start_date")
                end = self.data.get("end_date")
                if isinstance(start, str):
                    start_dt = datetime.fromisoformat(start)
                if isinstance(end, str):
                    end_dt = datetime.fromisoformat(end)
            except Exception:
                pass

        # --- LAST mode: pick N most recent files ---
        selected_names = set()
        if mode == Filter.MODE_LAST and self.data.get("last_n_files"):
            try:
                last_n = int(self.data["last_n_files"])
                sorted_tasks = sorted(
                    [t for t in tasks if t.get("DT_FILE_CREATED")],
                    key=lambda t: t["DT_FILE_CREATED"],
                    reverse=True,
                )
                selected_names = {
                    t["NA_HOST_FILE_NAME"]
                    for t in sorted_tasks[:last_n]
                }
            except Exception:
                pass

        # --- Normalize extension filter ---
        extension_filter = self.data.get("extension")
        if isinstance(extension_filter, str):
            extension_filter = extension_filter.strip().lower()
            if not extension_filter:
                extension_filter = None
        else:
            extension_filter = None

        # --- Main evaluation loop ---
        for t in tasks:
            path = t.get("NA_HOST_FILE_PATH", "")
            name = t.get("NA_HOST_FILE_NAME", "")
            full_path = os.path.join(path, name)
            include = False

            # MODE: ALL
            if mode == Filter.MODE_ALL:
                include = True

            # MODE: NONE
            elif mode == Filter.MODE_NONE:
                include = False

            # MODE: FILE (explicit match list)
            elif mode == Filter.MODE_FILE and candidate_set:
                include = full_path in candidate_set

            # MODE: RANGE (date filtering)
            elif mode == Filter.MODE_RANGE:
                mod = t.get("DT_FILE_CREATED")
                if isinstance(mod, str):
                    try:
                        mod = datetime.fromisoformat(mod)
                    except Exception:
                        mod = None
                if isinstance(mod, datetime):
                    if start_dt and end_dt:
                        include = start_dt <= mod <= end_dt
                    elif start_dt:
                        include = mod >= start_dt
                    elif end_dt:
                        include = mod <= end_dt

            # MODE: LAST (latest N files)
            elif mode == Filter.MODE_LAST and selected_names:
                include = name in selected_names

            # Optional extension filter
            if include and extension_filter:
                include = str(name).lower().endswith(extension_filter)

            flags.append(1 if include else 0)

        return flags


    # ------------------------------------------------------------------
    # Static utilities
    # ------------------------------------------------------------------
    @staticmethod
    def build_inputs_from_tasks(tasks: List[Dict[str, Any]]) -> Tuple[List[Tuple[str, str]], Dict[str, Dict[str, Any]]]:
        """Convert FILE_TASK database records into filterable input structures.

        Args:
            tasks: List of FILE_TASK records.

        Returns:
            Tuple[List[Tuple[str, str]], Dict[str, Dict[str, Any]]]:
                - Tuples: (directory, filename) pairs.
                - Metadata: Mapping of filename to metadata dict.
        """
        tuples = [(t["NA_HOST_FILE_PATH"], t["NA_HOST_FILE_NAME"]) for t in tasks]
        metadata = {
            t["NA_HOST_FILE_NAME"]: {
                "extension": t.get("NA_EXTENSION"),
                "dt_file_modified": t.get("DT_FILE_MODIFIED"),
            } for t in tasks
        }
        return tuples, metadata

    @staticmethod
    def build_inputs_from_files(
        file_list: List[str],
        file_metadata: List[Dict[str, Any]],
    ) -> Tuple[List[Tuple[str, str]], Dict[str, Dict[str, Any]]]:
        """Convert remote file discovery results into tuples and metadata dicts.

        Args:
            file_list: List of absolute remote file paths.
            file_metadata: List of metadata dicts with at least:
                - "NA_FILE": filename (basename)
                - "NA_EXTENSION": file extension
                - "DT_FILE_MODIFIED": last modified timestamp

        Returns:
            Tuple[List[Tuple[str, str]], Dict[str, Dict[str, Any]]]:
                - Tuples: (remote_dir, filename)
                - Metadata: {filename: {...}}
        """
        tuples: List[Tuple[str, str]] = []
        metadata: Dict[str, Dict[str, Any]] = {}

        for f in file_list:
            try:
                remote_dir, remote_name = posixpath.split(f)
                if remote_name:
                    tuples.append((remote_dir, remote_name))
            except Exception as e:
                print(f"[Filter.build_inputs_from_files] Skipped malformed path '{f}': {e}")

        for meta in file_metadata or []:
            name = meta.get("NA_FILE")
            if not name:
                continue
            metadata[name] = {
                "extension": meta.get("NA_EXTENSION"),
                "dt_file_modified": meta.get("DT_FILE_MODIFIED"),
            }

        return tuples, metadata

    @staticmethod
    def apply(
        tasks:List[Dict[str, Any]],
        filter_cfg: Dict[str, Any],
        candidate_paths: Optional[List[str]] = None,
        log: Optional[Any] = None,
    ) -> List[int]:
        """Apply a filter configuration to a given set of files and metadata.

        Args:
            files_tuple_list: (directory, filename) pairs.
            file_metadata: File metadata (list or dict form).
            filter_cfg: Filter configuration dictionary or JSON string.
            candidate_paths: Optional subset of paths (FILE mode).
            log: Optional logger for diagnostics.
            file_history_checker: Optional callback for history lookup.

        Returns:
            List[int]: List of flags (1 = file included, 0 = excluded).
        """
        try:
            f = Filter(filter_cfg, log=log)
            return f.evaluate(tasks, candidate_paths)
        except Exception as e:
            if log:
                log.error(f"[Filter] apply() failed: {e}")
            raise
    
def parse_filter(filter_raw: Union[str, Dict[str, Any], None], log: Optional[Any] = None) -> Dict[str, Any]:
    """Safely parse and normalize a raw filter (legacy wrapper).

    This function is kept for backward compatibility with older code that
    directly calls `parse_filter()` instead of instantiating `Filter`.

    Args:
        filter_raw (str | dict | None): Raw JSON or dict representing the filter.
        log (optional): Optional logger for diagnostics.

    Returns:
        dict: Normalized filter dictionary with keys:
            - mode (str): 'NONE', 'ALL', 'RANGE', 'LAST', or 'FILE'
            - start_date (str|None)
            - end_date (str|None)
            - last_n_files (int|None)
            - extension (str|None)
            - file_name (str|None)
    """
    return Filter(filter_raw, log=log).data


# =====================================================================
# Socket message parser (public API preserved)
# =====================================================================
def parse_socket_message(peername: Tuple[str, int], data: str, log: Optional[log] = None) -> Dict[str, Any]:
    """Parse a single-line control message coming from a TCP socket.

    Command Layout (space-separated tokens):
        <MSG_TYPE> <HOST_ID> <HOST_UID> <HOST_ADDR> <HOST_PORT> <USER> <PASSWORD> <FILTER_JSON>

    Args:
        peername (tuple[str, int]): (ip, port) pair identifying the peer.
        data (str): Raw message string (single line expected).
        log (log|None): Optional logger instance for diagnostics.

    Returns:
        dict: Parsed message with fields:
            - peer (dict): {'ip': str, 'port': int}
            - command (str|None)
            - host_id (int|None)
            - host_uid (str|None)
            - host_addr (str|None)
            - host_port (int|None)
            - user (str|None)
            - password (str|None)
            - filter (dict): Normalized filter mapping
                (defaults to k.NONE_FILTER if parsing fails).
    """
    peer_ip, peer_port = peername

    try:
        parts = data.strip().split(" ", 7)

        msg_type = parts[0]
        host_id = int(parts[1])
        host_uid = parts[2]
        host_addr = parts[3]
        host_port = int(parts[4])
        user = parts[5]
        password = parts[6]
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
            "filter": filter_dict,
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
            "filter": getattr(k, "NONE_FILTER", {"mode": "NONE"}),
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
        host_addr=host["host_addr"],
        port=host["port"],
        user=host["user"],
        password=host["password"],
        log=log,
    )

    daemon = hostDaemon(
        sftp_conn=sftp_conn,
        log=log,
    )

    return sftp_conn, daemon


def _random_jitter_sleep() -> None:
    """Small random delay to reduce race conditions between workers."""
    time.sleep(random.uniform(0.5, k.MAX_HOST_TASK_WAIT_TIME))