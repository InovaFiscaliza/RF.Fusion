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
import threading
import json
import paramiko
import posixpath
import fnmatch
import stat
import random
import ntpath
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Optional, Union
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from enum import Enum
import shlex



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

# Shared executor – limited number of worker threads
_TIMEOUT_EXECUTOR = ThreadPoolExecutor(
    max_workers=8,  # você pode ajustar entre 4–16 dependendo do hardware
    thread_name_prefix="timeout-worker"
)
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
        """Initialize SSH and SFTP connections with stability tuning.

        Args:
            host_uid (str): Unique host identifier (for logs).
            host_addr (str): Hostname or IP address.
            port (int): SSH port number.
            user (str): SSH username.
            password (str): SSH password.
            log (log): Logger instance.

        Raises:
            Exception: When connection to remote host fails.
        """

        self.log = log
        self.host_uid = host_uid
        self.host_addr = host_addr
        self.port = port
        self.user = user

        try:
            # -------------------------------------------------------------
            # SSH CLIENT SETUP
            # -------------------------------------------------------------
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy()
            )

            # -------------------------------------------------------------
            # CONNECT (timeouts apply ONLY to connection/auth phase)
            # -------------------------------------------------------------
            self.ssh_client.connect(
                hostname=host_addr,
                port=port,
                username=user,
                password=password,
                timeout=k.SSH_CONNECT_TIMEOUT,
                banner_timeout=k.SSH_BANNER_TIMEOUT,
                auth_timeout=k.SSH_AUTH_TIMEOUT,
                look_for_keys=False,
                allow_agent=False,
            )

            # -------------------------------------------------------------
            # TRANSPORT HARDENING
            # Critical for long-running and high-volume commands
            # -------------------------------------------------------------
            transport = self.ssh_client.get_transport()
            if not transport:
                raise RuntimeError("SSH transport not available")

            # Prevent idle disconnects (firewalls / OpenSSH)
            transport.set_keepalive(30)

            # Disable rekey during heavy stdout streaming
            transport.packetizer.REKEY_BYTES = 2**40
            transport.packetizer.REKEY_PACKETS = 2**40

            # Increase SSH window size to avoid stdout backpressure
            transport.window_size = 2**24  # 16 MB

            # -------------------------------------------------------------
            # SFTP SESSION
            # -------------------------------------------------------------
            self.sftp = self.ssh_client.open_sftp()

            self.log.entry(
                f"[SSH] Connected to {self.host_uid} "
                f"({self.host_addr}:{self.port}) as {self.user}"
            )

        except Exception as e:
            self.log.error(
                f"[SSH] Error initializing SSH to "
                f"'{self.host_uid}' ({self.host_addr}): {e}"
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
        
    def is_connected(self) -> bool:
        """Return True if the SSH/SFTP connection is still alive.

        Performs a lightweight check of the underlying SSH transport
        and, if possible, a quick SFTP lstat() on the remote home directory.

        Returns:
            bool: True if the connection is active; False if closed or broken.
        """
        try:
            # Check if SSH client and transport are still active
            if not self.ssh_client:
                return False

            transport = self.ssh_client.get_transport()
            if not transport or not transport.is_active():
                return False

            # Optional quick SFTP check (may fail silently if already closed)
            if self.sftp:
                try:
                    self.sftp.listdir(".")
                except Exception:
                    # Ignore file errors, only socket-level failures matter
                    pass

            return True
        except Exception:
            return False

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


    
    def detect_remote_os(self):
        """
        Detect the operating system of a remote host via SSH.

        Detection strategy:
            1) Attempt to identify a Unix/Linux system using `uname -s`.
            This command is only available on Unix-like systems and
            reliably returns "Linux" when executed successfully.

            2) If the Linux test fails, attempt to identify a Windows system
            using PowerShell. The command emits a deterministic string
            ("windows") to STDOUT, which works reliably in non-interactive
            SSH sessions (e.g. Paramiko).

        Design considerations:
            • Avoids using `cmd /c ver`, which is unreliable in SSH
            non-interactive contexts and may write output to STDERR.
            • Avoids parsing OS version strings or localized output.
            • Uses deterministic output for robust detection.
            • Safe for Windows Server, Windows 10/11, and Windows Embedded.

        Returns:
            str:
                "linux"   → Remote host is a Linux/Unix system
                "windows" → Remote host is a Windows system

        Notes:
            • If all detection methods fail, the caller should assume
            a Linux-like environment as a conservative fallback.
            • Any exception during command execution is silently ignored
            to allow fallback detection paths.
        """

        # ---------------------------------------------------------------
        # 1) Linux detection (uname exists only on Unix-like systems)
        # ---------------------------------------------------------------
        try:
            stdin, stdout, stderr = self.ssh_client.exec_command(
                "uname -s",
                timeout=5
            )
            uname = stdout.read().decode(errors="ignore").strip().lower()
            if "linux" in uname:
                return "linux"
        except Exception:
            pass

        # ---------------------------------------------------------------
        # 2) Windows detection via PowerShell (robust and deterministic)
        # ---------------------------------------------------------------
        try:
            stdin, stdout, stderr = self.ssh_client.exec_command(
                "powershell -NoProfile -Command Write-Output windows",
                timeout=5
            )
            out = stdout.read().decode(errors="ignore").strip().lower()
            if out == "windows":
                return "windows"
        except Exception:
            pass

        # ---------------------------------------------------------------
        # 3) Final fallback
        # ---------------------------------------------------------------
        return "linux"


    def sftp_find_files_with_metadata(
        self,
        remote_path: str,
        pattern: str,
        recursive: bool = True,
        newer_than: Optional[str] = None,
    ):
        """
        Cross-platform recursive filesystem traversal WITH metadata emission.
        """

        remote_path = remote_path.rstrip("/").rstrip("\\")

        pattern = pattern.strip().replace('"', "").replace("'", "")
        if not pattern.startswith("*"):
            pattern = "*" + pattern

        os_type = self.detect_remote_os()
        results: list[dict] = []

        # -----------------------------------------------------------
        # Log operation start (high-level intent)
        # -----------------------------------------------------------
        self.log.entry(
            f"[META] Traversal start | os={os_type} | "
            f"path={remote_path} | pattern={pattern} | "
            f"recursive={'yes' if recursive else 'no'} | "
            f"incremental={'yes' if newer_than else 'no'}"
        )

        start = time.monotonic()

        # ============================================================
        # LINUX BACKEND
        # ============================================================
        if os_type == "linux":

            newer = f'-newermt "{newer_than}"' if newer_than else ""

            cmd = (
                f"find {remote_path} -type f -iname '{pattern}' {newer} "
                "-printf '%p|%s|%C@|%T@|%A@|%U|%G|%m\n'"
            )

            # Log the exact command executed (critical for debugging)
            self.log.entry(f"[META][LINUX] exec: {cmd}")

            try:
                _, stdout, stderr = self.ssh_client.exec_command(
                    cmd, timeout=k.HOST_BUSY_TIMEOUT
                )

                for raw in iter(stdout.readline, ""):
                    raw = raw.strip()
                    if not raw:
                        continue

                    fullpath, size, c_at, m_at, a_at, uid, gid, mode = raw.split("|")

                    filename = os.path.basename(fullpath)
                    dirname  = os.path.dirname(fullpath)
                    _, ext   = os.path.splitext(filename)

                    results.append({
                        "NA_FULL_PATH": fullpath,
                        "NA_PATH": dirname,
                        "NA_FILE": filename,
                        "NA_EXTENSION": ext,
                        "VL_FILE_SIZE_KB": int(int(size) // 1024),
                        "DT_FILE_CREATED": datetime.fromtimestamp(float(c_at)),
                        "DT_FILE_MODIFIED": datetime.fromtimestamp(float(m_at)),
                        "DT_FILE_ACCESSED": datetime.fromtimestamp(float(a_at)),
                        "NA_OWNER": str(uid),
                        "NA_GROUP": str(gid),
                        "NA_PERMISSIONS": stat.filemode(int(mode, 8)),
                    })

                err = stderr.read().decode("utf-8", errors="ignore").strip()
                if err:
                    self.log.warning(f"[META][LINUX] STDERR: {err}")

            except Exception as e:
                self.log.error(f"[META][LINUX] discovery failed: {e}")
                return []

        # ============================================================
        # WINDOWS BACKEND
        # ============================================================
        elif os_type == "windows":

            recurse = "-Recurse" if recursive else ""
            date_guard = f"$cutoff = Get-Date '{newer_than}';" if newer_than else ""

            ps_cmd = (
                f"{date_guard}"
                f"Get-ChildItem -Path '{remote_path}' "
                f"-Filter '{pattern}' -File {recurse} "
                f"-ErrorAction SilentlyContinue | "
                f"{'Where-Object { $_.CreationTimeUtc -gt $cutoff } | ' if newer_than else ''}"
                f"ForEach-Object {{ "
                f"[string]::Join('|',"
                f"$_.FullName,"
                f"$_.Length,"
                f"$_.CreationTimeUtc.ToString('o'),"
                f"$_.LastWriteTimeUtc.ToString('o'),"
                f"'NTFS'"
                f") }}"
            )

            cmd = f'powershell -NoProfile -Command "{ps_cmd}"'

            # Log the exact command executed (critical for debugging)
            self.log.entry(f"[META][WINDOWS] exec: {cmd}")

            try:
                _, stdout, stderr = self.ssh_client.exec_command(
                    cmd, timeout=k.HOST_BUSY_TIMEOUT
                )

                for raw in iter(stdout.readline, ""):
                    raw = raw.strip()
                    if not raw:
                        continue
                    
                    # Split received powershell data
                    parts = raw.split("|")
                    if len(parts) != 5:
                        self.log.debug(
                            "[META][WINDOWS] noisy or invalid line ignored: %r", raw
                        )
                        continue

                    fullpath, size, c_at, m_at, perms = parts

                    fullpath = fullpath.replace("\\", "/")

                    filename = os.path.basename(fullpath)
                    dirname  = os.path.dirname(fullpath)
                    _, ext   = os.path.splitext(filename)

                    results.append({
                        "NA_FULL_PATH": fullpath,
                        "NA_PATH": dirname,
                        "NA_FILE": filename,
                        "NA_EXTENSION": ext,
                        "VL_FILE_SIZE_KB": int(int(size) // 1024),
                        "DT_FILE_CREATED": _parse_ps_iso(c_at),
                        "DT_FILE_MODIFIED": _parse_ps_iso(m_at),
                        "DT_FILE_ACCESSED": None,
                        "NA_OWNER": "",
                        "NA_GROUP": "0",
                        "NA_PERMISSIONS": perms,
                    })

                err = stderr.read().decode("utf-8", errors="ignore").strip()
                if err:
                    self.log.warning(f"[META][WINDOWS] STDERR: {err}")

            except Exception as e:
                self.log.error(f"[META][WINDOWS] discovery failed: {e}")
                return []

        else:
            self.log.error(f"[META] Unsupported OS '{os_type}'")
            return []

        elapsed = time.monotonic() - start

        # -----------------------------------------------------------
        # Log final summary (single authoritative line)
        # -----------------------------------------------------------
        self.log.entry(
            f"[META] Traversal completed | os={os_type} | "
            f"files={len(results)} | time={elapsed:.2f}s"
        )

        return results




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
    

    def get_config(self, timeout: int = 60) -> bool:
        """
        Load daemon configuration exclusively from the remote indexerD.cfg via SFTP.
        Timeout is enforced manually because sftp_conn.read() has no built-in timeout.
        """

        try:
            self.log.entry(f"[CONFIG] Reading remote cfg: {k.DAEMON_CFG_FILE}")

            # ----------------------------------------------
            # Execute SFTP read inside a worker thread
            # ----------------------------------------------
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    self.sftp_conn.read, k.DAEMON_CFG_FILE, "r"
                )
                cfg_content = future.result(timeout=timeout)

            # ----------------------------------------------
            # Validate response
            # ----------------------------------------------
            if not cfg_content:
                raise FileNotFoundError("Remote configuration file is empty or unreadable.")

            cfg, _ = parse_cfg(cfg_content)
            self.config = cfg

            self.log.entry("[CONFIG] Remote configuration loaded successfully.")
            return True

        # TIMEOUT
        except TimeoutError:
            self.log.error(
                f"[CONFIG] Timeout (> {timeout}s) while reading remote config. "
                "Closing SFTP connection."
            )
            try:
                self.sftp_conn.close()
            except Exception:
                pass
            return False

        # FILE NOT FOUND
        except FileNotFoundError as e:
            self.log.error(f"[CONFIG] File not found: {e}")
            return False

        # OTHER ERRORS
        except Exception as e:
            self.log.error(f"[CONFIG] Failed to load configuration: {e}")
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
            

    def get_metadata_files(
        self,
        host_id: int,
        filter_obj: Filter,
        callBackFileTask,
        callBackFileTaskHistory,
        callBackGetLastDBDate,
    ):
        """
        High-level metadata discovery orchestrator.

        This function represents the FINAL stage of discovery and is responsible
        for producing normalized file metadata ready for persistence.

        Design principles:
            • Traversal-driven (metadata is collected during filesystem walk)
            • OS-aware (Linux / Windows handled transparently)
            • Incremental-first (avoids reprocessing historical data)
            • DB-aware (deduplication is centralized in Python)
            • Stateless on the remote host (no temp files, no caching)

        Execution flow:
            1) Resolve discovery parameters (path, pattern, incremental cutoff).
            2) Perform recursive filesystem traversal WITH metadata emission.
            3) Apply bulk DB deduplication using FILE_TASK and FILE_TASK_HISTORY.
            4) Apply logical filters (FILE / RANGE / LAST / ALL).
            5) Return clean metadata structures for downstream pipelines.

        Important:
            • Remote side ONLY performs filesystem traversal.
            • All business rules remain in Python.
            • This guarantees identical semantics on Linux and Windows.
        """

        try:
            # -----------------------------------------------------------
            # Normalize filter object
            #
            # Allows callers to pass either a Filter instance or a raw dict.
            # Ensures a single, consistent interface downstream.
            # -----------------------------------------------------------
            if isinstance(filter_obj, dict):
                filter_obj = Filter(filter_obj, log=self.log)

            mode  = (filter_obj.data.get("mode")  or "").upper()
            agent = (filter_obj.data.get("agent") or "").upper()

            # -----------------------------------------------------------
            # Resolve discovery scope
            #
            # remote_dir → root directory for traversal
            # pattern    → filename glob (*.bin, override, etc.)
            # -----------------------------------------------------------
            remote_dir = filter_obj.data.get(
                "file_path", k.DEFAULT_DATA_FOLDER
            )
            pattern = filter_obj._build_pattern()

            # -----------------------------------------------------------
            # Incremental discovery cutoff
            #
            # Used to avoid reprocessing files older than the last
            # successful discovery (except in FILE mode).
            # -----------------------------------------------------------
            newer_than = None
            if mode != Filter.MODE_FILE:
                last_dt = callBackGetLastDBDate(host_id)
                if last_dt:
                    newer_than = last_dt.strftime("%Y-%m-%d %H:%M:%S")
            if mode == Filter.MODE_REDISCOVERY:
                newer_than = None  # Explicit override for full scan

            self.log.entry(
                f"[META] Discovery start | host={host_id} | agent={agent} | "
                f"mode={mode} | path={remote_dir} | pattern={pattern} | "
                f"incremental={'yes' if newer_than else 'no'}"
            )

            # -----------------------------------------------------------
            # Metadata discovery backend
            #
            # REMOTE → legacy/special mode: read explicit file list
            # LOCAL  → traversal-driven discovery with metadata emission
            # -----------------------------------------------------------
            if agent == "REMOTE":
                self.get_config()
                raw = self.sftp_conn.read(self.config["DUE_BACKUP"], mode="r")
                if not raw:
                    return []

                # REMOTE mode does NOT collect real metadata.
                # It only envelopes paths into a minimal structure.
                metadata = []
                for line in raw.decode("utf-8", errors="ignore") \
                                .replace("\x00", "") \
                                .splitlines():
                    line = line.strip()
                    if not line:
                        continue

                    filename = os.path.basename(line)
                    dirname  = os.path.dirname(line)
                    _, ext   = os.path.splitext(filename)

                    metadata.append({
                        "NA_FULL_PATH": line.replace("\\", "/"),
                        "NA_PATH": dirname.replace("\\", "/"),
                        "NA_FILE": filename,
                        "NA_EXTENSION": ext,
                        "VL_FILE_SIZE_KB": 0,
                        "DT_FILE_CREATED": None,
                        "DT_FILE_MODIFIED": None,
                        "DT_FILE_ACCESSED": None,
                        "NA_OWNER": "",
                        "NA_GROUP": "0",
                        "NA_PERMISSIONS": "",
                    })

            elif agent == "LOCAL":
                # Core path: recursive traversal with native OS backend
                metadata = self.sftp_conn.sftp_find_files_with_metadata(
                    remote_path=remote_dir,
                    pattern=pattern,
                    recursive=True,
                    newer_than=newer_than,
                )
            else:
                self.log.error(f"[META] Invalid agent '{agent}'")
                return []

            if not metadata:
                self.log.entry("[META] No files discovered")
                return []

            self.log.entry(
                f"[META] Discovery completed | files={len(metadata)}"
            )

            # -----------------------------------------------------------
            # FILE mode shortcut
            #
            # In FILE mode, DB deduplication is intentionally bypassed.
            # Caller explicitly controls the file list.
            # -----------------------------------------------------------
            if mode == Filter.MODE_FILE:
                filtered = filter_obj.evaluate_metadata(metadata)
                self.log.entry(
                    f"[META] FILE mode | returned={len(filtered)}"
                )
                return filtered

            # -----------------------------------------------------------
            # Bulk DB deduplication
            #
            # Uses preloaded FILE_TASK and FILE_TASK_HISTORY sets.
            # Avoids N database lookups and guarantees O(1) checks.
            # -----------------------------------------------------------
            task_names      = set(callBackFileTask(host_id))
            task_hist_names = set(callBackFileTaskHistory(host_id))

            before = len(metadata)

            filtered = [
                m for m in metadata
                if m.get("NA_FILE") not in task_names
                and m.get("NA_FILE") not in task_hist_names
            ]

            self.log.entry(
                f"[META] Deduplication | before={before} | after={len(filtered)}"
            )

            if not filtered:
                return []

            # -----------------------------------------------------------
            # Logical metadata filters
            #
            # Applies RANGE / LAST / ALL semantics using Filter rules.
            # -----------------------------------------------------------
            final = filter_obj.evaluate_metadata(filtered)

            self.log.entry(
                f"[META] Final result | returned={len(final)}"
            )

            return final

        except Exception as e:
            # Catch-all guard: discovery must NEVER crash the worker
            self.log.error(f"[META] Unexpected error: {e}")
            return []




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
    MODE_REDISCOVERY = "REDISCOVERY"  # reserved for future use

    VALID_MODES = (
        MODE_NONE,
        MODE_ALL,
        MODE_FILE,
        MODE_RANGE,
        MODE_LAST,
        MODE_REDISCOVERY,
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
            file_path=k.DEFAULT_DATA_FOLDER,
            agent="local",
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
            "file_path": f.get("file_path",k.DEFAULT_DATA_FOLDER),
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
            Filter.MODE_RANGE:          {"start_date", "end_date", "extension", "agent", "file_path"},
            Filter.MODE_FILE:           {"file_name", "extension", "agent","file_path"},
            Filter.MODE_LAST:           {"last_n_files", "extension", "agent","file_path"},
            Filter.MODE_ALL:            {"extension", "agent","file_path"},
            Filter.MODE_NONE:           {"extension", "agent","file_path"},
            Filter.MODE_REDISCOVERY:    {"extension", "agent","file_path"},
        }

        # --- Nullify unused fields ---
        all_fields = {"start_date", "end_date", "last_n_files", "extension", "file_name", "agent","file_path"}
        keep = active_fields.get(f["mode"], set())
        for key in all_fields - keep:
            f[key] = None

    # ------------------------------------------------------------------
    # Pattern Builder
    # ------------------------------------------------------------------
    def _build_pattern(self) -> str:
        """
        Safely construct a file matching pattern for discovery operations.

        Resolves interactions between 'file_name' and 'extension' fields while
        preventing malformed expressions such as '*.bin.bin'.

        Rules:
            - If file_name is provided:
                - If file_name has an extension → use as-is.
                - If file_name has no extension and extension exists → append it.
                - Otherwise, use file_name unchanged.

            - If file_name is not provided:
                - FILE mode → return "*".
                - Other modes:
                    - If extension exists → "*.<ext>"
                    - Otherwise           → "*"

        Additional Behavior:
            - Ensures wildcard '*' prefix if missing.
            - Normalizes extension by adding a leading dot if needed.
            - Strips stray quotes.
        """

        file_name = self.data.get("file_name")
        extension = self.data.get("extension")
        mode      = (self.data.get("mode") or "").upper()

        # Normalize inputs
        if file_name:
            file_name = file_name.strip().replace('"', "").replace("'", "")
        if extension:
            extension = extension.strip().replace('"', "").replace("'", "")

        # --------------------------------------------------------------
        # FILE mode
        # --------------------------------------------------------------
        if mode == Filter.MODE_FILE:
            if not file_name:
                return "*"

            # ensure wildcard prefix
            if not any(file_name.startswith(p) for p in ("*", "?")):
                file_name = "*" + file_name

            base, ext_in_name = os.path.splitext(file_name)

            if ext_in_name:
                return file_name

            if extension:
                if not extension.startswith("."):
                    extension = "." + extension
                return file_name + extension

            return file_name

        # --------------------------------------------------------------
        # Other modes
        # --------------------------------------------------------------
        if file_name:
            if not any(file_name.startswith(p) for p in ("*", "?")):
                file_name = "*" + file_name

            base, ext_in_name = os.path.splitext(file_name)

            if ext_in_name:
                return file_name

            if extension:
                if not extension.startswith("."):
                    extension = "." + extension
                return file_name + extension

            return file_name

        # --------------------------------------------------------------
        # No file_name
        # --------------------------------------------------------------
        if extension:
            if not extension.startswith("."):
                extension = "." + extension
            return "*" + extension

        return "*"



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
    def evaluate_database(
        self,
        host_id: int,
        search_type: Optional[int] = None,
        search_status: Optional[Union[int, List[int]]] = None,
        file_list: Optional[List[Any]] = None,
    ) -> Dict[str, Any]:
        """
        Builds SQL filtering metadata for FILE_TASK updates.

        Returns:
            {
                "where": { ... },
                "extra_sql": "ORDER BY ... LIMIT ...",
                "msg_prefix": "Backup Pending"
            }
        """

        mode = (self.data.get("mode") or "").upper()

        # ============================================================
        # Base SQL WHERE clause
        # ============================================================
        where: Dict[str, Any] = {"FK_HOST": host_id}

        # NU_TYPE
        if search_type is not None:
            where["NU_TYPE"] = search_type

        # NU_STATUS (int or list -> __in)
        if search_status is not None:
            if isinstance(search_status, (list, tuple)):
                where["NU_STATUS__in"] = list(search_status)
            else:
                where["NU_STATUS"] = search_status

        # ============================================================
        # Extension filter
        # ============================================================
        extension = self.data.get("extension")
        if isinstance(extension, str):
            extension = extension.strip().lower() or None
        if extension:
            where["NA_EXTENSION__like"] = f"%{extension}"

        # ============================================================
        # Extra SQL (ORDER BY, LIMIT)
        # ============================================================
        extra_sql = ""

        # ============================================================
        # msg_prefix (always computed for valid modes)
        # ============================================================
        msg_prefix = _compose_message(
            search_type, search_status, "", "", prefix_only=True
        )

        # ============================================================
        # MODE = ALL
        # ============================================================
        if mode == Filter.MODE_ALL:
            return {
                "where": where,
                "extra_sql": extra_sql,
                "msg_prefix": msg_prefix
            }

        # ============================================================
        # MODE = NONE
        # ============================================================
        if mode == Filter.MODE_NONE or mode==Filter.MODE_REDISCOVERY:
            return {"where": None, "extra_sql": "", "msg_prefix": None}

        # ============================================================
        # MODE = FILE
        # ============================================================
        if mode == Filter.MODE_FILE:

            if not file_list:
                return {"where": None, "extra_sql": "", "msg_prefix": None}

            normalized = []

            for item in file_list:

                # -----------------------------
                # String: filename or path
                # -----------------------------
                if isinstance(item, str):
                    normalized.append(os.path.basename(item))
                    continue

                # -----------------------------
                # Dict: metadata from daemon
                # -----------------------------
                if isinstance(item, dict):

                    # Priority: NA_FULL_PATH
                    if "NA_FULL_PATH" in item:
                        normalized.append(os.path.basename(item["NA_FULL_PATH"]))
                        continue

                    # Fallback: NA_FILE
                    if "NA_FILE" in item:
                        normalized.append(item["NA_FILE"])
                        continue

                    # Fallback: NA_HOST_FILE_NAME
                    if "NA_HOST_FILE_NAME" in item:
                        normalized.append(item["NA_HOST_FILE_NAME"])
                        continue

                    continue  # ignore invalid dicts

                # otherwise ignore invalid item types

            if not normalized:
                return {"where": None, "extra_sql": "", "msg_prefix": None}

            where["NA_HOST_FILE_NAME__in"] = normalized

            return {
                "where": where,
                "extra_sql": extra_sql,
                "msg_prefix": msg_prefix
            }

        # ============================================================
        # MODE = RANGE
        # ============================================================
        if mode == Filter.MODE_RANGE:

            start = self.data.get("start_date")
            end = self.data.get("end_date")

            if start and end:
                where["DT_FILE_CREATED__between"] = (start, end)
            elif start:
                where["DT_FILE_CREATED__gte"] = start
            elif end:
                where["DT_FILE_CREATED__lte"] = end
            else:
                return {"where": None, "extra_sql": "", "msg_prefix": None}

            return {
                "where": where,
                "extra_sql": extra_sql,
                "msg_prefix": msg_prefix
            }

        # ============================================================
        # MODE = LAST   (N most recent by DT_FILE_CREATED)
        # ============================================================
        if mode == Filter.MODE_LAST:
            last_n = int(self.data.get("last_n_files", 0))

            if last_n <= 0:
                return {"where": None, "extra_sql": "", "msg_prefix": None}

            extra_sql = f"ORDER BY DT_FILE_CREATED DESC LIMIT {last_n}"

            return {
                "where": where,
                "extra_sql": extra_sql,
                "msg_prefix": msg_prefix
            }

        # ============================================================
        # Fallback (should not happen, but safe)
        # ============================================================
        return {
            "where": where,
            "extra_sql": extra_sql,
            "msg_prefix": msg_prefix
        }
        
    def evaluate_metadata(self, metadata_list: list[dict]) -> list[dict]:
        """
        Apply secondary metadata-based filters (RANGE, LAST) while also enforcing
        basic file sanity checks:
            1. Minimum file size
            2. Minimum file age (e.g., ignore files modified less than X minutes ago)

        FILE, ALL, NONE → no timestamp filtering besides the basic protections.
        RANGE → applies start/end date interval.
        LAST → selects the last N based on timestamp.
        """

        # ----------------------------------------------------------------------
        # Basic protections (non-mutating)
        # ----------------------------------------------------------------------

        # 1. Minimum size filter
        filtered_metadata = [
            m for m in metadata_list
            if (m.get("VL_FILE_SIZE_KB") or 0) >= k.MIN_FILE_SIZE_KB
        ]

        if not filtered_metadata:
            return []

        # 2. Minimum age filter
        # --------------------------------------------------------------
        # Files modified within the last X minutes are ignored.
        # Prevents partial/incomplete files from leaking into backup.
        # --------------------------------------------------------------
        MIN_AGE_MINUTES = getattr(k, "MIN_FILE_AGE_MINUTES", 30)  # default = 30
        age_threshold = datetime.now() - timedelta(minutes=MIN_AGE_MINUTES)

        filtered_metadata = [
            m for m in filtered_metadata
            if (m.get("DT_FILE_CREATED") and m["DT_FILE_CREATED"] <= age_threshold)
        ]

        if not filtered_metadata:
            return []

        # From here on, only use the filtered list
        metadata_list = filtered_metadata

        # ----------------------------------------------------------------------
        # Extract input parameters
        # ----------------------------------------------------------------------
        mode      = (self.data.get("mode") or "").upper()
        extension = (self.data.get("extension") or "").lower()

        # ======================================================================
        # 1. FILE / ALL / NONE (only extension filter applies)
        # ======================================================================
        if mode in ("FILE", "ALL", "NONE"):
            if extension:
                return [
                    m for m in metadata_list
                    if (m.get("NA_EXTENSION") or "").lower() == extension
                ]
            return metadata_list

        # ======================================================================
        # 2. RANGE mode
        # ======================================================================
        if mode == "RANGE":

            start_raw = self.data.get("start_date")
            end_raw   = self.data.get("end_date")

            start = None
            end = None

            # Convert dates
            try:
                if isinstance(start_raw, datetime):
                    start = start_raw
                elif isinstance(start_raw, str) and start_raw.strip():
                    start = datetime.fromisoformat(start_raw)
            except:
                pass

            try:
                if isinstance(end_raw, datetime):
                    end = end_raw
                elif isinstance(end_raw, str) and end_raw.strip():
                    end = datetime.fromisoformat(end_raw)
            except:
                pass

            filtered = []

            # Apply RANGE logic
            for m in metadata_list:
                ts = m.get("DT_FILE_CREATED")
                if not ts:
                    continue

                if start and not end:
                    if ts >= start:
                        filtered.append(m)
                    continue

                if end and not start:
                    if ts <= end:
                        filtered.append(m)
                    continue

                if start and end:
                    if start <= ts <= end:
                        filtered.append(m)
                    continue

                filtered.append(m)

            # Extension enforcement
            if extension:
                filtered = [
                    m for m in filtered
                    if (m.get("NA_EXTENSION") or "").lower() == extension
                ]

            return filtered

        # ======================================================================
        # 3. LAST mode
        # ======================================================================
        if mode == "LAST":
            last_n = int(self.data.get("last_n") or 0)

            ordered = sorted(
                metadata_list,
                key=lambda m: m.get("DT_FILE_CREATED") or 0
            )

            if extension:
                ordered = [
                    m for m in ordered
                    if (m.get("NA_EXTENSION") or "").lower() == extension
                ]

            if last_n > 0:
                return ordered[-last_n:]

            return ordered

        # ======================================================================
        # 4. Unknown mode
        # ======================================================================
        if extension:
            return [
                m for m in metadata_list
                if (m.get("NA_EXTENSION") or "").lower() == extension
            ]

        return metadata_list


class BinValidationError(ValueError):
    """
    Raised when BIN semantic validation fails.
    Domain-level error (fatal validation).
    """
    pass

class ErrorHandler:
    """
    Centralized error tracking helper for microservices.

    Stores error state across multiple stages and provides utility methods for
    checking, logging, and retrieving structured error messages.

    Usage:
        err = ErrorHandler(log)
        err.set("Discovery failed", stage="DISCOVERY", exc=e)

        if err.triggered:
            err.log_error(host_id=..., task_id=...)
    """

    def __init__(self, log):
        self.logger = log          # <-- RENOMEADO
        self.reason = None
        self.stage = None
        self.exc = None

    def set(self, reason: str, stage: str = None, exc: Exception = None):
        """Register an error once."""
        if not self.reason:
            self.reason = reason
            self.stage = stage
            self.exc = exc

    @property
    def triggered(self) -> bool:
        return self.reason is not None

    @property
    def msg(self) -> str:
        if self.stage:
            return f"{self.stage}: {self.reason}"
        return self.reason or ""

    def log_error(self, host_id=None, task_id=None):
        """Unified logging format for errors."""
        parts = ["[ERROR_HANDLER]"]

        if self.stage:
            parts.append(f"[{self.stage}]")

        if host_id is not None:
            parts.append(f"[HOST={host_id}]")

        if task_id is not None:
            parts.append(f"[TASK={task_id}]")

        parts.append(self.reason or "Unknown error")

        if self.exc:
            parts.append(f"Exception: {repr(self.exc)}")

        self.logger.error(" ".join(parts))
        
    def format_error(self) -> str:
        """
        Return a compact, structured error string
        suitable for persistence (DB, history, audit).
        """
        if not self.triggered:
            return ""

        exc_type = type(self.exc).__name__ if self.exc else "Unknown"

        parts = ["[ERROR]"]

        if self.stage:
            parts.append(f"[stage={self.stage}]")

        parts.append(f"[type={exc_type}]")

        if self.reason:
            parts.append(self.reason)

        return " ".join(parts)



class TimeoutError(Exception):
    """Raised when a function exceeds the allowed timeout."""
    pass

def run_with_timeout(func, timeout: float):
    """
    Execute `func()` with a timeout using a global ThreadPoolExecutor.

    Benefits:
        - No thread leaking (all threads reused)
        - Real timeout control
        - Exceptions pass-through
        - Same signature you were already using

    Raises:
        TimeoutError
        Exception forwarded from func()
    """
    future = _TIMEOUT_EXECUTOR.submit(func)

    try:
        return future.result(timeout=timeout)

    except FuturesTimeoutError:
        raise TimeoutError(f"Operation timed out after {timeout} seconds")

    except Exception as e:
        raise e

  
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
def parse_socket_message(
    peername: Tuple[str, int],
    data: str,
    log: Optional[log] = None,
) -> Dict[str, Any]:
    """
    Parse a control message coming from a TCP socket.

    Expected payload (JSON):
    {
        "query_tag": str,
        "host_id": int,
        "host_uid": str,
        "host_add": str,
        "host_port": int,
        "user": str,
        "passwd": str,
        "filter": dict | str (JSON string)
    }
    """

    peer_ip, peer_port = peername

    try:
        payload = json.loads(data)

        # --------------------------------------------------------------
        # Mandatory fields
        # --------------------------------------------------------------
        command   = payload.get("query_tag")
        host_id   = int(payload.get("host_id"))
        host_uid  = payload.get("host_uid")
        host_addr = payload.get("host_add")
        host_port = int(payload.get("host_port"))
        user      = payload.get("user")
        password  = payload.get("passwd")

        # --------------------------------------------------------------
        # Filter normalization
        # --------------------------------------------------------------
        filter_raw = payload.get("filter")

        if isinstance(filter_raw, dict):
            filter_dict = filter_raw
        elif isinstance(filter_raw, str):
            try:
                filter_dict = json.loads(filter_raw)
            except Exception:
                filter_dict = getattr(k, "NONE_FILTER", {"mode": "NONE"})
        else:
            filter_dict = getattr(k, "NONE_FILTER", {"mode": "NONE"})

        return {
            "peer": {"ip": peer_ip, "port": peer_port},
            "command": command,
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
            log.entry(f"[parse_socket_message] JSON parse failed: {e} | raw={data}")

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

    The caller must close both `daemon` and `sftp_conn` after use.
    Typical usage is inside a try/finally cleanup block.

    Expected host dictionary format:
        {
            "HOST__ID_HOST": ...,
            "HOST__NA_HOST_NAME": ...,
            "HOST__NA_HOST_ADDRESS": ...,
            "HOST__NA_HOST_PORT": ...,
            "HOST__NA_HOST_USER": ...,
            "HOST__NA_HOST_PASSWORD": ...
        }

    Args:
        host (dict): Dictionary containing host metadata, usually obtained
            from DB JOIN operations via `_select_custom()`.
        log: Shared logger instance.

    Returns:
        Tuple[sftpConnection, hostDaemon]:
            A live SFTP connection and a hostDaemon object.
    """

    # --------------------------------------------------------------
    # Extract required HOST fields (raises KeyError if missing)
    # --------------------------------------------------------------
    try:
        host_uid  = host["HOST__NA_HOST_NAME"]
        host_addr = host["HOST__NA_HOST_ADDRESS"]
        port      = int(host["HOST__NA_HOST_PORT"])
        user      = host["HOST__NA_HOST_USER"]
        password  = host["HOST__NA_HOST_PASSWORD"]
    except KeyError as e:
        missing = str(e)
        log.error(f"[INIT] Missing field in host metadata: {missing}")
        raise

    # --------------------------------------------------------------
    # Create SFTP connection object
    # --------------------------------------------------------------
    sftp_conn = sftpConnection(
        host_uid=host_uid,
        host_addr=host_addr,
        port=port,
        user=user,
        password=password,
        log=log,
    )

    # --------------------------------------------------------------
    # Create daemon associated with the same SFTP session
    # --------------------------------------------------------------
    daemon = hostDaemon(
        sftp_conn=sftp_conn,
        log=log,
    )

    return sftp_conn, daemon


def _random_jitter_sleep() -> None:
    """Small random delay to reduce race conditions between workers."""
    time.sleep(random.uniform(0.5, k.MAX_HOST_TASK_WAIT_TIME))
    
def _compose_message(
    task_type: int,
    task_status: int,
    path: Optional[str] = None,
    name: Optional[str] = None,
    *,
    prefix_only: bool = False
) -> str:
    """
    Build a standardized NA_MESSAGE for FILE_TASK_HISTORY.

    Rules:
    - Messages describe ONLY task state transitions
    - Deterministic and short
    - Error details are handled externally (ErrorHandler)
    - Path/name are optional and used only when relevant
    """

    # -------------------------------------------------
    # Task type
    # -------------------------------------------------
    if task_type == k.FILE_TASK_BACKUP_TYPE:
        type_msg = "Backup"
    elif task_type == k.FILE_TASK_DISCOVERY:
        type_msg = "Discovery"
    else:
        type_msg = "Processing"

    # -------------------------------------------------
    # Status
    # -------------------------------------------------
    if task_status == k.TASK_PENDING:
        status_msg = "Pending"
    elif task_status == k.TASK_DONE:
        status_msg = "Done"
    elif task_status == k.TASK_RUNNING:
        status_msg = "Running"
    elif task_status == k.TASK_ERROR:
        status_msg = "Error"
    else:
        status_msg = f"Status-{task_status}"

    prefix = f"{type_msg} {status_msg}"

    if prefix_only:
        return prefix

    # -------------------------------------------------
    # SUCCESS / PENDING / RUNNING / ERROR (state only)
    # -------------------------------------------------
    if path and name:
        return f"{prefix} of file {path}/{name}"

    return prefix


    
def _parse_ps_iso(ts: str) -> datetime:
    """
    Parse PowerShell ISO timestamp into naive local datetime.

    PowerShell emits up to 7 fractional digits (ticks, 100ns),
    which Python does not accept. This function:
        • truncates to microseconds (6 digits)
        • removes timezone information
    """
    if "." in ts:
        head, tail = ts.split(".", 1)

        # tail example: "4475322-03:00"
        frac = tail[:6]  # microseconds
        rest = tail[6:]

        # remove timezone
        if "+" in rest:
            rest = rest.split("+", 1)[0]
        elif "-" in rest:
            rest = rest.split("-", 1)[0]

        ts = f"{head}.{frac}"

    else:
        ts = ts.split("+", 1)[0].split("-", 1)[0]

    return datetime.fromisoformat(ts)


