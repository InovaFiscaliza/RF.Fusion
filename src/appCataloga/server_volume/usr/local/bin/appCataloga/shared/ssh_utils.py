from __future__ import annotations

import sys
import os
import stat
import time
import paramiko
from datetime import datetime
from typing import List, Optional, Union
from enum import Enum

from .logging_utils import log
from shared.file_metadata import FileMetadata
from . import legacy, tools


# ---------------------------------------------------------------------
# Config import path (as in original code). We keep the behavior so the
# module remains drop-in compatible with existing deployments.
# ---------------------------------------------------------------------
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)

CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402  (must be available at runtime)

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
        """Check remote file existence."""
        try:
            self.sftp.lstat(filename)
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            self.log.error(
                f"Error checking '{filename}' in "
                f"'{self.host_uid}'({self.host_addr}). {e}"
            )
            raise

    def touch(self, filename: str) -> None:
        """Create a zero-length file remotely (like 'touch')."""
        try:
            if self.test(filename):
                return
            with self.sftp.open(filename, "w"):
                pass
        except Exception as e:
            self.log.error(
                f"Error touching '{filename}' in "
                f"'{self.host_uid}'({self.host_addr}). {e}"
            )
            raise

    def append(self, filename: str, content: str) -> None:
        """Append text content to a remote file."""
        try:
            with self.sftp.open(filename, "a", encoding="utf-8") as f:
                f.write(content)
        except Exception as e:
            self.log.error(
                f"Error appending to '{filename}' in "
                f"'{self.host_uid}'({self.host_addr}). {e}"
            )
            raise

    def write(self, filename: str, content: str) -> None:
        """Write (overwrite) text content to a remote file."""
        try:
            with self.sftp.open(filename, "w") as f:
                f.write(content)
        except Exception as e:
            self.log.error(
                f"Error writing '{filename}' in "
                f"'{self.host_uid}'({self.host_addr}). {e}"
            )
            raise

    def read(self, filename: str, mode: str = "r") -> Union[str, bytes]:
        """Read content from a remote file (text or binary)."""
        try:
            if "b" in mode:
                with self.sftp.open(filename, "rb") as f:
                    return f.read()
            else:
                with self.sftp.open(filename, "r") as f:
                    return f.read()
        except FileNotFoundError:
            self.log.error(
                f"File '{filename}' not found in "
                f"'{self.host_uid}'({self.host_addr})"
            )
            return "" if "b" not in mode else b""
        except Exception as e:
            self.log.error(
                f"Error reading '{filename}' in "
                f"'{self.host_uid}'({self.host_addr}). {e}"
            )
            raise

    def remove(self, filename: str) -> None:
        """Remove a remote file if it exists."""
        try:
            self.sftp.remove(filename)
        except FileNotFoundError:
            self.log.error(
                f"File '{filename}' not found in "
                f"'{self.host_uid}'({self.host_addr})"
            )
        except Exception as e:
            self.log.error(
                f"Error removing '{filename}' in "
                f"'{self.host_uid}'({self.host_addr}). {e}"
            )
            raise

    def is_connected(self) -> bool:
        """Return True if the SSH/SFTP connection is still alive."""
        try:
            if not self.ssh_client:
                return False

            transport = self.ssh_client.get_transport()
            if not transport or not transport.is_active():
                return False

            if self.sftp:
                try:
                    self.sftp.listdir(".")
                except Exception:
                    pass

            return True
        except Exception:
            return False

    def close(self) -> None:
        """Close SFTP and SSH sessions (best-effort)."""
        try:
            self.sftp.close()
            self.ssh_client.close()
        except Exception as e:
            self.log.error(
                f"Error closing SFTP/SSH for "
                f"'{self.host_uid}'({self.host_addr}). {e}"
            )

    # =================================================================
    # OS Detection
    # =================================================================
    def detect_remote_os(self):
        """
        Detect the operating system of a remote host via SSH.

        Detection strategy:
            1) Attempt to identify a Unix/Linux system using `uname -s`.
            2) If that fails, detect Windows using PowerShell.

        Returns:
            str: "linux" or "windows"
        """

        # ---------------------------------------------------------------
        # 1) Linux detection
        # ---------------------------------------------------------------
        try:
            stdin, stdout, stderr = self.ssh_client.exec_command(
                "uname -s", timeout=5
            )
            uname = stdout.read().decode(errors="ignore").strip().lower()
            if "linux" in uname:
                return "linux"
        except Exception:
            pass

        # ---------------------------------------------------------------
        # 2) Windows detection
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

        return "linux"

    # =================================================================
    # Cross-platform discovery with metadata
    # =================================================================
    def sftp_find_files_with_metadata(
        self,
        remote_path: str,
        pattern: str,
        recursive: bool = True,
        newer_than: Optional[str] = None,
    ):
        """
        Perform a cross-platform remote filesystem traversal (Linux or Windows)
        and return a COMPLETE in-memory list of file metadata entries.

        ⚠️ IMPORTANT DESIGN NOTES ⚠️
        -----------------------------
        • This function intentionally RETURNS A FULL LIST.
        It does NOT stream, batch, or yield results.
        This behavior is REQUIRED for backward compatibility
        with the current discovery pipeline.

        • Memory usage can be high for hosts with a large number
        of files (e.g. CelPlan nodes). This is a KNOWN and ACCEPTED
        limitation at this stage of the refactor.

        • The only responsibility of this function is:
            - Remote traversal
            - Metadata extraction
            - Normalization into FileMetadata objects

        • NO filtering, deduplication, or business logic must be added here.
        Those concerns belong to higher layers (hostDaemon / discovery).

        • The returned list preserves the same semantic fields that were
        historically returned as dictionaries, but now uses FileMetadata
        objects for type safety and consistency.

        Args:
            remote_path (str):
                Base directory on the remote host to start traversal from.

            pattern (str):
                Filename pattern (glob-like) used for matching files.
                Example: "*.bin", "*.dbm"

            recursive (bool):
                Whether to recurse into subdirectories.
                NOTE: On Windows, recursion is handled by PowerShell.

            newer_than (Optional[str]):
                If provided, only files newer than this timestamp
                will be returned. Format must be compatible with:
                - Linux: find -newermt
                - Windows: PowerShell Get-Date

        Returns:
            list[FileMetadata]:
                A fully materialized list of FileMetadata objects.

        Raises:
            None explicitly.
            Any SSH or parsing error is logged and results in
            an empty list being returned.
        """

        # ------------------------------------------------------------
        # Normalize and sanitize input parameters
        # ------------------------------------------------------------
        # Remove trailing path separators to avoid duplicate slashes
        remote_path = remote_path.rstrip("/").rstrip("\\")

        # Sanitize pattern to avoid malformed shell / PowerShell calls
        pattern = pattern.strip().replace('"', "").replace("'", "")
        if not pattern.startswith("*"):
            pattern = "*" + pattern

        # Detect remote operating system (Linux or Windows)
        os_type = self.detect_remote_os()

        # NOTE:
        # This list is intentionally built in-memory.
        # DO NOT change this to a generator here.
        results: list[FileMetadata] = []

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

            # Build incremental discovery guard if requested
            newer = f'-newermt "{newer_than}"' if newer_than else ""

            # Use GNU find with a fixed, parseable output format
            # Field order is STRICT and MUST NOT be changed lightly
            cmd = (
                f"find {remote_path} -type f -iname '{pattern}' {newer} "
                "-printf '%p|%s|%C@|%T@|%A@|%U|%G|%m\n'"
            )

            self.log.entry(f"[META][LINUX] exec: {cmd}")

            try:
                _, stdout, stderr = self.ssh_client.exec_command(
                    cmd, timeout=k.HOST_BUSY_TIMEOUT
                )

                # Read stdout line-by-line to avoid buffering large outputs
                for raw in iter(stdout.readline, ""):
                    raw = raw.strip()
                    if not raw:
                        continue

                    # Expected format:
                    # fullpath|size|ctime|mtime|atime|uid|gid|mode
                    fullpath, size, c_at, m_at, a_at, uid, gid, mode = raw.split("|")

                    filename = os.path.basename(fullpath)
                    dirname = os.path.dirname(fullpath)
                    _, ext = os.path.splitext(filename)

                    # Create FileMetadata object with legacy-compatible fields
                    results.append(
                        FileMetadata(
                            NA_FULL_PATH=fullpath,
                            NA_PATH=dirname,
                            NA_FILE=filename,
                            NA_EXTENSION=ext,
                            VL_FILE_SIZE_KB=int(size) // 1024,
                            DT_FILE_CREATED=datetime.fromtimestamp(float(c_at)),
                            DT_FILE_MODIFIED=datetime.fromtimestamp(float(m_at)),
                            DT_FILE_ACCESSED=datetime.fromtimestamp(float(a_at)),
                            NA_OWNER=str(uid),
                            NA_GROUP=str(gid),
                            NA_PERMISSIONS=stat.filemode(int(mode, 8)),
                        )
                    )

                # Capture any stderr output for diagnostics
                err = stderr.read().decode("utf-8", errors="ignore").strip()
                if err:
                    self.log.warning(f"[META][LINUX] STDERR: {err}")

            except Exception as e:
                # Any failure here aborts discovery for this host
                self.log.error(f"[META][LINUX] discovery failed: {e}")
                return []

        # ============================================================
        # WINDOWS BACKEND
        # ============================================================
        elif os_type == "windows":

            recurse = "-Recurse" if recursive else ""
            date_guard = f"$cutoff = Get-Date '{newer_than}';" if newer_than else ""

            # PowerShell command designed to:
            # - List files only
            # - Optionally recurse
            # - Optionally apply incremental date filter
            # - Emit a pipe-separated, parseable line per file
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
            self.log.entry(f"[META][WINDOWS] exec: {cmd}")

            try:
                _, stdout, stderr = self.ssh_client.exec_command(
                    cmd, timeout=k.HOST_BUSY_TIMEOUT
                )

                for raw in iter(stdout.readline, ""):
                    raw = raw.strip()
                    if not raw:
                        continue

                    parts = raw.split("|")
                    if len(parts) != 5:
                        # Windows stdout can be noisy; ignore malformed lines
                        self.log.warning(
                            f"[META][WINDOWS] noisy or invalid line ignored: {raw}"
                        )
                        continue

                    fullpath, size, c_at, m_at, perms = parts
                    fullpath = fullpath.replace("\\", "/")

                    filename = os.path.basename(fullpath)
                    dirname = os.path.dirname(fullpath)
                    _, ext = os.path.splitext(filename)

                    results.append(
                        FileMetadata(
                            NA_FULL_PATH=fullpath,
                            NA_PATH=dirname,
                            NA_FILE=filename,
                            NA_EXTENSION=ext,
                            VL_FILE_SIZE_KB=int(size) // 1024,
                            DT_FILE_CREATED=tools.parse_ps_iso(c_at),
                            DT_FILE_MODIFIED=tools.parse_ps_iso(m_at),
                            DT_FILE_ACCESSED=None,
                            NA_OWNER="",
                            NA_GROUP="0",
                            NA_PERMISSIONS=perms,
                        )
                    )

                err = stderr.read().decode("utf-8", errors="ignore").strip()
                if err:
                    self.log.warning(f"[META][WINDOWS] STDERR: {err}")

            except Exception as e:
                self.log.error(f"[META][WINDOWS] discovery failed: {e}")
                return []

        # ============================================================
        # UNSUPPORTED OS
        # ============================================================
        else:
            self.log.error(f"[META] Unsupported OS '{os_type}'")
            return []

        elapsed = time.monotonic() - start

        self.log.entry(
            f"[META] Traversal completed | os={os_type} | "
            f"files={len(results)} | time={elapsed:.2f}s"
        )

        return results
    
    def iter_find_files_with_metadata(
        self,
        remote_path: str,
        pattern: str,
        *,
        recursive: bool = True,
        newer_than: Optional[str] = None,
        batch_size: int = 1000,
    ):
        """
        Stream remote filesystem metadata in FIXED-SIZE batches.

        This function is the lowest-level discovery primitive.
        It:
            - streams stdout line-by-line (no buffering of full output)
            - parses raw filesystem metadata
            - groups FileMetadata objects into bounded batches
            - yields each batch atomically

        Memory contract:
            - At most `batch_size` FileMetadata objects live at once.

        Yields:
            list[FileMetadata]
        """

        remote_path = remote_path.rstrip("/").rstrip("\\")
        pattern = pattern.strip().replace('"', "").replace("'", "")
        if not pattern.startswith("*"):
            pattern = "*" + pattern

        os_type = self.detect_remote_os()
        batch: list[FileMetadata] = []

        self.log.entry(
            f"[META][ITER] start | os={os_type} | path={remote_path} | "
            f"pattern={pattern} | batch_size={batch_size} | "
            f"incremental={'yes' if newer_than else 'no'}"
        )

        # ============================================================
        # LINUX
        # ============================================================
        if os_type == "linux":

            newer = f'-newermt "{newer_than}"' if newer_than else ""

            cmd = (
                f"find {remote_path} -type f -iname '{pattern}' {newer} "
                "-printf '%p|%s|%C@|%T@|%A@|%U|%G|%m\n'"
            )

            _, stdout, stderr = self.ssh_client.exec_command(
                cmd, timeout=k.HOST_BUSY_TIMEOUT
            )

            for raw in iter(stdout.readline, ""):
                raw = raw.strip()
                if not raw:
                    continue

                try:
                    fullpath, size, c_at, m_at, a_at, uid, gid, mode = raw.split("|")

                    filename = os.path.basename(fullpath)
                    dirname = os.path.dirname(fullpath)
                    _, ext = os.path.splitext(filename)

                    batch.append(
                        FileMetadata(
                            NA_FULL_PATH=fullpath,
                            NA_PATH=dirname,
                            NA_FILE=filename,
                            NA_EXTENSION=ext,
                            VL_FILE_SIZE_KB=int(int(size) // 1024),
                            DT_FILE_CREATED=datetime.fromtimestamp(float(c_at)),
                            DT_FILE_MODIFIED=datetime.fromtimestamp(float(m_at)),
                            DT_FILE_ACCESSED=datetime.fromtimestamp(float(a_at)),
                            NA_OWNER=str(uid),
                            NA_GROUP=str(gid),
                            NA_PERMISSIONS=stat.filemode(int(mode, 8)),
                        )
                    )

                except Exception as e:
                    self.log.warning(f"[META][LINUX] invalid line skipped: {raw} ({e})")
                    continue

                if len(batch) >= batch_size:
                    yield batch
                    batch = []

        # ============================================================
        # WINDOWS
        # ============================================================
        elif os_type == "windows":

            recurse = "-Recurse" if recursive else ""
            date_guard = f"$cutoff = Get-Date '{newer_than}';" if newer_than else ""

            ps_cmd = (
                f"{date_guard}"
                f"Get-ChildItem -Path '{remote_path}' -Filter '{pattern}' "
                f"-File {recurse} -ErrorAction SilentlyContinue | "
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
            _, stdout, stderr = self.ssh_client.exec_command(
                cmd, timeout=k.HOST_BUSY_TIMEOUT
            )

            for raw in iter(stdout.readline, ""):
                raw = raw.strip()
                if not raw:
                    continue

                parts = raw.split("|")
                if len(parts) != 5:
                    self.log.warning(f"[META][WINDOWS] noisy line ignored: {raw}")
                    continue

                fullpath, size, c_at, m_at, perms = parts
                fullpath = fullpath.replace("\\", "/")

                filename = os.path.basename(fullpath)
                dirname = os.path.dirname(fullpath)
                _, ext = os.path.splitext(filename)

                batch.append(
                    FileMetadata(
                        NA_FULL_PATH=fullpath,
                        NA_PATH=dirname,
                        NA_FILE=filename,
                        NA_EXTENSION=ext,
                        VL_FILE_SIZE_KB=int(int(size) // 1024),
                        DT_FILE_CREATED=tools.parse_ps_iso(c_at),
                        DT_FILE_MODIFIED=tools.parse_ps_iso(m_at),
                        DT_FILE_ACCESSED=None,
                        NA_OWNER="",
                        NA_GROUP="0",
                        NA_PERMISSIONS=perms,
                    )
                )

                if len(batch) >= batch_size:
                    yield batch
                    batch = []

        else:
            self.log.error(f"[META][ITER] Unsupported OS '{os_type}'")
            return

        # flush final
        if batch:
            yield batch

        self.log.entry("[META][ITER] completed")

