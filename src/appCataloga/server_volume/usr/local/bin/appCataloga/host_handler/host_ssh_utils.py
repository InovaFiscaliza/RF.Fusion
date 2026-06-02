"""
SSH and SFTP transport helpers for appCataloga.

This module wraps Paramiko with the conventions expected by the rest of the
project: durable connections, metadata-friendly file operations, and
cross-platform remote traversal helpers.
"""

from __future__ import annotations

import sys
import os
import shlex
import stat
import socket
import threading
import time
import paramiko
from collections.abc import Iterator
from datetime import datetime
from typing import Any, TypeAlias, TYPE_CHECKING

from shared import errors, tools
from shared.file_metadata import FileMetadata
from shared.logging_utils import log


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

if TYPE_CHECKING:
    from db.dbHandlerBKP import dbHandlerBKP



# =====================================================================
# Connectivity probe types and SSH probe helpers
# =====================================================================

ConnectivityProbePayload: TypeAlias = dict[str, Any]

def _probe_result(
    *,
    state: str,
    reason: str,
    icmp_online: bool,
    ssh_online: bool,
    error: str | None = None,
) -> ConnectivityProbePayload:
    return {
        "state": state,
        "reason": reason,
        "icmp_online": icmp_online,
        "ssh_online": ssh_online,
        "error": error,
    }


def _connect_short_ssh_probe(addr: str, port: int, user: str, password: str) -> None:
    """
    Attempt the short supervisory SSH login used by host probes.

    Intentionally raises the original Paramiko/socket exception so callers
    can classify the failure without losing stage-specific details.
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            hostname=addr,
            port=int(port),
            username=user,
            password=password,
            timeout=k.HOST_CHECK_SSH_PROBE_TIMEOUT_SEC,
            banner_timeout=k.HOST_CHECK_SSH_PROBE_TIMEOUT_SEC,
            auth_timeout=k.HOST_CHECK_SSH_PROBE_TIMEOUT_SEC,
            look_for_keys=False,
            allow_agent=False,
        )
    finally:
        try:
            client.close()
        except Exception:
            pass


def ssh_probe(addr: str, port: int, user: str, password: str) -> ConnectivityProbePayload:
    """
    Run the short supervisory SSH login for one ICMP-reachable address.

    Outcomes:
        - online:      SSH login succeeded
        - auth_error:  credentials rejected (AuthenticationException, non-timeout)
        - degraded:    everything else — auth timeout, no valid connections,
                       transport errors, generic failures

    Never raises.
    """
    try:
        _connect_short_ssh_probe(addr=addr, port=port, user=user, password=password)
        return _probe_result(
            state=k.HOST_CONN_ONLINE, reason="ssh_connect_ok",
            icmp_online=True, ssh_online=True,
        )
    except Exception as e:
        classification = errors.classify_ssh_connect_exc(e)
        return _probe_result(
            state=classification.state, reason=classification.reason,
            icmp_online=True, ssh_online=classification.ssh_online,
            error=str(e),
        )


def persist_auth_error(
    db: dbHandlerBKP,
    task: dict,
    detail: str,
    *,
    logger: Any,
) -> tuple[int, str]:
    """
    Suspend host-dependent work after an SSH authentication failure.

    Auth rejection is not transient; retries keep failing until credentials
    are fixed. Suspends all dependent queues and returns ERROR status for
    the caller to close the task.
    """
    next_count = max(0, int(task["host_check_error_count"] or 0)) + 1

    db.host_update(
        host_id=task["host_id"],
        reset=True,
        DT_LAST_CHECK=task["now"],
        DT_LAST_FAIL=task["now"],
        NU_HOST_CHECK_ERROR=next_count,
    )

    db.host_task_suspend_by_host(task["host_id"])
    db.file_task_suspend_by_host(task["host_id"])
    db.file_history_suspend_by_host(task["host_id"])

    logger.event(
        "host_auth_error_suspended",
        host_id=task["host_id"],
        task_id=task["task_id"],
        error_count=next_count,
        detail=detail,
    )

    return (k.TASK_ERROR, f"SSH authentication failed during connectivity confirmation | {detail}")


# =====================================================================
# SFTP Connection
# =====================================================================
class sftpConnection:
    """Light wrapper over Paramiko SSH/SFTP with convenience methods."""

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

        Raises:
            Exception: When connection to remote host fails.
        """

        self.log = log
        self.host_uid = host_uid
        self.host_addr = host_addr
        self.connect_addr = host_addr
        self.port = port
        self.user = user

        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy()
            )

            # Timeouts cover connection/auth only; data transfer timeouts are separate.
            self.ssh_client.connect(
                hostname=self.connect_addr,
                port=port,
                username=user,
                password=password,
                timeout=k.SSH_CONNECT_TIMEOUT,
                banner_timeout=k.SSH_BANNER_TIMEOUT,
                auth_timeout=k.SSH_AUTH_TIMEOUT,
                look_for_keys=False,
                allow_agent=False,
            )

            # Transport tuning matters here because some hosts stream large
            # directory listings and stay connected for long periods.
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

            self.sftp = self.ssh_client.open_sftp()

            self.log.entry(
                f"[SSH] Connected to {self.host_uid} "
                f"({self.host_addr} -> {self.connect_addr}:{self.port}) as {self.user}"
            )

        except Exception as e:
            classification = errors.classify_ssh_connect_exc(e)
            self.log.error(
                f"[SSH][{classification.reason}] Error initializing SSH to "
                f"'{self.host_uid}' ({self.host_addr} -> {self.connect_addr}): {e}"
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

    def read(self, filename: str, mode: str = "r") -> str | bytes:
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
    
    def read_cookie_list(self, filename: str) -> list[str]:
        """Read a 'list cookie' file (one item per line) from remote host.

        Returns non-empty stripped lines. Returns empty list if file is missing.
        """
        try:
            data = self.read(filename, "r")
            if not data:
                return []
            return [ln.strip() for ln in str(data).splitlines() if ln.strip()]
        except Exception as e:
            self.log.error(f"Error reading cookie list '{filename}' from '{self.host_uid}'. {e}")
            return []

    def write_cookie_list(self, filename: str, lines: list[str]) -> None:
        """Write a 'list cookie' file (one item per line) to remote host.

        Raises:
            Exception: On SFTP I/O errors.
        """
        try:
            content = "\n".join(lines) + "\n" if lines else ""
            self.write(filename, content)
        except Exception as e:
            self.log.error(f"Error writing cookie list '{filename}' to '{self.host_uid}'. {e}")
            raise

    
    def abort_transfer(self, reason: str | None = None) -> None:
        """Force-close the active SFTP/SSH transport during a stalled transfer."""
        if reason:
            self.log.event(
                "backup_transfer_abort",
                host=self.host_uid,
                address=self.host_addr,
                reason=reason,
            )

        try:
            if self.sftp:
                self.sftp.close()
        except Exception:
            pass

        try:
            if self.ssh_client:
                transport = self.ssh_client.get_transport()
                if transport:
                    transport.close()
        except Exception:
            pass

        try:
            if self.ssh_client:
                self.ssh_client.close()
        except Exception:
            pass

    def transfer(
        self,
        remote_file: str,
        local_file: str,
        *,
        max_seconds: float | None = None,
        stall_timeout_seconds: float | None = None,
        progress_poll_seconds: float | None = None,
        heartbeat_seconds: float | None = None,
    ) -> None:
        """Download a remote file to a local path with progress watchdogs.

        Raises:
            Exception: On SFTP I/O errors (e.g., permissions, network).
        """
        max_seconds = float(
            k.BACKUP_TRANSFER_MAX_SECONDS
            if max_seconds is None else max_seconds
        )
        stall_timeout_seconds = float(
            k.BACKUP_TRANSFER_STALL_TIMEOUT_SECONDS
            if stall_timeout_seconds is None else stall_timeout_seconds
        )
        progress_poll_seconds = max(
            0.1,
            float(
                k.BACKUP_TRANSFER_PROGRESS_POLL_SECONDS
                if progress_poll_seconds is None else progress_poll_seconds
            ),
        )
        heartbeat_seconds = float(
            k.BACKUP_TRANSFER_HEARTBEAT_SECONDS
            if heartbeat_seconds is None else heartbeat_seconds
        )

        started_at = time.monotonic()
        stop_event = threading.Event()
        state_lock = threading.Lock()
        state = {
            "bytes_transferred": 0,
            "remote_total_bytes": 0,
            "last_progress_at": started_at,
            "last_log_at": started_at,
            "last_local_size": 0,
            "abort_exc": None,
        }

        def record_progress(transferred: int, total: int) -> None:
            now = time.monotonic()
            with state_lock:
                if transferred > state["bytes_transferred"]:
                    state["bytes_transferred"] = transferred
                    state["last_progress_at"] = now
                if total > state["remote_total_bytes"]:
                    state["remote_total_bytes"] = total

        def watchdog() -> None:
            while not stop_event.wait(progress_poll_seconds):
                now = time.monotonic()

                # Cross-check with on-disk size: some SFTP implementations call
                # the progress callback less frequently than expected, so a
                # growing local file counts as proof of forward progress.
                try:
                    local_size = os.path.getsize(local_file)
                except OSError:
                    local_size = 0

                abort_exc = None
                with state_lock:
                    if local_size > state["last_local_size"]:
                        state["last_local_size"] = local_size
                        if local_size > state["bytes_transferred"]:
                            state["bytes_transferred"] = local_size
                        state["last_progress_at"] = now

                    elapsed = now - started_at
                    stalled_for = now - state["last_progress_at"]
                    transferred = state["bytes_transferred"]
                    total = state["remote_total_bytes"] or None

                    if heartbeat_seconds > 0 and (now - state["last_log_at"]) >= heartbeat_seconds:
                        state["last_log_at"] = now
                        if hasattr(self.log, "event"):
                            self.log.event(
                                "backup_transfer_progress",
                                host=self.host_uid,
                                remote_file=remote_file,
                                local_file=local_file,
                                transferred_bytes=transferred,
                                total_bytes=total,
                                elapsed_seconds=round(elapsed, 1),
                                stalled_for_seconds=round(stalled_for, 1),
                            )

                    if max_seconds > 0 and elapsed > max_seconds:
                        abort_exc = TimeoutError(
                            f"SFTP transfer exceeded {max_seconds:.0f}s without finishing"
                        )
                    elif stall_timeout_seconds > 0 and stalled_for > stall_timeout_seconds:
                        abort_exc = TimeoutError(
                            f"SFTP transfer stalled for {stalled_for:.1f}s without progress"
                        )

                    if abort_exc is not None and state["abort_exc"] is None:
                        state["abort_exc"] = abort_exc

                if abort_exc is None:
                    continue

                self.abort_transfer(reason=str(abort_exc))
                return

        watchdog_thread = None

        try:
            watchdog_thread = threading.Thread(
                target=watchdog,
                name=f"sftp-watchdog-{self.host_uid}",
                daemon=True,
            )
            watchdog_thread.start()
            self.sftp.get(remote_file, local_file, callback=record_progress)
        except Exception as e:
            stop_event.set()
            if watchdog_thread is not None:
                watchdog_thread.join(timeout=1.0)

            with state_lock:
                abort_exc = state["abort_exc"]

            if abort_exc is not None:
                raise abort_exc from e

            self.log.error(
                f"Error transferring '{remote_file}' from '{self.host_uid}'({self.host_addr}) to '{local_file}'. {e}"
            )
            raise
        finally:
            stop_event.set()
            try:
                if watchdog_thread is not None:
                    watchdog_thread.join(timeout=1.0)
            except Exception:
                pass

        # sftp.get returned without raising, but the watchdog may have raced
        # and set abort_exc just as the download finished. Honour the timeout
        # rather than treating a timed-out transfer as a success.
        with state_lock:
            abort_exc = state["abort_exc"]

        if abort_exc is not None:
            raise abort_exc
    
    
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
                    # Soft probe: can surface a stale transport before the
                    # keepalive does. Failure is swallowed because transport.is_active()
                    # is the decisive liveness signal; we do not want to report
                    # dead just because the working directory is inaccessible.
                    self.sftp.listdir(".")
                except Exception:
                    pass

            return True
        except Exception:
            return False

    def close(self) -> None:
        """Close SFTP and SSH sessions (best-effort)."""
        try:
            if self.sftp:
                self.sftp.close()
            if self.ssh_client:
                self.ssh_client.close()
        except Exception as e:
            self.log.error(
                f"Error closing SFTP/SSH for "
                f"'{self.host_uid}'({self.host_addr}). {e}"
            )
            
    def size(self, filename: str) -> int:
        """Return remote file size in bytes.

        Raises:
            FileNotFoundError: If file does not exist.
            Exception: On other SFTP errors.
        """
        try:
            return self.sftp.stat(filename).st_size
        except FileNotFoundError:
            raise
        except Exception as e:
            self.log.error(
                f"Error getting size of '{filename}' in "
                f"'{self.host_uid}'({self.host_addr}). {e}"
            )
            raise

    def read_file_metadata(self, filename: str) -> FileMetadata:
        """
        Read one authoritative metadata snapshot for a single remote file.

        Discovery and backup should agree on the meaning of extension, size,
        created time, and modified time. This helper therefore reuses the same
        OS-specific extraction rules as bulk discovery instead of guessing from
        a partial SFTP stat result.
        """
        normalized_path = filename.rstrip("/").rstrip("\\")
        os_type = self.detect_remote_os()

        # Linux metadata is collected through `find -printf` so backup and
        # discovery keep the same timestamp semantics (`%C@`, `%T@`, `%A@`).
        if os_type == "linux":
            quoted_path = shlex.quote(normalized_path)
            cmd = (
                f"find {quoted_path} -maxdepth 0 -type f "
                "-printf '%p|%s|%C@|%T@|%A@|%U|%G|%m\n'"
            )

            _, stdout, stderr = self.ssh_client.exec_command(
                cmd,
                timeout=k.HOST_BUSY_TIMEOUT,
            )
            raw = stdout.readline().strip()
            err = stderr.read().decode("utf-8", errors="ignore").strip()

            if err:
                self.log.warning(f"[META][LINUX][ONE] STDERR: {err}")

            if not raw:
                raise FileNotFoundError(f"Remote file not found: {normalized_path}")

            fullpath, size, c_at, m_at, a_at, uid, gid, mode = raw.split("|")
            filename_only = os.path.basename(fullpath)
            dirname = os.path.dirname(fullpath)
            _, ext = os.path.splitext(filename_only)

            return FileMetadata(
                NA_FULL_PATH=fullpath,
                NA_PATH=dirname,
                NA_FILE=filename_only,
                NA_EXTENSION=ext,
                VL_FILE_SIZE_KB=int(size) // 1024,
                DT_FILE_CREATED=datetime.fromtimestamp(float(c_at)),
                DT_FILE_MODIFIED=datetime.fromtimestamp(float(m_at)),
                DT_FILE_ACCESSED=datetime.fromtimestamp(float(a_at)),
                NA_OWNER=str(uid),
                NA_GROUP=str(gid),
                NA_PERMISSIONS=stat.filemode(int(mode, 8)),
            )

        # Windows uses PowerShell because SFTP alone does not expose creation
        # time in a portable way. We normalize the output to the same
        # `FileMetadata` contract used everywhere else.
        if os_type == "windows":
            ps_path = normalized_path.replace("'", "''")
            ps_cmd = (
                f"$item = Get-Item -LiteralPath '{ps_path}' -ErrorAction Stop; "
                "[string]::Join('|',"
                "$item.FullName,"
                "$item.Length,"
                "$item.CreationTimeUtc.ToString('o'),"
                "$item.LastWriteTimeUtc.ToString('o'),"
                "'NTFS'"
                ")"
            )
            cmd = f'powershell -NoProfile -Command "{ps_cmd}"'

            _, stdout, stderr = self.ssh_client.exec_command(
                cmd,
                timeout=k.HOST_BUSY_TIMEOUT,
            )
            raw = stdout.readline().strip()
            err = stderr.read().decode("utf-8", errors="ignore").strip()

            if err:
                self.log.warning(f"[META][WINDOWS][ONE] STDERR: {err}")

            if not raw:
                raise FileNotFoundError(f"Remote file not found: {normalized_path}")

            fullpath, size, c_at, m_at, perms = raw.split("|")
            fullpath = fullpath.replace("\\", "/")
            filename_only = os.path.basename(fullpath)
            dirname = os.path.dirname(fullpath)
            _, ext = os.path.splitext(filename_only)

            return FileMetadata(
                NA_FULL_PATH=fullpath,
                NA_PATH=dirname,
                NA_FILE=filename_only,
                NA_EXTENSION=ext,
                VL_FILE_SIZE_KB=int(size) // 1024,
                DT_FILE_CREATED=tools.parse_ps_iso(c_at),
                DT_FILE_MODIFIED=tools.parse_ps_iso(m_at),
                DT_FILE_ACCESSED=None,
                NA_OWNER="",
                NA_GROUP="0",
                NA_PERMISSIONS=perms,
            )

        raise RuntimeError(f"Unsupported OS '{os_type}' for metadata probe")

    # =================================================================
    # OS Detection
    # =================================================================
    def detect_remote_os(self) -> str:
        """Detect remote operating system via SSH. Returns 'linux' or 'windows'."""
        try:
            stdin, stdout, stderr = self.ssh_client.exec_command(
                "uname -s", timeout=5
            )
            uname = stdout.read().decode(errors="ignore").strip().lower()
            if "linux" in uname:
                return "linux"
        except Exception:
            pass

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

        # Both probes failed; assume Linux as the safer default for unknown hosts.
        return "linux"

    # =================================================================
    # Cross-platform discovery with metadata
    # =================================================================
    def iter_find_files_with_metadata(
        self,
        remote_path: str,
        pattern: str,
        *,
        recursive: bool = True,
        newer_than: str | None = None,
        batch_size: int = 1000,
    ) -> Iterator[list[FileMetadata]]:
        """
        Stream remote filesystem metadata in fixed-size batches.

        Streams stdout line-by-line to avoid buffering large remote listings.
        At most `batch_size` FileMetadata objects live in memory at once.
        """

        remote_path = remote_path.rstrip("/").rstrip("\\")
        pattern = pattern.strip().replace('"', "").replace("'", "")
        # Ensure the pattern acts as a suffix glob, not an exact filename match.
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
