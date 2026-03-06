from __future__ import annotations

import os
import sys
import time
import json
from enum import Enum
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from shared.file_metadata import FileMetadata
from typing import Dict, Union

# ---------------------------------------------------------------------
# Ensure config import path (same rule used in legacy)
# ---------------------------------------------------------------------
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)

CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402
from .filter import Filter
from .ssh_utils import sftpConnection
from .logging_utils import log

# =====================================================================
# HaltFlag class to manage objects
# =====================================================================
class HaltFlagState(Enum):
    NO_FLAG = 0
    OWN_FLAG = 1
    FOREIGN_FLAG = 2
    STALE_FLAG = 3  # flag existe, mas está vencido (tempo excedido)


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
        sftp_conn: sftpConnection,
        log: log,
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

            cfg, _ = self.parse_cfg(cfg_content)
            self.config = cfg

            self.log.entry("[CONFIG] Remote configuration loaded successfully.")
            return True

        # TIMEOUT
        except FuturesTimeoutError:
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
                        self.log.warning(
                            f"[HALT] HALT_FLAG stale (age={elapsed:.1f}s > {max_age}s)."
                        )
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
        """
        flag_path = self.config.get("HALT_FLAG")
        if not flag_path:
            self.log.warning("[HALT] No HALT_FLAG path configured.")
            return

        try:
            payload = {
                "service": service,
                "pid": os.getpid() if use_pid else None,
                "timestamp": datetime.now().isoformat(),
            }

            json_data = json.dumps(payload, indent=2)
            self.sftp_conn.write(flag_path, json_data.encode("utf-8"))

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

            if state == HaltFlagState.NO_FLAG:
                self._write_halt_flag(service, use_pid=use_pid)
                return True

            elif state == HaltFlagState.OWN_FLAG:
                self.log.entry(f"[HALT] HALT_FLAG already owned by {service}. Proceeding.")
                return True

            elif state == HaltFlagState.STALE_FLAG:
                self.log.entry("[HALT] Stale HALT_FLAG detected. Cleaning up...")
                self.release_halt_flag(service, use_pid=use_pid)
                time.sleep(1)
                self._write_halt_flag(service, use_pid=use_pid)
                return True

            elif state == HaltFlagState.FOREIGN_FLAG:
                if attempt >= max_cycles:
                    self.log.warning(
                        "[HALT] Timeout reached. Forcing HALT_FLAG removal to prevent deadlock."
                    )
                    self.release_halt_flag(service, use_pid=use_pid, force=True)
                    self._write_halt_flag(service, use_pid=use_pid)
                    return True

                self.log.entry(
                    f"[HALT] HALT_FLAG held by another service. Waiting ({attempt}/{max_cycles})..."
                )
                time.sleep(sleep_interval)
                continue

            else:
                self.log.warning(
                    f"[HALT] Unknown HALT_FLAG state '{state}'. Retrying..."
                )
                time.sleep(sleep_interval)

        self.log.warning(
            f"[HALT] Unable to acquire HALT_FLAG after {halt_timeout}s."
        )
        return False

    def release_halt_flag(
        self,
        service: str,
        use_pid: bool = False,
        force: bool = False,
    ) -> None:
        """
        Safely remove the HALT_FLAG file from the remote host.

        The function checks flag ownership and only removes it if:
        - The flag belongs to the same service (OWN_FLAG), or
        - The flag is stale (STALE_FLAG), or
        - The `force` parameter is explicitly True.
        """
        flag_path = self.config.get("HALT_FLAG")
        if not flag_path:
            self.log.warning(
                "[HALT] release_halt_flag(): No HALT_FLAG path configured."
            )
            return

        try:
            if force:
                try:
                    self.sftp_conn.remove(flag_path)
                    self.log.entry("[HALT] HALT_FLAG forcibly removed.")
                except FileNotFoundError:
                    self.log.entry(
                        "[HALT] HALT_FLAG already absent (force mode)."
                    )
                except Exception as e:
                    self.log.warning(
                        f"[HALT] Forced HALT_FLAG removal failed: {e}"
                    )
                return

            state = self._check_halt_flag(service, use_pid=use_pid)

            if state in (HaltFlagState.OWN_FLAG, HaltFlagState.STALE_FLAG):
                try:
                    self.sftp_conn.remove(flag_path)
                    self.log.entry(
                        f"[HALT] HALT_FLAG removed ({state.name.lower()})."
                    )
                except FileNotFoundError:
                    self.log.entry("[HALT] HALT_FLAG already absent.")
                except Exception as e:
                    self.log.warning(
                        f"[HALT] Failed to remove HALT_FLAG: {e}"
                    )
            else:
                self.log.entry(
                    f"[HALT] HALT_FLAG not removed (state={state.name})."
                )

        except Exception as e:
            self.log.warning(
                f"[HALT] release_halt_flag() encountered an unexpected error: {e}"
            )

    # ----------------------------------------------------------------------
    # Discovery
    # ----------------------------------------------------------------------
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

        This method represents the FINAL aggregation stage of the discovery process.
        It is responsible for:

            • Determining the discovery source (LOCAL filesystem or REMOTE list)
            • Performing full in-memory metadata collection
            • Applying database-level deduplication
            • Applying semantic filters via Filter.evaluate_metadata()

        ⚠️ IMPORTANT DESIGN CONSTRAINTS ⚠️
        ----------------------------------
        • This function INTENTIONALLY returns a fully materialized list.
        It does NOT stream, batch, or chunk results.

        • All metadata handled by this function MUST be FileMetadata objects.
        Legacy dict-based metadata is no longer supported.

        • This function MUST remain side-effect free:
            - No DB writes
            - No task creation
            - No backlog manipulation

        Args:
            host_id (int):
                Identifier of the host being processed.

            filter_obj (Filter):
                Parsed filter configuration controlling discovery behavior.

            callBackFileTask (callable):
                Callback returning existing FILE_TASK filenames for this host.

            callBackFileTaskHistory (callable):
                Callback returning historical FILE_TASK filenames for this host.

            callBackGetLastDBDate (callable):
                Callback returning the last successful discovery timestamp.

        Returns:
            list[FileMetadata]:
                Fully filtered list of discovered file metadata objects.
        """

        try:
            # ------------------------------------------------------------------
            # Normalize filter object
            # ------------------------------------------------------------------
            if isinstance(filter_obj, dict):
                filter_obj = Filter(filter_obj, log=self.log)

            mode = (filter_obj.data.get("mode") or "").upper()
            agent = (filter_obj.data.get("agent") or "").upper()

            remote_dir = filter_obj.data.get(
                "file_path", k.DEFAULT_DATA_FOLDER
            )
            pattern = filter_obj._build_pattern()

            # ------------------------------------------------------------------
            # Determine incremental discovery cutoff
            # ------------------------------------------------------------------
            newer_than = None

            if mode != Filter.MODE_FILE:
                last_dt = callBackGetLastDBDate(host_id)
                if last_dt:
                    newer_than = last_dt.strftime("%Y-%m-%d %H:%M:%S")

            if mode == Filter.MODE_REDISCOVERY:
                newer_than = None

            # ------------------------------------------------------------------
            # Metadata acquisition
            # ------------------------------------------------------------------
            if agent == "REMOTE":
                # REMOTE mode relies on a pre-generated DUE_BACKUP file
                self.get_config()
                raw = self.sftp_conn.read(self.config["DUE_BACKUP"], mode="r")

                if not raw:
                    return []

                metadata: list = []

                for line in (
                    raw.decode("utf-8", errors="ignore")
                    .replace("\x00", "")
                    .splitlines()
                ):
                    line = line.strip()
                    if not line:
                        continue

                    filename = os.path.basename(line)
                    dirname = os.path.dirname(line)
                    _, ext = os.path.splitext(filename)

                    # NOTE:
                    # REMOTE discovery does not provide filesystem metadata.
                    # Fields are intentionally populated with neutral defaults.
                    metadata.append(
                        FileMetadata(
                            NA_FULL_PATH=line.replace("\\", "/"),
                            NA_PATH=dirname.replace("\\", "/"),
                            NA_FILE=filename,
                            NA_EXTENSION=ext,
                            VL_FILE_SIZE_KB=0,
                            DT_FILE_CREATED=None,
                            DT_FILE_MODIFIED=None,
                            DT_FILE_ACCESSED=None,
                            NA_OWNER="",
                            NA_GROUP="0",
                            NA_PERMISSIONS="",
                        )
                    )

            elif agent == "LOCAL":
                # LOCAL discovery performs full filesystem traversal via SSH
                metadata = self.sftp_conn.sftp_find_files_with_metadata(
                    remote_path=remote_dir,
                    pattern=pattern,
                    recursive=True,
                    newer_than=newer_than,
                )

            else:
                self.log.error(f"[META] Invalid agent '{agent}'")
                return []

            # ------------------------------------------------------------------
            # Early exit if nothing was discovered
            # ------------------------------------------------------------------
            if not metadata:
                self.log.entry("[META] No files discovered")
                return []

            self.log.entry(
                f"[META] Discovery completed | files={len(metadata)}"
            )

            # ------------------------------------------------------------------
            # FILE mode: no deduplication, only semantic filtering
            # ------------------------------------------------------------------
            if mode == Filter.MODE_FILE:
                filtered = filter_obj.evaluate_metadata(metadata)
                self.log.entry(
                    f"[META] FILE mode | returned={len(filtered)}"
                )
                return filtered

            # ------------------------------------------------------------------
            # Database-level deduplication
            # ------------------------------------------------------------------
            task_names = set(callBackFileTask(host_id))
            task_hist_names = set(callBackFileTaskHistory(host_id))

            before = len(metadata)

            filtered = [
                m for m in metadata
                if m.NA_FILE not in task_names
                and m.NA_FILE not in task_hist_names
            ]

            self.log.entry(
                f"[META] Deduplication | before={before} | after={len(filtered)}"
            )

            if not filtered:
                return []

            # ------------------------------------------------------------------
            # Final semantic filtering
            # ------------------------------------------------------------------
            final = filter_obj.evaluate_metadata(filtered)

            self.log.entry(
                f"[META] Final result | returned={len(final)}"
            )

            return final

        except Exception as e:
            self.log.error(f"[META] Unexpected error: {e}")
            return []


    # ----------------------------------------------------------------------
    # Backup completion logging
    # ----------------------------------------------------------------------
    def write_backup_done(self, message: str) -> None:
        """Append a line to BACKUP_DONE file in the remote node."""
        if not self.config or "BACKUP_DONE" not in self.config:
            self.log.warning(
                "[HostDaemon] BACKUP_DONE not defined in config."
            )
            return
        try:
            self.sftp_conn.append(
                self.config["BACKUP_DONE"], message + "\n"
            )
            self.log.entry(
                f"[HostDaemon] Logged to BACKUP_DONE: {message}"
            )
        except Exception as e:
            self.log.warning(
                f"[HostDaemon] Failed to append BACKUP_DONE: {e}"
            )

    # ----------------------------------------------------------------------
    # Cleanup / termination
    # ----------------------------------------------------------------------
    def close_host(self, cleanup_due_backup: bool = False) -> None:
        """Clean up temporary files and release resources gracefully."""
        try:
            if cleanup_due_backup and self.config:
                self.sftp_conn.remove(self.config["DUE_BACKUP"])
        except Exception as e:
            self.log.warning(
                f"[HostDaemon] Could not remove DUE_BACKUP: {e}"
            )
        try:
            self.sftp_conn.close()
        except Exception as e:
            self.log.warning(
                f"[HostDaemon] Error closing SFTP session: {e}"
            )
            
    
    def iter_metadata_files(
        self,
        hostname: str,
        host_id: int,
        filter_obj: Filter,
        callBackCheckFile,
        callBackGetLastDBDate,
        *,
        batch_size: int = 1000,
    ):
        """
        High-level metadata discovery orchestrator.

        This generator coordinates the complete metadata discovery lifecycle
        for a given host. It is intentionally DATABASE-AGNOSTIC and relies on
        callbacks to delegate persistence-aware decisions.

        Responsibilities:
            • Discover filesystem metadata remotely
            • Apply incremental discovery rules
            • Delegate deduplication to an external callback
            • Enforce semantic Filter rules
            • Yield bounded batches of FileMetadata eligible for persistence

        Architectural guarantees:
            • Does NOT know database schema or tables
            • Does NOT perform SQL or persistence logic
            • Uses callbacks to externalize stateful decisions
            • Memory usage is strictly bounded by `batch_size`
            • Safe for reuse, testing, and mocking

        Discovery modes (derived from Filter):
            - NONE / DEFAULT:
                Incremental discovery using last DB timestamp
            - FILE:
                Explicit file discovery (timestamp ignored)
            - REDISCOVERY:
                Full rescan of the remote path (timestamp ignored)

        Deduplication strategy:
            • Entirely delegated to `callBackCheckFile`
            • Callback must accept and return List[FileMetadata]
            • Callback may use database, cache, or other mechanisms

        Args:
            host_id (int):
                Host identifier.
            filter_obj (Filter | dict):
                Discovery filter definition.
            callBackCheckFile (callable):
                Callback responsible for filtering out existing files.
            callBackGetLastDBDate (callable):
                Callback returning last discovery timestamp for incremental mode.
            batch_size (int):
                Maximum batch size and memory bound.

        Yields:
            List[FileMetadata]:
                Filtered and deduplicated batches of metadata.
        """

        # ------------------------------------------------------------
        # Normalize filter input
        # ------------------------------------------------------------
        if isinstance(filter_obj, dict):
            filter_obj = Filter(filter_obj, log=self.log)

        # ------------------------------------------------------------
        # Resolve discovery semantics
        # ------------------------------------------------------------
        mode = (filter_obj.data.get("mode") or "").upper()
        agent = (filter_obj.data.get("agent") or "").upper()

        # ------------------------------------------------------------
        # Resolve remote scan parameters
        # ------------------------------------------------------------
        remote_dir = filter_obj.data.get("file_path", k.DEFAULT_DATA_FOLDER)
        pattern = filter_obj._build_pattern(hostname=hostname)

        # ------------------------------------------------------------
        # Incremental discovery cutoff
        # ------------------------------------------------------------
        newer_than = None
        if mode != Filter.MODE_FILE:
            last_dt = callBackGetLastDBDate(host_id)
            if last_dt:
                newer_than = last_dt.strftime("%Y-%m-%d %H:%M:%S")

        if mode == Filter.MODE_REDISCOVERY:
            newer_than = None

        # ------------------------------------------------------------
        # Agent validation
        # ------------------------------------------------------------
        if agent != "LOCAL":
            self.log.error(f"[META] Unsupported agent '{agent}'")
            return

        # ------------------------------------------------------------
        # Remote metadata discovery loop
        # ------------------------------------------------------------
        for batch in self.sftp_conn.iter_find_files_with_metadata(
            remote_path=remote_dir,
            pattern=pattern,
            newer_than=newer_than,
            batch_size=batch_size,
        ):
            # --------------------------------------------
            # Delegated deduplication phase
            # --------------------------------------------
            # The iterator does NOT know how deduplication is done.
            # It only trusts the callback contract.
            # Only available for modes except FILE
            if mode != Filter.MODE_FILE:
                batch = callBackCheckFile(
                    host_id=host_id,
                    batch=batch,
                    batch_size=batch_size,
                )
            else:
                self.log.entry(
                    f"[META] MODE_FILE active — skipping deduplication for host {host_id}"
            )

            if not batch:
                continue

            # --------------------------------------------
            # Filter evaluation phase
            # --------------------------------------------
            batch = filter_obj.evaluate_metadata(batch)

            if batch:
                yield batch

