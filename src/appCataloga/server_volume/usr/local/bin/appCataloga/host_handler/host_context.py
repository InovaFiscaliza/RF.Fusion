"""
Remote discovery helpers shared by appCataloga workers.

This module provides the high-level `hostDaemon` abstraction used by the
server-side discovery flow to traverse remote filesystems over SSH/SFTP and
yield `FileMetadata` batches without coupling traversal logic to database code.
"""

from __future__ import annotations

import os
import sys
from shared.filter import Filter
from shared.logging_utils import log

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
from .ssh_utils import sftpConnection

class hostDaemon:
    """High-level SSH/SFTP helper used by server-side discovery flows."""

    def __init__(
        self,
        sftp_conn: sftpConnection,
        log: log,
    ):
        """Initialize the host daemon wrapper around one SSH/SFTP session."""
        self.sftp_conn = sftp_conn
        self.log = log

    # ----------------------------------------------------------------------
    # Cleanup / termination
    # ----------------------------------------------------------------------
    def close_host(self, cleanup_due_backup: bool = False) -> None:
        """Close the underlying SSH/SFTP session gracefully.

        The ``cleanup_due_backup`` argument is kept only for backward
        compatibility with legacy call sites. The server-side discovery flow no
        longer manages any remote indexer files.
        """
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


def init_host_context(host: dict, log):
    """
    Initialize the shared SSH/SFTP and host-daemon context for one host row.
    """
    try:
        host_uid = host["HOST__NA_HOST_NAME"]
        host_addr = host["HOST__NA_HOST_ADDRESS"]
        port = int(host["HOST__NA_HOST_PORT"])
        user = host["HOST__NA_HOST_USER"]
        password = host["HOST__NA_HOST_PASSWORD"]
    except KeyError as exc:
        missing = str(exc)
        log.error(f"[INIT] Missing field in host metadata: {missing}")
        raise

    sftp_conn = sftpConnection(
        host_uid=host_uid,
        host_addr=host_addr,
        port=port,
        user=user,
        password=password,
        log=log,
    )

    daemon = hostDaemon(
        sftp_conn=sftp_conn,
        log=log,
    )

    return sftp_conn, daemon
