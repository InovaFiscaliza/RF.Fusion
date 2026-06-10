"""
Remote discovery helpers shared by appCataloga workers.

Provides SSH/SFTP context initialization and the `iter_metadata_files` generator
that coordinates remote filesystem traversal for the discovery flow.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from shared.file_metadata import FileMetadata
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
from .host_connectivity import resolve_host_addresses
from .host_ssh_utils import sftpConnection

def iter_metadata_files(
    sftp_conn: sftpConnection,
    log: log,
    hostname: str,
    host_id: int,
    filter_obj: Filter,
    callBackCheckFile,
    callBackGetLastDBDate,
    *,
    batch_size: int = 1000,
) -> Iterator[list[FileMetadata]]:
    """
    High-level metadata discovery orchestrator for one remote host.

    Database-agnostic; deduplication and cutoff decisions stay in callbacks.
    Memory use is bounded by `batch_size`.

    Discovery modes come from `Filter`:
        - NONE / DEFAULT:  incremental using the last DB timestamp
        - FILE:            explicit file list (timestamp ignored)
        - REDISCOVERY:     full rescan (timestamp ignored)
    """
    if isinstance(filter_obj, dict):
        filter_obj = Filter(filter_obj, log=log)

    mode = (filter_obj.data.get("mode") or "").upper()
    remote_dir = filter_obj.data.get("file_path", k.DEFAULT_DATA_FOLDER)
    pattern = filter_obj._build_pattern(hostname=hostname)

    newer_than = None
    if mode != Filter.MODE_FILE:
        last_dt = callBackGetLastDBDate(host_id)
        if last_dt:
            newer_than = last_dt.strftime("%Y-%m-%d %H:%M:%S")

    # REDISCOVERY ignores the DB cutoff on purpose.
    if mode == Filter.MODE_REDISCOVERY:
        newer_than = None

    for batch in sftp_conn.iter_find_files_with_metadata(
        remote_path=remote_dir,
        pattern=pattern,
        newer_than=newer_than,
        batch_size=batch_size,
    ):
        if mode != Filter.MODE_FILE:
            batch = callBackCheckFile(
                host_id=host_id,
                batch=batch,
                batch_size=batch_size,
            )
        else:
            log.event(
                "metadata_mode_file_skip_dedup",
                component="host_metadata",
                operation="dedup",
                host=hostname,
                host_id=host_id,
                mode="file",
            )

        if not batch:
            continue

        batch = filter_obj.evaluate_metadata(batch)

        if batch:
            yield batch


def init_host_context(host: dict, log) -> sftpConnection:
    """Initialize one remote SSH/SFTP session from a HOST row."""
    # Address resolution already sorts operational 172.x.x.x endpoints first.
    resolved_addr = resolve_host_addresses(host["HOST__NA_HOST_ADDRESS"])[0]
    return sftpConnection(
        host_uid=host["HOST__NA_HOST_NAME"],
        host_addr=resolved_addr,
        port=int(host["HOST__NA_HOST_PORT"]),
        user=host["HOST__NA_HOST_USER"],
        password=host["HOST__NA_HOST_PASSWORD"],
        log=log,
    )
