#!/opt/conda/envs/appdata/bin/python
# -*- coding: utf-8 -*-
"""
One-shot operational reconciler for CWSM `.mat` history rows.

This script is intentionally outside the normal RF.Fusion worker contracts.
Its only purpose is to backfill the new `*_HOST` metadata columns in
`FILE_TASK_HISTORY` by rediscovering the original `.zip` files on CWSM hosts.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402
from db.dbHandlerBKP import dbHandlerBKP  # noqa: E402
from host_handler.host_connectivity import is_host_online, resolve_host_addresses  # noqa: E402
from host_handler.host_ssh_utils import sftpConnection  # noqa: E402
from shared.logging_utils import log  # noqa: E402
from shared.file_metadata import FileMetadata  # noqa: E402


SERVICE_NAME = "cwsm_zip_host_metadata_reconcile"
LOCAL_TRASH_DIRS = (
    "/mnt/reposfi/trash",
    "/mnt/reposfi/trash/resolved_files",
)
_CWSM_FILENAME_WINDOW_RE = re.compile(
    r"\[(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})\]_\[(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})\]"
)


def _build_zip_name(candidate: dict[str, Any]) -> str | None:
    """Resolve the expected source `.zip` filename for one `.mat` history row."""

    host_name = (candidate.get("NA_HOST_FILE_NAME") or "").strip()
    if host_name.lower().endswith(".zip"):
        return host_name

    server_name = (candidate.get("NA_SERVER_FILE_NAME") or "").strip()
    if server_name.lower().endswith(".mat"):
        return server_name[:-4] + ".zip"

    return None


def _parse_cwsm_filename_timestamps(file_name: str) -> tuple[datetime, datetime] | None:
    """Recover host-side timestamps from the CWSM filename window."""

    match = _CWSM_FILENAME_WINDOW_RE.search(file_name)
    if match is None:
        return None

    start = datetime.strptime(match.group(1), "%Y-%m-%d-%H-%M-%S")
    end = datetime.strptime(match.group(2), "%Y-%m-%d-%H-%M-%S")
    offset = timedelta(hours=k.APP_ANALISE_CWSM_FILENAME_UTC_OFFSET_HOURS)
    return start + offset, end + offset


def _build_local_zip_metadata(file_path: str) -> FileMetadata | None:
    """Build a metadata record from one local trash `.zip` file."""

    file_name = os.path.basename(file_path)
    timestamps = _parse_cwsm_filename_timestamps(file_name)
    if timestamps is None:
        return None

    created_at, modified_at = timestamps
    stat_result = os.stat(file_path)
    return FileMetadata(
        NA_FULL_PATH=file_path,
        NA_PATH=os.path.dirname(file_path),
        NA_FILE=file_name,
        NA_EXTENSION=os.path.splitext(file_name)[1].lower(),
        VL_FILE_SIZE_KB=int(stat_result.st_size) // 1024,
        DT_FILE_CREATED=created_at,
        DT_FILE_MODIFIED=modified_at,
        DT_FILE_ACCESSED=None,
        NA_OWNER="",
        NA_GROUP="",
        NA_PERMISSIONS="",
    )


def _load_local_zip_metadata_map(
    candidate_rows: list[dict[str, Any]],
    logger: log,
) -> dict[str, list[FileMetadata]]:
    """
    Index matching local trash `.zip` files by basename.

    The local trash no longer preserves the remote path. Basename match is the
    safest usable key here, but only unique matches are trusted later.
    """

    needed_names = {
        zip_name
        for row in candidate_rows
        if (zip_name := _build_zip_name(row)) is not None
    }
    metadata_map: dict[str, list[FileMetadata]] = defaultdict(list)

    for root_dir in LOCAL_TRASH_DIRS:
        if not os.path.isdir(root_dir):
            continue

        for dir_path, _, file_names in os.walk(root_dir):
            for file_name in file_names:
                if file_name not in needed_names:
                    continue

                metadata = _build_local_zip_metadata(os.path.join(dir_path, file_name))
                if metadata is None:
                    logger.warning_event(
                        "cwsm_zip_reconcile_local_timestamp_unresolved",
                        component="cwsm_zip_reconcile",
                        operation="local_index",
                        local_path=os.path.join(dir_path, file_name),
                    )
                    continue

                metadata_map[file_name].append(metadata)

    logger.event(
        "cwsm_zip_reconcile_local_index_loaded",
        component="cwsm_zip_reconcile",
        operation="local_index",
        requested_zip_files=len(needed_names),
        matched_zip_files=sum(len(rows) for rows in metadata_map.values()),
        unique_zip_names=len(metadata_map),
    )
    return metadata_map


def _connect_host(host: dict[str, Any], logger: log) -> sftpConnection:
    """Open one SSH/SFTP context for a reconciled host."""

    resolved_addrs = resolve_host_addresses(host["NA_HOST_ADDRESS"])
    last_error: Exception | None = None

    for resolved_addr in resolved_addrs:
        try:
            return sftpConnection(
                host_uid=host["NA_HOST_NAME"],
                host_addr=resolved_addr,
                port=int(host["NA_HOST_PORT"]),
                user=host["NA_HOST_USER"],
                password=host["NA_HOST_PASSWORD"],
                log=logger,
            )
        except Exception as exc:
            last_error = exc
            logger.warning_event(
                "cwsm_zip_reconcile_ssh_candidate_failed",
                component="cwsm_zip_reconcile",
                operation="connect",
                host=host["NA_HOST_NAME"],
                host_id=int(host["ID_HOST"]),
                address=resolved_addr,
                port=int(host["NA_HOST_PORT"]),
                error=exc,
            )

    if last_error is None:
        raise RuntimeError("No resolved host address available for SSH reconciliation.")

    raise last_error


def _load_zip_metadata_map(
    sftp_conn: sftpConnection,
    host_name: str,
    host_id: int,
    candidate_rows: list[dict[str, Any]],
    logger: log,
) -> dict[tuple[str, str], FileMetadata]:
    """
    Rediscover `.zip` files for one host and keep them in memory.

    The key is `(remote_path, file_name)` so later DB reconciliation can
    match rows deterministically without reopening SSH per history row.
    """

    metadata_map: dict[tuple[str, str], FileMetadata] = {}
    scanned_paths = sorted(
        {
            str(row["NA_HOST_FILE_PATH"]).rstrip("/").rstrip("\\")
            for row in candidate_rows
            if row.get("NA_HOST_FILE_PATH")
        }
    )

    for remote_path in scanned_paths:
        for batch in sftp_conn.iter_find_files_with_metadata(
            remote_path=remote_path,
            pattern="*.zip",
            newer_than=None,
            batch_size=1000,
        ):
            for metadata in batch:
                metadata_map[(metadata.NA_PATH, metadata.NA_FILE)] = metadata

    logger.event(
        "cwsm_zip_rediscovery_loaded",
        component="cwsm_zip_reconcile",
        operation="rediscover",
        host=host_name,
        host_id=host_id,
        scanned_paths=len(scanned_paths),
        rediscovered_zip_files=len(metadata_map),
    )
    return metadata_map


def _update_history_host_metadata(
    db: dbHandlerBKP,
    candidate: dict[str, Any],
    metadata: FileMetadata,
) -> None:
    """Persist reconciled `.zip` metadata into the new `_HOST` columns."""

    db.file_history_update(
        history_id=int(candidate["ID_HISTORY"]),
        NA_EXTENSION_HOST=metadata.NA_EXTENSION,
        VL_FILE_SIZE_KB_HOST=metadata.VL_FILE_SIZE_KB,
        DT_FILE_CREATED_HOST=metadata.DT_FILE_CREATED,
        DT_FILE_MODIFIED_HOST=metadata.DT_FILE_MODIFIED,
    )


def _reconcile_host_from_local_trash(
    db: dbHandlerBKP,
    host: dict[str, Any],
    host_candidates: list[dict[str, Any]],
    local_zip_map: dict[str, list[FileMetadata]],
    logger: log,
) -> tuple[int, int]:
    """Reconcile one host using local trash `.zip` files only."""

    host_updated = 0
    host_missing = 0

    for candidate in host_candidates:
        zip_name = _build_zip_name(candidate)
        if not zip_name:
            host_missing += 1
            logger.warning_event(
                "cwsm_zip_reconcile_name_unresolved",
                component="cwsm_zip_reconcile",
                operation="local_match",
                host=host["NA_HOST_NAME"],
                host_id=int(host["ID_HOST"]),
                history_id=int(candidate["ID_HISTORY"]),
                server_file=candidate.get("NA_SERVER_FILE_NAME"),
            )
            continue

        matches = local_zip_map.get(zip_name, [])
        metadata = _select_local_zip_metadata(matches)
        if metadata is None:
            host_missing += 1
            event_name = "cwsm_zip_reconcile_local_zip_ambiguous" if matches else "cwsm_zip_reconcile_local_zip_missing"
            logger.warning_event(
                event_name,
                component="cwsm_zip_reconcile",
                operation="local_match",
                host=host["NA_HOST_NAME"],
                host_id=int(host["ID_HOST"]),
                history_id=int(candidate["ID_HISTORY"]),
                expected_zip=zip_name,
                candidates=len(matches),
            )
            continue

        _update_history_host_metadata(db, candidate, metadata)
        host_updated += 1

    return host_updated, host_missing


def _select_local_zip_metadata(matches: list[FileMetadata]) -> FileMetadata | None:
    """Accept duplicate local copies only when their metadata is identical."""

    if not matches:
        return None

    signatures = {
        (
            match.NA_EXTENSION,
            match.VL_FILE_SIZE_KB,
            match.DT_FILE_CREATED,
            match.DT_FILE_MODIFIED,
        )
        for match in matches
    }
    if len(signatures) != 1:
        return None

    return matches[0]


def _group_candidates_by_host(
    candidates: Iterable[dict[str, Any]],
) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        grouped[int(row["FK_HOST"])].append(row)
    return grouped


def main(
    host_id: int | None = None,
    host_busy_timeout: int | None = None,
    use_local_trash_fallback: bool = False,
    use_local_trash_only: bool = False,
) -> None:
    if host_busy_timeout is not None:
        k.HOST_BUSY_TIMEOUT = host_busy_timeout

    logger = log(os.path.join("/tmp", f"{SERVICE_NAME}.log"))
    db = dbHandlerBKP(k.BKP_DATABASE_NAME, logger)

    candidates = db.file_history_list_cwsm_zip_reconciliation_candidates(
        host_id=host_id,
        include_offline=(use_local_trash_fallback or use_local_trash_only),
    )
    grouped = _group_candidates_by_host(candidates)
    host_rows = db.host_list_for_cwsm_zip_reconciliation(sorted(grouped)) if grouped else []
    hosts_by_id = {int(row["ID_HOST"]): row for row in host_rows}
    local_zip_map = (
        _load_local_zip_metadata_map(candidates, logger)
        if use_local_trash_fallback and candidates
        else {}
    )

    logger.event(
        "cwsm_zip_reconcile_started",
        component="cwsm_zip_reconcile",
        operation="main",
        hosts=len(grouped),
        history_rows=len(candidates),
    )

    total_updated = 0
    total_missing = 0
    total_offline_hosts = 0
    total_failed_hosts = 0

    for host_id, host_candidates in grouped.items():
        host = hosts_by_id.get(host_id)
        if host is None:
            logger.warning_event(
                "cwsm_zip_reconcile_host_missing",
                component="cwsm_zip_reconcile",
                operation="main",
                host_id=host_id,
                rows=len(host_candidates),
            )
            total_missing += len(host_candidates)
            continue

        if use_local_trash_only:
            host_updated, host_missing = _reconcile_host_from_local_trash(
                db,
                host,
                host_candidates,
                local_zip_map,
                logger,
            )
            total_updated += host_updated
            total_missing += host_missing
            logger.event(
                "cwsm_zip_reconcile_host_done",
                component="cwsm_zip_reconcile",
                operation="local_only",
                host=host["NA_HOST_NAME"],
                host_id=host_id,
                updated=host_updated,
                missing=host_missing,
                rows=len(host_candidates),
            )
            continue

        if not is_host_online(host["NA_HOST_ADDRESS"]):
            if use_local_trash_fallback:
                host_updated, host_missing = _reconcile_host_from_local_trash(
                    db,
                    host,
                    host_candidates,
                    local_zip_map,
                    logger,
                )
                total_updated += host_updated
                total_missing += host_missing
                logger.event(
                    "cwsm_zip_reconcile_host_done",
                    component="cwsm_zip_reconcile",
                    operation="local_fallback",
                    host=host["NA_HOST_NAME"],
                    host_id=host_id,
                    updated=host_updated,
                    missing=host_missing,
                    rows=len(host_candidates),
                )
                continue

            logger.warning_event(
                "cwsm_zip_reconcile_host_offline",
                component="cwsm_zip_reconcile",
                operation="icmp_precheck",
                host=host["NA_HOST_NAME"],
                host_id=host_id,
                rows=len(host_candidates),
            )
            total_offline_hosts += 1
            total_missing += len(host_candidates)
            continue

        sftp_conn: sftpConnection | None = None
        try:
            sftp_conn = _connect_host(host, logger)
            zip_map = _load_zip_metadata_map(
                sftp_conn,
                host_name=host["NA_HOST_NAME"],
                host_id=host_id,
                candidate_rows=host_candidates,
                logger=logger,
            )

            host_updated = 0
            host_missing = 0

            for candidate in host_candidates:
                zip_name = _build_zip_name(candidate)
                if not zip_name:
                    host_missing += 1
                    logger.warning_event(
                        "cwsm_zip_reconcile_name_unresolved",
                        component="cwsm_zip_reconcile",
                        operation="match",
                        host=host["NA_HOST_NAME"],
                        host_id=host_id,
                        history_id=int(candidate["ID_HISTORY"]),
                        server_file=candidate.get("NA_SERVER_FILE_NAME"),
                    )
                    continue

                key = (candidate["NA_HOST_FILE_PATH"], zip_name)
                metadata = zip_map.get(key)
                if metadata is None:
                    host_missing += 1
                    logger.warning_event(
                        "cwsm_zip_reconcile_zip_missing",
                        component="cwsm_zip_reconcile",
                        operation="match",
                        host=host["NA_HOST_NAME"],
                        host_id=host_id,
                        history_id=int(candidate["ID_HISTORY"]),
                        remote_path=candidate.get("NA_HOST_FILE_PATH"),
                        expected_zip=zip_name,
                    )
                    continue

                _update_history_host_metadata(db, candidate, metadata)
                host_updated += 1

            total_updated += host_updated
            total_missing += host_missing

            logger.event(
                "cwsm_zip_reconcile_host_done",
                component="cwsm_zip_reconcile",
                operation="main",
                host=host["NA_HOST_NAME"],
                host_id=host_id,
                updated=host_updated,
                missing=host_missing,
                rows=len(host_candidates),
            )

        except Exception as exc:
            if use_local_trash_fallback:
                host_updated, host_missing = _reconcile_host_from_local_trash(
                    db,
                    host,
                    host_candidates,
                    local_zip_map,
                    logger,
                )
                total_updated += host_updated
                total_missing += host_missing
                logger.event(
                    "cwsm_zip_reconcile_host_done",
                    component="cwsm_zip_reconcile",
                    operation="local_fallback",
                    host=host["NA_HOST_NAME"],
                    host_id=host_id,
                    updated=host_updated,
                    missing=host_missing,
                    rows=len(host_candidates),
                )
                continue

            total_failed_hosts += 1
            total_missing += len(host_candidates)
            logger.error_event(
                "cwsm_zip_reconcile_host_failed",
                component="cwsm_zip_reconcile",
                operation="main",
                host=host["NA_HOST_NAME"],
                host_id=host_id,
                rows=len(host_candidates),
                error=exc,
            )

        finally:
            if sftp_conn is not None:
                try:
                    sftp_conn.close()
                except Exception:
                    pass

    logger.event(
        "cwsm_zip_reconcile_completed",
        component="cwsm_zip_reconcile",
        operation="main",
        updated=total_updated,
        missing=total_missing,
        offline_hosts=total_offline_hosts,
        failed_hosts=total_failed_hosts,
        hosts=len(grouped),
        history_rows=len(candidates),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reconcile CWSM .zip host metadata")
    parser.add_argument("--host-id", type=int, default=None, help="Limit reconciliation to one FK_HOST")
    parser.add_argument(
        "--host-busy-timeout",
        type=int,
        default=None,
        help="Override SSH discovery timeout for this one-shot run",
    )
    parser.add_argument(
        "--use-local-trash-fallback",
        action="store_true",
        help="Backfill offline or SSH-failed hosts from local trash `.zip` files",
    )
    parser.add_argument(
        "--use-local-trash-only",
        action="store_true",
        help="Skip ICMP and SSH. Reconcile only from local trash `.zip` files",
    )
    args = parser.parse_args()
    main(
        host_id=args.host_id,
        host_busy_timeout=args.host_busy_timeout,
        use_local_trash_fallback=args.use_local_trash_fallback,
        use_local_trash_only=args.use_local_trash_only,
    )
