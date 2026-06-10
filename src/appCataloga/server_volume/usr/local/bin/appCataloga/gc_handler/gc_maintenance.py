#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Domain helpers for one garbage-collection pass.

The entrypoint owns daemon cadence and process lifecycle. This module owns the
GC rules:
which candidates are eligible, which directories are allowed, and which
deletions must update `FILE_TASK_HISTORY`.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import TYPE_CHECKING

import config as k
from db.dbHandlerBKP import dbHandlerBKP
from shared import file_utils

if TYPE_CHECKING:
    from shared.logging_utils import log as logger_type


def _event_fields(operation: str) -> dict[str, str]:
    """Return the shared log fields for GC domain events."""
    return {
        "component": "gc_maintenance",
        "operation": operation,
    }


def _is_path_within(path: str, root: str) -> bool:
    """Return whether `path` stays inside the managed directory `root`."""
    try:
        normalized_path = os.path.normpath(path)
        normalized_root = os.path.normpath(root)
        return os.path.commonpath([normalized_path, normalized_root]) == normalized_root
    except ValueError:
        # Mixed drives or invalid roots must fail closed.
        return False


def _delete_file(path: str, *, logger: logger_type) -> bool:
    """Delete one quarantined artifact.

    Missing files count as already cleaned. GC should stay idempotent.
    """
    try:
        os.remove(path)
        logger.event("garbage_file_deleted", path=path, **_event_fields("delete_file"))
        return True
    except FileNotFoundError:
        logger.warning_event(
            "garbage_file_missing",
            path=path,
            **_event_fields("delete_file"),
        )
        return True
    except Exception as exc:
        logger.error_event(
            "garbage_delete_failed",
            path=path,
            error=exc,
            **_event_fields("delete_file"),
        )
        return False


def build_resolved_files_trash_path() -> str:
    """Return the quarantine for superseded appAnalise leftovers."""
    return file_utils.build_resolved_files_trash_path()


def get_resolved_files_gc_candidates(
    *,
    batch_size: int,
    quarantine_days: int,
    logger: logger_type,
) -> list[str]:
    """Return aged files from `trash/resolved_files`.

    This channel is filesystem-only. It has no matching `FILE_TASK_HISTORY` row.
    """
    resolved_root = build_resolved_files_trash_path()

    if not os.path.isdir(resolved_root):
        return []

    # Use mtime because resolved-files artifacts have no queue timestamp.
    cutoff_ts = time.time() - (quarantine_days * 86400)
    candidates: list[tuple[float, str]] = []

    for root, _, files in os.walk(resolved_root):
        for name in files:
            full_path = os.path.join(root, name)

            try:
                modified_at = os.path.getmtime(full_path)
            except FileNotFoundError:
                continue
            except Exception as exc:
                logger.error_event(
                    "garbage_resolved_files_stat_failed",
                    path=full_path,
                    error=exc,
                    **_event_fields("get_resolved_files_gc_candidates"),
                )
                continue

            if modified_at <= cutoff_ts:
                candidates.append((modified_at, full_path))

    candidates.sort(key=lambda item: item[0])
    return [path for _, path in candidates[:batch_size]]


def log_gc_configuration(
    *,
    logger: logger_type,
    trash_root: str,
    resolved_root: str,
) -> None:
    """Log the retention contract used by the daemon."""
    logger.event(
        "garbage_configuration",
        **_event_fields("log_gc_configuration"),
        trash_root=trash_root,
        trash_quarantine_days=k.GC_QUARANTINE_DAYS,
        resolved_root=resolved_root,
        resolved_quarantine_days=k.GC_RESOLVED_FILES_QUARANTINE_DAYS,
        batch_size=k.GC_BATCH_SIZE,
    )


def collect_gc_candidates(
    db_bp: dbHandlerBKP,
    *,
    logger: logger_type,
) -> tuple[list[dict], list[str]]:
    """Gather the next batch from both GC channels.

    One channel is backed by `FILE_TASK_HISTORY`. The other is filesystem-only.
    """
    history_rows = db_bp.file_history_get_gc_candidates(
        batch_size=k.GC_BATCH_SIZE,
        quarantine_days=k.GC_QUARANTINE_DAYS,
    )
    resolved_rows = get_resolved_files_gc_candidates(
        batch_size=k.GC_BATCH_SIZE,
        quarantine_days=k.GC_RESOLVED_FILES_QUARANTINE_DAYS,
        logger=logger,
    )
    return history_rows, resolved_rows


def delete_history_artifacts(
    db_bp: dbHandlerBKP,
    history_rows: list[dict],
    *,
    trash_root: str,
    resolved_root: str,
    logger: logger_type,
) -> int:
    """Delete main-trash artifacts still referenced by `FILE_TASK_HISTORY`.

    These rows need a DB update after the file disappears.
    """
    deleted = 0

    for row in history_rows:
        server_path = row["NA_SERVER_FILE_PATH"]
        server_file = row["NA_SERVER_FILE_NAME"]

        if not server_path or not server_file:
            logger.warning_event(
                "garbage_invalid_path_metadata",
                **_event_fields("delete_history_artifacts"),
            )
            continue

        if not _is_path_within(server_path, trash_root):
            logger.error_event(
                "garbage_refused_outside_trash",
                path=server_path,
                **_event_fields("delete_history_artifacts"),
            )
            continue

        if _is_path_within(server_path, resolved_root):
            # `resolved_files` is owned by the second GC channel.
            logger.warning_event(
                "garbage_history_points_to_resolved_files",
                path=server_path,
                **_event_fields("delete_history_artifacts"),
            )
            continue

        file_path = os.path.join(server_path, server_file)

        if _delete_file(file_path, logger=logger):
            db_bp.file_history_update(
                history_id=row["ID_HISTORY"],
                IS_PAYLOAD_DELETED=1,
                DT_PAYLOAD_DELETED=datetime.now(),
            )
            logger.event(
                "garbage_history_artifact_deleted",
                history_id=row["ID_HISTORY"],
                path=file_path,
                **_event_fields("delete_history_artifacts"),
            )
            deleted += 1

    return deleted


def delete_resolved_files_artifacts(
    resolved_rows: list[str],
    *,
    resolved_root: str,
    logger: logger_type,
) -> int:
    """Delete superseded artifacts from `trash/resolved_files`.

    This channel is pure filesystem cleanup. No history row is updated here.
    """
    deleted = 0

    for file_path in resolved_rows:
        if not _is_path_within(file_path, resolved_root):
            logger.error_event(
                "garbage_refused_outside_resolved_files",
                path=file_path,
                **_event_fields("delete_resolved_files_artifacts"),
            )
            continue

        if _delete_file(file_path, logger=logger):
            logger.event(
                "garbage_resolved_artifact_deleted",
                path=file_path,
                **_event_fields("delete_resolved_files_artifacts"),
            )
            deleted += 1

    return deleted
