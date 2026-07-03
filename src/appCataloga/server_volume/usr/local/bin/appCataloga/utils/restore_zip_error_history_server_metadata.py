#!/opt/conda/envs/appdata/bin/python
# -*- coding: utf-8 -*-
"""
One-shot recovery of repository metadata for errored `.zip` history rows.

This script scans `FILE_TASK_HISTORY` rows whose original host artifact was a
`.zip` and whose processing status is `ERROR`. It restores the server-side
artifact columns only when the original `.zip` can still be found on disk.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import Any

from bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

import config as k  # noqa: E402
from db.dbHandlerBKP import dbHandlerBKP  # noqa: E402
from shared import file_utils  # noqa: E402
from shared.logging_utils import log  # noqa: E402


SERVICE_NAME = "restore_zip_error_history_server_metadata"


def _normalize_path(path: str) -> str:
    """Normalize one filesystem path for deterministic comparisons."""
    return os.path.normpath(path.strip())


def _build_repository_artifact(file_path: str) -> dict[str, Any]:
    """Build one repository-artifact payload from a verified local file."""
    stat_result = os.stat(file_path)
    return {
        "file_path": os.path.dirname(file_path),
        "file_name": os.path.basename(file_path),
        "extension": os.path.splitext(file_path)[1].lower(),
        "size_kb": max(1, int(stat_result.st_size / 1024) or 1),
        "dt_created": datetime.fromtimestamp(stat_result.st_ctime),
        "dt_modified": datetime.fromtimestamp(stat_result.st_mtime),
    }


def _candidate_paths(row: dict[str, Any]) -> list[str]:
    """Return the plausible repository locations for the original `.zip`."""
    host_file_name = str(row["NA_HOST_FILE_NAME"]).strip()
    candidates = []

    current_path = row.get("NA_SERVER_FILE_PATH")
    current_name = row.get("NA_SERVER_FILE_NAME")
    if current_path and current_name:
        candidates.append(os.path.join(str(current_path).strip(), str(current_name).strip()))
    if current_path and host_file_name:
        candidates.append(os.path.join(str(current_path).strip(), host_file_name))

    candidates.append(
        os.path.join(k.REPO_FOLDER, k.TRASH_FOLDER, host_file_name)
    )
    candidates.append(
        os.path.join(file_utils.build_resolved_files_trash_path(), host_file_name)
    )

    unique_candidates = []
    seen = set()
    for path in candidates:
        normalized = _normalize_path(path)
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_candidates.append(normalized)
    return unique_candidates


def _resolve_original_zip_artifact(row: dict[str, Any]) -> dict[str, Any] | None:
    """Resolve one trustworthy `.zip` artifact from the repository."""
    existing_paths = [
        path
        for path in _candidate_paths(row)
        if path.lower().endswith(".zip") and os.path.isfile(path)
    ]
    if len(existing_paths) != 1:
        return None
    return _build_repository_artifact(existing_paths[0])


def _process_batch(
    db: dbHandlerBKP,
    *,
    batch_limit: int,
    host_id: int | None,
    dry_run: bool,
    logger: log,
) -> dict[str, int]:
    """Scan and optionally repair one ordered batch stream."""
    scanned = 0
    restored = 0
    missing_or_ambiguous = 0
    unchanged = 0
    failed = 0
    after_history_id: int | None = None

    while True:
        rows = db.file_history_list_zip_error_server_restore_candidates(
            limit=batch_limit,
            host_id=host_id,
            after_history_id=after_history_id,
        )
        if not rows:
            break

        for row in rows:
            scanned += 1
            after_history_id = int(row["ID_HISTORY"])
            artifact = _resolve_original_zip_artifact(row)
            if artifact is None:
                missing_or_ambiguous += 1
                logger.warning_event(
                    "zip_error_history_restore_skipped",
                    service=SERVICE_NAME,
                    history_id=int(row["ID_HISTORY"]),
                    host_id=int(row["FK_HOST"]),
                    host_name=row.get("NA_HOST_NAME"),
                    host_file_name=row.get("NA_HOST_FILE_NAME"),
                )
                continue

            current_signature = (
                row.get("NA_SERVER_FILE_PATH"),
                row.get("NA_SERVER_FILE_NAME"),
                row.get("NA_EXTENSION_SERVER"),
                row.get("VL_FILE_SIZE_KB_SERVER"),
                row.get("DT_FILE_CREATED_SERVER"),
                row.get("DT_FILE_MODIFIED_SERVER"),
            )
            new_signature = (
                artifact["file_path"],
                artifact["file_name"],
                artifact["extension"],
                artifact["size_kb"],
                artifact["dt_created"],
                artifact["dt_modified"],
            )
            if current_signature == new_signature:
                unchanged += 1
                continue

            logger.event(
                "zip_error_history_restore_candidate",
                service=SERVICE_NAME,
                history_id=int(row["ID_HISTORY"]),
                host_id=int(row["FK_HOST"]),
                file=os.path.join(artifact["file_path"], artifact["file_name"]),
                dry_run=dry_run,
            )

            if dry_run:
                continue

            try:
                result = db.file_history_restore_server_artifact_metadata(
                    history_id=int(row["ID_HISTORY"]),
                    repository_artifact=artifact,
                )
                if result.get("rows_affected") == 1:
                    restored += 1
                else:
                    failed += 1
            except Exception as exc:
                failed += 1
                logger.error_event(
                    "zip_error_history_restore_failed",
                    service=SERVICE_NAME,
                    history_id=int(row["ID_HISTORY"]),
                    host_id=int(row["FK_HOST"]),
                    error=repr(exc),
                )

        if len(rows) < batch_limit:
            break

    return {
        "scanned": scanned,
        "restored": restored,
        "missing_or_ambiguous": missing_or_ambiguous,
        "unchanged": unchanged,
        "failed": failed,
    }


def main() -> int:
    """Run the one-shot metadata restoration utility."""
    parser = argparse.ArgumentParser(
        description=(
            "Restore original .zip repository metadata into FILE_TASK_HISTORY "
            "for errored processing rows."
        )
    )
    parser.add_argument("--host-id", type=int, default=None)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    logger = log(target_screen=True)
    db = dbHandlerBKP(k.BKP_DATABASE_NAME, logger)
    summary = _process_batch(
        db,
        batch_limit=args.limit,
        host_id=args.host_id,
        dry_run=not args.apply,
        logger=logger,
    )
    logger.entry(
        "[SUMMARY] "
        f"dry_run={not args.apply} scanned={summary['scanned']} "
        f"restored={summary['restored']} unchanged={summary['unchanged']} "
        f"missing_or_ambiguous={summary['missing_or_ambiguous']} "
        f"failed={summary['failed']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
