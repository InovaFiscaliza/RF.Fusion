#!/usr/bin/python3
"""
Repository garbage-collection worker.

Worker responsible for cleaning retired repository artifacts after quarantine.

The worker owns two distinct cleanup channels:
    1. The operator-facing artifact still referenced by `FILE_TASK_HISTORY`
       and quarantined in the main `trash` area
    2. Superseded source/export leftovers quarantined in `trash/resolved_files`

That distinction matters because only the first channel updates
`IS_PAYLOAD_DELETED/DT_PAYLOAD_DELETED`. Files in `resolved_files` are no
longer tracked by `FILE_TASK_HISTORY`; they are cleaned purely by filesystem
retention.

The worker is deliberately small because it is the last step in the file
lifecycle and must stay easy to reason about during production cleanup
incidents.
"""

import os
import sys
import time
from datetime import datetime

from bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)


# ---------------------------------------------------------------
# Internal modules
# ---------------------------------------------------------------
import config as k
from server_handler import signal_runtime
from shared import errors, logging_utils
from db.dbHandlerBKP import dbHandlerBKP


# ===============================================================
# GLOBAL STATE
# ===============================================================

log = logging_utils.log(target_screen=False)
process_status = {"running": True}


signal_runtime.install_shutdown_handlers(
    process_status=process_status,
    logger=log,
)


# ===============================================================
# File operations
# ===============================================================
def _is_path_within(path: str, root: str) -> bool:
    """
    Return whether `path` is inside the managed directory `root`.

    Garbage collection must stay conservative. Using normalized ancestry checks
    is safer than substring matching because a corrupted history path like
    `/mnt/reposfi/trash_bkp` should not be treated as the real trash tree.
    """
    try:
        normalized_path = os.path.normpath(path)
        normalized_root = os.path.normpath(root)
        return os.path.commonpath([normalized_path, normalized_root]) == normalized_root
    except ValueError:
        return False


def delete_file(path):
    """
    Delete one quarantined artifact.

    Behavior:
        - If file exists → delete
        - If file missing → treat as already deleted
    """

    try:
        os.remove(path)
        log.event("garbage_file_deleted", path=path)
        return True

    except FileNotFoundError:
        log.warning(f"event=garbage_file_missing path={path}")
        return True

    except Exception as e:
        log.error(f"event=garbage_delete_failed path={path} error={e}")
        return False


def build_resolved_files_trash_path() -> str:
    """
    Return the dedicated quarantine used by appAnalise export leftovers.

    Files in this folder are no longer the canonical artifact of the
    processing attempt. Because `FILE_TASK_HISTORY` now points elsewhere, the
    garbage collector must clean this area directly from the filesystem.
    """
    return os.path.join(
        k.REPO_FOLDER,
        k.TRASH_FOLDER,
        k.RESOLVED_FILES_TRASH_SUBDIR,
    )


def get_resolved_files_gc_candidates(batch_size: int, quarantine_days: int):
    """
    Return aged files from `trash/resolved_files` for direct filesystem cleanup.

    `task_flow` refreshes the file mtime when an artifact enters this
    quarantine, so filesystem age reflects "time spent in resolved_files"
    rather than the original creation time of the payload.
    """
    resolved_root = build_resolved_files_trash_path()

    if not os.path.isdir(resolved_root):
        return []

    cutoff_ts = time.time() - (quarantine_days * 86400)
    candidates = []

    for root, _, files in os.walk(resolved_root):
        for name in files:
            full_path = os.path.join(root, name)

            try:
                modified_at = os.path.getmtime(full_path)
            except FileNotFoundError:
                continue
            except Exception as e:
                log.error(
                    f"event=garbage_resolved_files_stat_failed path={full_path} error={e}"
                )
                continue

            if modified_at <= cutoff_ts:
                candidates.append((modified_at, full_path))

    candidates.sort(key=lambda item: item[0])
    return [path for _, path in candidates[:batch_size]]


def log_gc_configuration(*, trash_root: str, resolved_root: str) -> None:
    """
    Log the effective retention contract used by the worker.

    Startup logs should make the two GC channels obvious during production
    incidents: one window for the history-owned artifact and a shorter one for
    superseded `resolved_files` leftovers.
    """
    log.event(
        "garbage_configuration",
        trash_root=trash_root,
        trash_quarantine_days=k.GC_QUARANTINE_DAYS,
        resolved_root=resolved_root,
        resolved_quarantine_days=k.GC_RESOLVED_FILES_QUARANTINE_DAYS,
        batch_size=k.GC_BATCH_SIZE,
    )


def collect_gc_candidates(db_bp):
    """
    Gather the next batch of candidates from both GC channels.

    Returns:
        tuple[list[dict], list[str]]: history-owned trash rows and direct
        `resolved_files` filesystem paths.
    """
    history_rows = db_bp.file_history_get_gc_candidates(
        batch_size=k.GC_BATCH_SIZE,
        quarantine_days=k.GC_QUARANTINE_DAYS,
    )
    resolved_rows = get_resolved_files_gc_candidates(
        batch_size=k.GC_BATCH_SIZE,
        quarantine_days=k.GC_RESOLVED_FILES_QUARANTINE_DAYS,
    )
    return history_rows, resolved_rows


def delete_history_artifacts(db_bp, history_rows, *, trash_root: str, resolved_root: str) -> int:
    """
    Delete main-trash artifacts still referenced by `FILE_TASK_HISTORY`.

    Every successful delete updates `IS_PAYLOAD_DELETED/DT_PAYLOAD_DELETED`
    because the history row is the source of truth for operator-facing error
    artifacts.
    """
    deleted = 0

    for row in history_rows:
        server_path = row["NA_SERVER_FILE_PATH"]
        server_file = row["NA_SERVER_FILE_NAME"]

        if not server_path or not server_file:
            log.warning("event=garbage_invalid_path_metadata")
            continue

        # History rows are allowed to delete only the operator-facing
        # artifact that was explicitly finalized into the main trash.
        if not _is_path_within(server_path, trash_root):
            log.error(f"event=garbage_refused_outside_trash path={server_path}")
            continue
        if _is_path_within(server_path, resolved_root):
            log.warning(
                f"event=garbage_history_points_to_resolved_files path={server_path}"
            )
            continue

        file_path = os.path.join(server_path, server_file)

        if delete_file(file_path):
            # These columns describe the lifecycle of the artifact currently
            # referenced by FILE_TASK_HISTORY. They do not say anything about
            # older source/export leftovers in `trash/resolved_files`.
            db_bp.file_history_update(
                history_id=row["ID_HISTORY"],
                IS_PAYLOAD_DELETED=1,
                DT_PAYLOAD_DELETED=datetime.now(),
            )
            log.event(
                "garbage_history_artifact_deleted",
                history_id=row["ID_HISTORY"],
                path=file_path,
            )
            deleted += 1

    return deleted


def delete_resolved_files_artifacts(resolved_rows, *, resolved_root: str) -> int:
    """
    Delete superseded artifacts from `trash/resolved_files`.

    These files are no longer referenced by FILE_TASK_HISTORY, so filesystem
    retention is their only cleanup contract.
    """
    deleted = 0

    for file_path in resolved_rows:
        if not _is_path_within(file_path, resolved_root):
            log.error(
                f"event=garbage_refused_outside_resolved_files path={file_path}"
            )
            continue

        if delete_file(file_path):
            log.event("garbage_resolved_artifact_deleted", path=file_path)
            deleted += 1

    return deleted


# ===============================================================
# Main loop
# ===============================================================
def main():
    """
    Run the garbage-collection loop until shutdown is requested.

    Processing contract summary:
        - main `trash`:
            contains the artifact still referenced by `FILE_TASK_HISTORY`
        - `trash/resolved_files`:
            contains superseded artifacts that no longer have history ownership

    The loop therefore collects from both places, but only the main-trash path
    updates `IS_PAYLOAD_DELETED/DT_PAYLOAD_DELETED`.

    Retention policy:
        - main `trash` uses `GC_QUARANTINE_DAYS`
        - `trash/resolved_files` uses the shorter
          `GC_RESOLVED_FILES_QUARANTINE_DAYS`
    """

    log.service_start("appCataloga_garbage_collector")

    db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    trash_root = os.path.join(k.REPO_FOLDER, k.TRASH_FOLDER)
    resolved_root = build_resolved_files_trash_path()
    log_gc_configuration(
        trash_root=trash_root,
        resolved_root=resolved_root,
    )

    while process_status["running"]:

        try:
            history_rows, resolved_rows = collect_gc_candidates(db_bp)

            if not history_rows and not resolved_rows:
                log.event("garbage_candidates_empty")
                time.sleep(k.GC_IDLE_SLEEP)
                continue

            deleted_history_payloads = delete_history_artifacts(
                db_bp,
                history_rows,
                trash_root=trash_root,
                resolved_root=resolved_root,
            )
            deleted_resolved_files = delete_resolved_files_artifacts(
                resolved_rows,
                resolved_root=resolved_root,
            )
            deleted = deleted_history_payloads + deleted_resolved_files

            db_bp.commit()

            log.event(
                "garbage_batch_processed",
                deleted=deleted,
                deleted_history_payloads=deleted_history_payloads,
                deleted_resolved_files=deleted_resolved_files,
            )

        except Exception as e:
            log.error(f"event=garbage_loop_error error={e}")

        time.sleep(k.GC_LOOP_SLEEP)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal garbage-collector worker crash",
            stage="MAIN",
            exc=e,
        )
        err.log_error()
        raise
