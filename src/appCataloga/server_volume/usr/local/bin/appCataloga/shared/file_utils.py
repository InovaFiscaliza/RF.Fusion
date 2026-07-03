
from __future__ import annotations

import errno
import hashlib
import os
import time
from typing import Any, Protocol

import config as k


TRANSIENT_FILESYSTEM_ERRNOS = {
    errno.EBUSY,
    errno.EAGAIN,
    errno.ESTALE,
    errno.ETXTBSY,
}


def _emit_file_event(
    logger: "FileEventLogger" | None,
    level: str,
    event: str,
    **fields,
) -> None:
    """Emit one structured file-utils event when a logger is available."""
    if logger is None:
        return

    payload = {
        "component": "file_utils",
        **fields,
    }
    getattr(logger, level)(event, **payload)

# ======================================================================
# Server filename builder (ARCHITECTURAL CONTRACT)
# ======================================================================
def build_server_filename(host_uid: str, remote_path: str, filename: str) -> str:
    """
    Build a deterministic server-side filename.

    This is the single source of truth for server-side backup filenames.
    It must not be reimplemented elsewhere, otherwise reprocessing and file
    lineage become inconsistent.

    Pattern:
        p-<hash>--<original_filename>

    Hash source:
        sha1(host_uid + ":" + remote_path)[:8]

    The hash:
        • Prevents filename collisions
        • Is stable across reprocessing
        • Does NOT depend on server paths

    Args:
        host_uid (str): Unique identifier of the host/station
        remote_path (str): Absolute path on the remote host
        filename (str): Original filename on the host

    Returns:
        str: Server-side filename
    """
    # CelPlan payloads keep their original filename because the station
    # naming is already unique and downstream tooling expects it.
    if "CW" in host_uid:
        return filename

    h = hashlib.sha1(
        f"{host_uid}:{remote_path}".encode("utf-8")
    ).hexdigest()[:8]

    return f"p-{h}--{filename}"


def build_server_filepath(host_uid: str) -> str:
    """
    Build and guarantee the server-side staging directory for a host.

    Creates the directory if it does not exist. Returns the absolute path.
    """
    path = os.path.join(k.REPO_FOLDER, k.TMP_FOLDER, host_uid)
    os.makedirs(path, exist_ok=True)
    return path


def is_transient_filesystem_error(exc: Exception) -> bool:
    """Return whether one filesystem failure is worth retrying."""
    return isinstance(exc, OSError) and exc.errno in TRANSIENT_FILESYSTEM_ERRNOS


def file_move(
    filename: str,
    path: str,
    new_path: str,
    *,
    refresh_mtime: bool = False,
) -> dict:
    """Move one artifact into its next canonical directory."""
    source = os.path.join(path, filename)
    target = os.path.join(new_path, filename)

    os.makedirs(new_path, exist_ok=True)

    for attempt in range(3):
        try:
            os.rename(source, target)
            break
        except OSError as exc:
            if not is_transient_filesystem_error(exc) or attempt == 2:
                raise OSError(
                    exc.errno,
                    f"{exc.strerror}: {source} -> {target}",
                ) from exc
            time.sleep(0.5)

    if refresh_mtime:
        for attempt in range(3):
            try:
                os.utime(target, None)
                break
            except OSError as exc:
                if not is_transient_filesystem_error(exc) or attempt == 2:
                    raise OSError(
                        exc.errno,
                        f"{exc.strerror}: {target}",
                    ) from exc
                time.sleep(0.5)

    return {"filename": filename, "path": new_path}


def build_history_metadata_from_file_meta(file_meta: dict) -> dict:
    """Project worker file metadata into FILE_TASK_HISTORY-style fields."""
    return {
        "name": file_meta["file_name"],
        "extension": file_meta["extension"],
        "size_kb": file_meta["size_kb"],
        "dt_created": file_meta["dt_created"],
        "dt_modified": file_meta["dt_modified"],
    }


def is_same_file(file_a: dict | None, file_b: dict | None) -> bool:
    """Check whether two metadata dictionaries point to the same path."""
    if not file_a or not file_b:
        return False

    path_a = os.path.normpath(file_a["full_path"])
    path_b = os.path.normpath(file_b["full_path"])
    return path_a == path_b


def move_file_if_present(
    file_meta: dict | None,
    destination_path: str,
    *,
    refresh_mtime: bool = False,
    move_kind: str | None = None,
    logger: "FileEventLogger" | None = None,
) -> dict | None:
    """Move one file when it still exists and return its new metadata."""
    if not file_meta or not os.path.exists(file_meta["full_path"]):
        return None

    source_path = file_meta["full_path"]
    target_path = os.path.join(destination_path, file_meta["file_name"])
    event_fields = {
        "operation": "move_file_if_present",
        "file": file_meta["file_name"],
        "source_dir": file_meta["file_path"],
        "destiny_dir": destination_path,
        "success": False,
        "refresh_mtime": refresh_mtime,
    }
    if move_kind:
        event_fields["kind"] = move_kind

    try:
        file_move(
            filename=file_meta["file_name"],
            path=file_meta["file_path"],
            new_path=destination_path,
            refresh_mtime=refresh_mtime,
        )
    except Exception as exc:
        _emit_file_event(
            logger,
            "error_event",
            "file_move",
            **event_fields,
            source=source_path,
            destiny=target_path,
            error_type=type(exc).__name__,
            error=str(exc),
        )
        raise

    event_fields["success"] = True
    _emit_file_event(
        logger,
        "event",
        "file_move",
        **event_fields,
    )

    moved_meta = dict(file_meta)
    moved_meta["file_path"] = destination_path
    moved_meta["full_path"] = os.path.join(destination_path, file_meta["file_name"])
    return moved_meta


def delete_file_if_present(
    file_meta: dict | None,
    *,
    delete_kind: str | None = None,
    logger: "FileEventLogger" | None = None,
) -> dict | None:
    """Delete one file when it still exists and return its original metadata."""
    if not file_meta or not os.path.exists(file_meta["full_path"]):
        return None

    target_path = file_meta["full_path"]
    event_fields = {
        "operation": "delete_file_if_present",
        "file": file_meta["file_name"],
        "source_dir": file_meta["file_path"],
        "success": False,
    }
    if delete_kind:
        event_fields["kind"] = delete_kind

    for attempt in range(3):
        try:
            os.remove(target_path)
            break
        except OSError as exc:
            if not is_transient_filesystem_error(exc) or attempt == 2:
                _emit_file_event(
                    logger,
                    "error_event",
                    "file_delete",
                    **event_fields,
                    source=target_path,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                raise
            time.sleep(0.5)

    event_fields["success"] = True
    _emit_file_event(
        logger,
        "event",
        "file_delete",
        **event_fields,
    )
    return dict(file_meta)


def build_resolved_files_trash_path() -> str:
    """Return the dedicated quarantine for export-resolved leftovers."""
    return os.path.join(
        k.REPO_FOLDER,
        k.TRASH_FOLDER,
        k.RESOLVED_FILES_TRASH_SUBDIR,
    )


def promote_final_artifact(
    new_path: str,
    file_meta: dict,
    source_file_meta: dict,
    export: bool,
    filename: str,
    *,
    logger: "FileEventLogger" | None = None,
) -> dict:
    """Move the canonical artifact into the repository folder."""
    final_file_meta = move_file_if_present(
        file_meta,
        new_path,
        move_kind="promote_final",
        logger=logger,
    )

    if final_file_meta is None:
        raise FileNotFoundError(f"Final output file unavailable: {file_meta}")

    if export and not is_same_file(source_file_meta, final_file_meta):
        move_file_if_present(
            source_file_meta,
            build_resolved_files_trash_path(),
            refresh_mtime=True,
            move_kind="quarantine_source",
            logger=logger,
        )

    _emit_file_event(
        logger,
        "event",
        "processing_completed",
        operation="promote_final_artifact",
        file=filename,
        export=export,
        final_file=final_file_meta["full_path"],
    )

    return final_file_meta


def quarantine_error_artifact(
    file_meta: dict | None,
    source_file_meta: dict | None,
    export: bool,
    *,
    logger: "FileEventLogger" | None = None,
) -> tuple[str | None, dict | None]:
    """Move error artifacts to quarantine and return history linkage data."""
    if not source_file_meta:
        return None, None

    trash_path = os.path.join(k.REPO_FOLDER, k.TRASH_FOLDER)
    resolved_trash_path = build_resolved_files_trash_path()
    history_meta_override = None
    server_path = None

    distinct_export = (
        export and file_meta and not is_same_file(file_meta, source_file_meta)
    )

    if distinct_export:
        delete_file_if_present(
            file_meta,
            delete_kind="quarantine_error_export",
            logger=logger,
        )
        trashed_source_meta = move_file_if_present(
            source_file_meta,
            trash_path,
            move_kind="quarantine_error_source",
            logger=logger,
        )
        if trashed_source_meta:
            server_path = trashed_source_meta["file_path"]
    else:
        trashed_source_meta = move_file_if_present(
            source_file_meta,
            trash_path,
            move_kind="quarantine_error_source",
            logger=logger,
        )
        if trashed_source_meta:
            server_path = trashed_source_meta["file_path"]

    if server_path is None:
        server_path = source_file_meta.get("file_path")

    return server_path, history_meta_override


class FileEventLogger(Protocol):
    """Minimal logger contract required by file-utils helpers."""

    def event(self, event: str, **fields: Any) -> None:
        ...

    def error_event(self, event: str, **fields: Any) -> None:
        ...
