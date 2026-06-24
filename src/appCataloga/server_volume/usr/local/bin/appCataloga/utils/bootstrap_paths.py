"""
Shared runtime path bootstrap for appCataloga entrypoints.

Most top-level scripts need the same three locations on `sys.path`:
the app root itself, the shared config directory under `etc/appCataloga`,
and the local `db/` package folder. Keeping that bootstrap here removes a
large block of repetitive setup code from every worker.
"""

from __future__ import annotations

import os
import sys


def _find_project_root(entry_file: str) -> str:
    """
    Resolve the appCataloga root even when helpers live in subdirectories.
    """
    current_dir = os.path.dirname(os.path.abspath(entry_file))

    while True:
        has_db = os.path.isdir(os.path.join(current_dir, "db"))
        has_shared = os.path.isdir(os.path.join(current_dir, "shared"))
        if has_db and has_shared:
            return current_dir

        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            raise RuntimeError(
                f"Unable to locate appCataloga project root from {entry_file}"
            )
        current_dir = parent_dir


def _add_path(path: str, *, prepend: bool = False) -> None:
    """
    Add a path to `sys.path` only once.
    """
    if path in sys.path:
        return

    if prepend:
        sys.path.insert(0, path)
    else:
        sys.path.append(path)


def bootstrap_app_paths(entry_file: str) -> str:
    """
    Prepare runtime import paths for an appCataloga entrypoint.

    Returns:
        str: Absolute project root for the calling script.
    """
    project_root = _find_project_root(entry_file)
    _add_path(project_root, prepend=True)

    config_dir = os.path.abspath(
        os.path.join(project_root, "../../../../etc/appCataloga")
    )
    if os.path.isdir(config_dir):
        _add_path(config_dir)

    db_dir = os.path.join(project_root, "db")
    if os.path.isdir(db_dir):
        _add_path(db_dir)

    return project_root
