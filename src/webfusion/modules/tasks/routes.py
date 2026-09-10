"""Render the unified workspace for task creation and queue operations."""

from __future__ import annotations

from flask import render_template

from modules.server.usage_metrics import record_page_view
from modules.tasks.blueprints import tasks_bp, tasks_api_bp


# Import route handlers after the shared blueprints are initialized.
from modules.tasks import queue_routes, station_routes  # noqa: E402,F401


@tasks_bp.route("/", methods=["GET"])
def task_workspace() -> str:
    """Render the shared entry point for all task workflows.

    Args:
        None. Flask supplies the current request context.

    Returns:
        Rendered task workspace. Type: str. It provides links to task creation
        and the queue-management workflows.
    """
    record_page_view()
    return render_template("tasks/workspace.html")