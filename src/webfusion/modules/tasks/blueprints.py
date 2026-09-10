"""Shared blueprints for the task workspace and its APIs."""

from flask import Blueprint


tasks_bp = Blueprint("tasks", __name__, url_prefix="/tasks")
tasks_api_bp = Blueprint("tasks_api", __name__, url_prefix="/api/tasks")
