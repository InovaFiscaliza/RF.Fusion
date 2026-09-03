"""Render the restricted WebFusion user-directory administration pages."""

from __future__ import annotations
from flask import Blueprint, current_app, redirect, render_template, request, url_for
from modules.server.usage_metrics import record_page_view
from modules.users import service


users_bp = Blueprint("users", __name__, url_prefix="/users")


@users_bp.route("/", methods=["GET"])
def user_directory():
    """Render the searchable identity directory and privilege controls.

    Args:
        None. Flask supplies optional query parameters through `request.args`:
            `email`, `job_title`, and `department` are str filters; `role` is
            a str role filter; `error` and `notice` are str feedback codes.

    Returns:
        Rendered directory page. Type: str. The template receives `users` as
        list[dict], `filters` as dict[str, str], `filter_options` as
        dict[str, list[str]], `role_filters` as tuple[str, ...], and feedback
        messages as str or None.
    """
    error_message = service.get_notice_error_message(request.args.get("error"))
    success_message = service.get_notice_success_message(request.args.get("notice"))
    directory_data = {
        "users": [],
        "filters": {"email": "", "job_title": "", "department": "", "role": "all"},
        "filter_options": {"users": [], "job_titles": [], "departments": []},
        "role_filters": service.ROLE_FILTERS,
    }

    try:
        directory_data = service.get_directory_data(request.args)
    except Exception:
        current_app.logger.exception("webfusion_user_directory_list_failed")
        error_message = "Não foi possível consultar os usuários cadastrados."

    record_page_view()
    return render_template(
        "users/users.html",
        **directory_data,
        error_message=error_message,
        success_message=success_message,
    )


@users_bp.route("/", methods=["POST"])
def create_user():
    """Create a directory user before its first F5-authenticated visit.

    Args:
        None. Flask supplies form data through `request.form`. The submitted
            fields are validated by the user-directory service.

    Returns:
        Redirect response. Type: Response. Redirects to the directory with a
        success notice, a validation error, or a generic creation error.

    Raises:
        None. Validation and persistence exceptions are converted into
            directory feedback messages.
    """
    try:
        service.create_directory_user(request.form)
    except ValueError as error:
        return redirect(url_for("users.user_directory", error=str(error)))
    except Exception:
        current_app.logger.exception("webfusion_user_create_failed")
        return redirect(url_for("users.user_directory", error="create_failed"))

    return redirect(url_for("users.user_directory", notice="created"))


@users_bp.route("/privileges", methods=["POST"])
def update_user_privileges():
    """Apply the selected privileges for one directory user.

    Args:
        None. Flask supplies form data through `request.form`. The submitted
            identity and privilege fields are validated by the
            user-directory service.

    Returns:
        Redirect response. Type: Response. Redirects to the directory with a
        success notice, a validation error, or a generic privileges error.

    Raises:
        None. Lookup, validation, and persistence exceptions are converted
            into directory feedback messages.
    """
    try:
        service.update_directory_user_privileges(request.form)
    except (LookupError, ValueError) as error:
        return redirect(url_for("users.user_directory", error=str(error)))
    except Exception:
        current_app.logger.exception("webfusion_user_privileges_update_failed")
        return redirect(url_for("users.user_directory", error="privileges_failed"))

    return redirect(url_for("users.user_directory", notice="privileges_updated"))


@users_bp.route("/delete", methods=["POST"])
def delete_user():
    """Delete one directory identity and all of its WebFusion privileges.

    Args:
        None. Flask supplies form data through `request.form`. The submitted
            identity field is validated by the user-directory service.

    Returns:
        Redirect response. Type: Response. Redirects to the directory with a
        success notice, a validation error, or a generic deletion error.

    Raises:
        None. Lookup, validation, and persistence exceptions are converted
            into directory feedback messages.
    """
    try:
        service.delete_directory_user(request.form)
    except (LookupError, ValueError) as error:
        return redirect(url_for("users.user_directory", error=str(error)))
    except Exception:
        current_app.logger.exception("webfusion_user_delete_failed")
        return redirect(url_for("users.user_directory", error="delete_failed"))

    return redirect(url_for("users.user_directory", notice="deleted"))
