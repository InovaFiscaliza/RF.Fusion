"""Render the restricted WebFusion user-directory administration pages."""

from __future__ import annotations

from flask import Blueprint, current_app, redirect, render_template, request, url_for

from db import (
    create_webfusion_user,
    delete_webfusion_user,
    list_webfusion_users,
    update_webfusion_user_privileges,
)
from modules.server.usage_metrics import record_page_view


users_bp = Blueprint("users", __name__, url_prefix="/users")


@users_bp.route("/", methods=["GET"])
def user_directory():
    """Render the searchable identity directory and privilege controls."""
    search = str(request.args.get("search") or "").strip()
    error_message = _notice_error_message(request.args.get("error"))
    success_message = _notice_success_message(request.args.get("notice"))
    users = []

    try:
        users = list_webfusion_users(search)
    except Exception:
        current_app.logger.exception("webfusion_user_directory_list_failed")
        error_message = "Não foi possível consultar os usuários cadastrados."

    record_page_view()
    return render_template(
        "users/users.html",
        users=users,
        search=search,
        error_message=error_message,
        success_message=success_message,
    )


@users_bp.route("/", methods=["POST"])
def create_user():
    """Create a manual directory user before its first F5-authenticated visit."""
    try:
        create_webfusion_user(
            user_name=_form_value("user_name"),
            user_email=_form_value("user_email").casefold(),
            job_title=_form_value("job_title") or None,
            department=_form_value("department") or None,
            location=_form_value("location") or None,
            is_admin=request.form.get("is_admin") == "1",
            is_developer=request.form.get("is_developer") == "1",
        )
    except ValueError as error:
        return redirect(url_for("users.user_directory", error=str(error)))
    except Exception:
        current_app.logger.exception("webfusion_user_create_failed")
        return redirect(url_for("users.user_directory", error="create_failed"))

    return redirect(url_for("users.user_directory", notice="created"))


@users_bp.route("/privileges", methods=["POST"])
def update_user_privileges():
    """Apply the active admin and developer privileges for one directory user."""
    try:
        update_webfusion_user_privileges(
            user_email=_form_value("user_email").casefold(),
            is_admin=request.form.get("is_admin") == "1",
            is_developer=request.form.get("is_developer") == "1",
        )
    except (LookupError, ValueError) as error:
        return redirect(url_for("users.user_directory", error=str(error)))
    except Exception:
        current_app.logger.exception("webfusion_user_privileges_update_failed")
        return redirect(url_for("users.user_directory", error="privileges_failed"))

    return redirect(url_for("users.user_directory", notice="privileges_updated"))


@users_bp.route("/delete", methods=["POST"])
def delete_user():
    """Delete one directory identity and all of its WebFusion privileges."""
    try:
        delete_webfusion_user(user_email=_form_value("user_email").casefold())
    except (LookupError, ValueError) as error:
        return redirect(url_for("users.user_directory", error=str(error)))
    except Exception:
        current_app.logger.exception("webfusion_user_delete_failed")
        return redirect(url_for("users.user_directory", error="delete_failed"))

    return redirect(url_for("users.user_directory", notice="deleted"))


def _form_value(name: str) -> str:
    """Return one trimmed form field value."""
    return str(request.form.get(name) or "").strip()


def _notice_success_message(notice: str | None) -> str | None:
    """Translate successful redirects into concise operator notices."""
    messages = {
        "created": "Usuário cadastrado com sucesso.",
        "privileges_updated": "Privilégios atualizados com sucesso.",
        "deleted": "Usuário e privilégios removidos com sucesso.",
    }
    return messages.get(str(notice or "").strip())


def _notice_error_message(error: str | None) -> str | None:
    """Keep redirect errors clear without exposing database details."""
    messages = {
        "create_failed": "Não foi possível cadastrar o usuário.",
        "privileges_failed": "Não foi possível atualizar os privilégios.",
        "delete_failed": "Não foi possível excluir o usuário.",
    }
    normalized_error = str(error or "").strip()
    return messages.get(normalized_error, normalized_error or None)
