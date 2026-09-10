"""Provide application services for the WebFusion user directory."""

from __future__ import annotations

from auth.db_users import (
    create_webfusion_user,
    delete_webfusion_user,
    get_webfusion_user,
    list_user_filter_options,
    list_webfusion_users,
    update_webfusion_user_privileges,
)


ROLE_FILTERS = (
    ("all", "Todos os papéis"),
    ("admin", "Administrador"),
    ("developer", "Desenvolvedor"),
    ("none", "Sem privilégios"),
)


def get_current_user_profile(user_email: str) -> dict[str, object] | None:
    """Return the stored profile for the current proxy identity.

    Args:
        user_email: Normalized email supplied by the authentication proxy.
            Type: str.

    Returns:
        Stored user profile. Type: dict[str, object] | None. The dictionary
            contains all `USERS` fields plus `NA_ROLE`, `IS_ADMIN`, and
            `IS_DEVELOPER`; returns `None` when the identity is not registered.

    Raises:
        ValueError: If `user_email` has an invalid format.
        Exception: If the database query fails.
    """
    return get_webfusion_user(user_email)


def get_directory_data(filters: dict[str, str | None]) -> dict[str, object]:
    """Build the user directory data required by the rendered page.

    Args:
        filters: Raw directory filters. Type: dict[str, str | None]. Accepted
            keys are `email`, `job_title`, `department`, and `role`; missing
            values are normalized to empty strings or the `all` role.

    Returns:
        Directory view data. Type: dict[str, object]. Contains `users` as
        list[dict[str, object]], `filters` as dict[str, str], `filter_options`
        as dict[str, list[dict[str, str]]], and `role_filters` as
        tuple[tuple[str, str], ...].
    """
    normalized_filters = _normalize_filters(filters)
    return {
        "users": list_webfusion_users(**normalized_filters),
        "filters": normalized_filters,
        "filter_options": list_user_filter_options(),
        "role_filters": ROLE_FILTERS,
    }


def create_directory_user(form: dict[str, str | None]) -> None:
    """Create a manually registered directory user.

    Args:
        form: Submitted registration fields. Type: dict[str, str | None]. Uses
            `user_name`, `user_email`, `job_title`, `department`, `location`,
            `is_admin`, and `is_developer`; the role keys use `1` when enabled.

    Returns:
        None. Persists the normalized user and initial roles.

    Raises:
        ValueError: If the submitted email is invalid or already registered.
        Exception: If the database write fails.
    """
    create_webfusion_user(
        user_name=_form_value(form, "user_name"),
        user_email=_form_value(form, "user_email").casefold(),
        job_title=_form_value(form, "job_title") or None,
        department=_form_value(form, "department") or None,
        location=_form_value(form, "location") or None,
        is_admin=form.get("is_admin") == "1",
        is_developer=form.get("is_developer") == "1",
    )


def update_directory_user_privileges(form: dict[str, str | None]) -> None:
    """Update the active roles for one directory user.

    Args:
        form: Submitted privilege fields. Type: dict[str, str | None]. Uses
            `user_email`, `is_admin`, and `is_developer`; role keys use `1`
            when enabled.

    Returns:
        None. Persists the selected roles for the normalized email.

    Raises:
        ValueError: If the email is invalid.
        LookupError: If the user does not exist.
        Exception: If the database write fails.
    """
    update_webfusion_user_privileges(
        user_email=_form_value(form, "user_email").casefold(),
        is_admin=form.get("is_admin") == "1",
        is_developer=form.get("is_developer") == "1",
    )


def delete_directory_user(form: dict[str, str | None]) -> None:
    """Delete one directory user and its role records.

    Args:
        form: Submitted deletion fields. Type: dict[str, str | None]. Requires
            the `user_email` key.

    Returns:
        None. Deletes the user for the normalized email.

    Raises:
        ValueError: If the email is invalid.
        LookupError: If the user does not exist.
        Exception: If the database write fails.
    """
    delete_webfusion_user(user_email=_form_value(form, "user_email").casefold())


def get_notice_success_message(notice: str | None) -> str | None:
    """Translate a successful redirect code into a user-facing message.

    Args:
        notice: Redirect notice code. Type: str | None.

    Returns:
        Localized success message. Type: str | None. Returns `None` for an
        absent or unsupported code.
    """
    messages = {
        "created": "Usuário cadastrado com sucesso.",
        "privileges_updated": "Privilégios atualizados com sucesso.",
        "deleted": "Usuário e privilégios removidos com sucesso.",
    }
    return messages.get(_form_value({"value": notice}, "value"))


def get_notice_error_message(error: str | None) -> str | None:
    """Translate known redirect errors without exposing database details.

    Args:
        error: Redirect error code or an expected validation message. Type:
            str | None.

    Returns:
        Localized error message. Type: str | None. Returns validation messages
        unchanged and `None` for an absent value.
    """
    messages = {
        "create_failed": "Não foi possível cadastrar o usuário.",
        "privileges_failed": "Não foi possível atualizar os privilégios.",
        "delete_failed": "Não foi possível excluir o usuário.",
    }
    normalized_error = _form_value({"value": error}, "value")
    return messages.get(normalized_error, normalized_error or None)


def _normalize_filters(filters: dict[str, str | None]) -> dict[str, str]:
    """Normalize the accepted user-directory filters.

    Args:
        filters: Raw directory filters. Type: dict[str, str | None]. Supported
            keys are `email`, `job_title`, `department`, and `role`.

    Returns:
        Normalized filters. Type: dict[str, str]. Always contains `email`,
        `job_title`, `department`, and `role`; `role` is one of the values in
        `ROLE_FILTERS` or `all`.
    """
    accepted_roles = {value for value, _ in ROLE_FILTERS}
    role_filter = _form_value(filters, "role").casefold()
    return {
        "email": _form_value(filters, "email").casefold(),
        "job_title": _form_value(filters, "job_title"),
        "department": _form_value(filters, "department"),
        "role": role_filter if role_filter in accepted_roles else "all",
    }


def _form_value(values: dict[str, str | None], name: str) -> str:
    """Return one optional submitted value without surrounding whitespace.

    Args:
        values: Submitted or query values. Type: dict[str, str | None].
        name: Key to read from `values`. Type: str.

    Returns:
        Normalized field value. Type: str. Returns an empty string for missing
        or null values.
    """
    return str(values.get(name) or "").strip()