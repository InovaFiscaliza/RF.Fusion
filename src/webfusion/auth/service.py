"""Manage request identities supplied by the upstream authentication proxy.

This module normalizes identity headers, limits access-role lookups to the
routes that require them, and records observed profiles without blocking an
HTTP response.
"""

from __future__ import annotations

import logging
import re

from flask import Request

from auth.db_users import get_access_role, register_observed_user


ANONYMOUS_USER_LABEL = "Anônimo"
IDENTITY_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
ACCESS_RESTRICTED_BLUEPRINTS = frozenset(
    {
        "tasks",
        "tasks_api",
        "users",
        "users_admin_api",
        "configuration",
        "configuration_api",
    }
)


class AuthService:
    """Build, persist, and authorize identities for WebFusion requests.

    Use this service from Flask request hooks to convert proxy headers into a
    normalized identity and load access roles when a route or template needs
    them.

    Attributes:
        observed_user_profiles: Profiles already persisted by this process,
            indexed by normalized email. Type:
            dict[str, tuple[str | None, str | None, str | None, str | None]].
            Each tuple contains the name, job title, department, and location,
            in that order. It starts as an empty dictionary.
    """

    def __init__(self) -> None:
        """Initialize the in-memory cache of observed user profiles.

        Args:
            None.

        Returns:
            None. Creates an empty `observed_user_profiles` dictionary.
        """
        self.observed_user_profiles: dict[
            str,
            tuple[str | None, str | None, str | None, str | None],
        ] = {}

    def identity_header_value(self, request: Request, name: str) -> str | None:
        """Return an identity header normalized after the WSGI header decode.

        Args:
            request: Flask request containing headers forwarded by the
                authentication proxy. Type: flask.Request.
            name: Name of the identity header to read. Type: str.

        Returns:
            Normalized header value. Type: str | None. Returns `None` when the
            header is missing or blank, and repairs UTF-8 text decoded as
            Latin-1 by WSGI when possible.
        """
        value = str(request.headers.get(name) or "").strip()
        if not value:
            return None

        try:
            return value.encode("latin-1").decode("utf-8")
        except UnicodeError:
            return value

    def request_identity(self, request: Request) -> dict[str, str | None]:
        """Build the current identity from headers set by the authentication proxy.

        Args:
            request: Flask request containing forwarded `X-User-*` headers.
                Type: flask.Request.

        Returns:
            Normalized request identity. Type: dict[str, str | None]. Contains
            the required keys `name`, `email`, `job_title`, `department`,
            `location`, `avatar_url`, `label`, and `role`. All values except
            `label` may be `None`.
        """
        header_email = self.identity_header_value(request, "X-User-Email")
        email = header_email.casefold() if self.is_valid_identity_email(header_email) else None
        name = self.normalize_identity_name(
            self.identity_header_value(request, "X-User-Name"),
            email,
        )
        return {
            "name": name,
            "email": email,
            "job_title": self.identity_header_value(request, "X-User-Job-Title"),
            "department": self.identity_header_value(request, "X-User-Department"),
            "location": self.identity_header_value(request, "X-User-Location"),
            "avatar_url": self.identity_header_value(request, "X-User-Avatar-Url"),
            "label": name or email or ANONYMOUS_USER_LABEL,
            "role": None,
        }

    def is_restricted_blueprint(self, blueprint_name: str) -> bool:
        """Return whether a blueprint requires an active WebFusion role.

        Args:
            blueprint_name: Blueprint name extracted from a Flask endpoint.
                Type: str.

        Returns:
            Access restriction result. Type: bool. `True` means the request
            must have an active WebFusion role.
        """
        return blueprint_name in ACCESS_RESTRICTED_BLUEPRINTS

    def is_valid_identity_email(self, value: str | None) -> bool:
        """Validate an identity email forwarded by the authentication proxy.

        Args:
            value: Email value read from an authentication header. Type:
                str | None.

        Returns:
            Validation result. Type: bool. `True` only when the value is one
            non-empty email address in the expected format.
        """
        return bool(IDENTITY_EMAIL_PATTERN.fullmatch(str(value or "").strip()))

    def normalize_identity_name(self, name: str | None, email: str | None) -> str | None:
        """Remove an email duplicated inside a display name sent by the proxy.

        Args:
            name: Display name read from an identity header. Type: str | None.
            email: Normalized email for the same identity. Type: str | None.

        Returns:
            Display name without a duplicated email. Type: str | None. Returns
            `None` when the name is missing, blank, or becomes blank after
            normalization.
        """
        normalized_name = str(name or "").strip()
        if not normalized_name or not email:
            return normalized_name or None

        if email not in normalized_name.casefold():
            return normalized_name

        normalized_name = re.sub(re.escape(email), "", normalized_name, flags=re.IGNORECASE)
        return normalized_name.strip(" -|") or None

    def record_observed_user(
        self,
        identity: dict[str, str | None],
        logger: logging.Logger,
    ) -> None:
        """Register a newly observed F5 identity without blocking navigation.

        Args:
            identity: Normalized request identity. Type: dict[str, str | None].
                Must contain `name`, `email`, `job_title`, `department`, and
                `location`. The profile is persisted only when `email` is valid.
            logger: Logger used to record failures without interrupting the HTTP
                response. Type: logging.Logger.

        Returns:
            None. Persists a valid profile when it differs from the matching
            entry in `observed_user_profiles`; logs and suppresses failures.
        """
        user_email = identity["email"]
        user_profile = (
            identity["name"],
            identity["job_title"],
            identity["department"],
            identity["location"],
        )
        if not self.is_valid_identity_email(user_email):
            logger.warning("webfusion_observed_user_ignored_invalid_email")
            return

        # Repeated page renders must not write an unchanged profile again.
        if self.observed_user_profiles.get(user_email) == user_profile:
            return

        try:
            register_observed_user(
                user_name=identity["name"],
                user_email=user_email,
                job_title=identity["job_title"],
                department=identity["department"],
                location=identity["location"],
            )
        except Exception:
            logger.exception("webfusion_observed_user_registration_failed")
            return

        self.observed_user_profiles[user_email] = user_profile

    def load_access_role(
        self,
        identity: dict[str, str | None],
        logger: logging.Logger,
    ) -> None:
        """Load the role once when a route guard or template needs it.

        Args:
            identity: Normalized request identity. Type: dict[str, str | None].
                Must contain `email` and `role`; this method updates `role` with
                the access role found for the email or leaves it as `None`.
            logger: Logger used to record role lookup failures. Type:
                logging.Logger.

        Returns:
            None. Looks up and updates `identity["role"]` at most once for a
            single identity dictionary with a valid email.
        """
        user_email = identity["email"]
        if not user_email or identity["role"] is not None:
            return

        try:
            identity["role"] = get_access_role(user_email)
        except Exception:
            logger.exception("webfusion_access_role_lookup_failed")