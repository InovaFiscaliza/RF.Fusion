"""Manage request identities supplied by the upstream authentication proxy.

This module normalizes identity headers, limits access-role lookups to the
routes that require them, and records observed profiles without blocking an
HTTP response.
"""

from __future__ import annotations

import logging
import re
from collections import OrderedDict
from threading import Lock
from time import monotonic
from urllib.parse import urlsplit

from flask import Request

from api import microsoft_api
from auth.config import (
    PROFILE_IMAGE_URL_MAX_LENGTH, PROFILE_IMAGE_REFRESH_SECONDS,
    PROFILE_IMAGE_RETRY_SECONDS, PROFILE_IMAGE_CACHE_LIMIT,
)
from auth.db_users import (
    get_access_role, get_profile_image_url, register_observed_user, update_profile_image_url,
)


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
            dict[str, tuple[str | None, ...]].
            Each tuple contains name, job title, department, location, and photo,
            in that order. It starts as an empty dictionary.
        photo_refresh_after: Bounded refresh deadlines indexed by email.
            Type: OrderedDict[str, float]. Values use monotonic seconds.
        _photo_refresh_lock: Lock protecting refresh reservations between
            Waitress threads. Type: threading.Lock.
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
            tuple[str | None, ...],
        ] = {}
        self.photo_refresh_after: OrderedDict[str, float] = OrderedDict()
        self._photo_refresh_lock = Lock()

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
            "avatar_url": self.normalize_profile_image_url(
                self.identity_header_value(request, "X-User-Avatar-Url")
            ),
            "label": name or email or ANONYMOUS_USER_LABEL,
            "role": None,
        }

    def normalize_profile_image_url(self, value: str | None) -> str | None:
        """Accept a local photo path or a credential-free HTTPS URL.

        Args:
            value: Optional header or stored image URL. Type: str | None.

        Returns:
            Valid image URL. Type: str | None. Unsupported or malformed URLs
            are treated as absent. Query strings and fragments are rejected
            so signed URLs and bearer credentials are not persisted.
        """
        value = str(value or "").strip()
        if not value or len(value) > PROFILE_IMAGE_URL_MAX_LENGTH:
            return None
        if "\\" in value or any(character.isspace() or ord(character) < 32 for character in value):
            return None
        try:
            parsed = urlsplit(value)
            if parsed.query or parsed.fragment or parsed.username or parsed.password:
                return None
            if value.startswith("/") and not value.startswith("//"):
                return value
            if parsed.scheme == "https" and parsed.hostname and parsed.port in (None, 443):
                return value
        except ValueError:
            return None
        return None

    def load_profile_image(
        self, identity: dict[str, str | None], logger: logging.Logger,
    ) -> None:
        """Load the stored photo and periodically synchronize it with Graph.

        Args:
            identity: Request identity with `email` and `avatar_url` keys.
                Type: dict[str, str | None].
            logger: Logger for failed lookups. Type: logging.Logger.

        Returns:
            None. Sets `avatar_url` from the database or Graph. Without a token,
            photo, or successful request, the stored image remains available.
            A valid proxy image takes precedence over Graph synchronization.
        """
        if identity["avatar_url"] or not identity["email"]:
            return
        try:
            identity["avatar_url"] = self.normalize_profile_image_url(
                get_profile_image_url(identity["email"])
            )
        except Exception:
            logger.exception("webfusion_profile_image_lookup_failed")
            return

        user_email = identity["email"]
        now = monotonic()
        with self._photo_refresh_lock:
            if self.photo_refresh_after.get(user_email, 0) > now:
                return
            # Reserve before network I/O to avoid duplicate requests across threads.
            self.photo_refresh_after[user_email] = now + PROFILE_IMAGE_RETRY_SECONDS
            self.photo_refresh_after.move_to_end(user_email)
            if len(self.photo_refresh_after) > PROFILE_IMAGE_CACHE_LIMIT:
                self.photo_refresh_after.popitem(last=False)
        try:
            image_url = microsoft_api.get_profile_image_url(user_email)
            if image_url != identity["avatar_url"]:
                update_profile_image_url(user_email, image_url)
            identity["avatar_url"] = image_url
        except microsoft_api.MicrosoftNotConfigured:
            return
        except microsoft_api.MicrosoftPhotoNotFound:
            # A missing remote photo must not erase an existing local image.
            pass
        except microsoft_api.MicrosoftPhotoError as error:
            logger.warning("webfusion_microsoft_photo_failed: %s", error)
            return
        except Exception:
            logger.exception("webfusion_profile_image_update_failed")
            return
        with self._photo_refresh_lock:
            self.photo_refresh_after[user_email] = monotonic() + PROFILE_IMAGE_REFRESH_SECONDS

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
                `location`, and optionally `avatar_url`. The profile is
                persisted only when `email` is valid.
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
            identity.get("avatar_url"),
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
                profile_image_url=identity.get("avatar_url"),
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
