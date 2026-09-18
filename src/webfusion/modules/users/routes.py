"""Render the restricted WebFusion user-directory administration pages."""

from __future__ import annotations
from http import HTTPStatus
from pathlib import Path
from xml.etree import ElementTree
from flask import (
    Blueprint,
    Response,
    current_app,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from modules.server.usage_metrics import record_page_view
from modules.users import service


users_bp = Blueprint("users", __name__, url_prefix="/users")
users_api_bp = Blueprint("users_api", __name__, url_prefix="/api/users")
users_admin_api_bp = Blueprint(
    "users_admin_api",
    __name__,
    url_prefix="/api/users",
)

RESPONSE_IDENTITY_HEADERS = {
    "X-User-Name": "name",
    "X-User-Email": "email",
    "X-User-Job-Title": "job_title",
    "X-User-Department": "department",
    "X-User-Location": "location",
    "X-User-Avatar-Url": "avatar_url",
}
API_ROLE_NAMES = {
    "admin": "admin",
    "developer": "dev",
}
USER_PROFILE_HEADER = "X-User-Profile"


@users_api_bp.route("/avatar.svg", methods=["GET"])
def profile_avatar() -> Response:
    """Render the provided fallback SVG with the current user's initial.

    Args:
        None. Uses `g.webfusion_user`, containing optional `name` and `email`.

    Returns:
        SVG response. Type: flask.Response. Identified users receive
        `profile.svg` with one escaped uppercase initial. Anonymous users
        receive `profile_out.svg`. Personalized responses are never cached.
    """
    identity = g.webfusion_user
    label = identity["name"] or identity["email"]
    filename = "profile.svg" if label else "profile_out.svg"
    root = ElementTree.parse(Path(current_app.static_folder) / "img" / filename).getroot()
    if label:
        initial = root.find(".//{http://www.w3.org/2000/svg}text")
        if initial is None:
            raise ValueError("O SVG de perfil não contém o elemento da inicial.")
        initial.text = label.strip()[0].upper()[0]
    response = Response(ElementTree.tostring(root, encoding="utf-8"), mimetype="image/svg+xml")
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@users_api_bp.route("", methods=["GET"])
@users_api_bp.route("/", methods=["GET"])
def user_headers() -> Response:
    """Return the proxy identity as response headers without a body.

    Args:
        None. Flask supplies the normalized identity through
            `g.webfusion_user`.

    Returns:
        Empty response. Type: flask.Response. The status is 204. The response
        includes the known `X-User-*` identity fields, `X-User-Roles` as
        `user`, `admin`, or `dev`, and disables caching.
    """
    return _identity_headers_response()


def _identity_headers_response() -> Response:
    """Build an empty response containing the normalized proxy identity.

    Args:
        None. Flask supplies the normalized identity through
            `g.webfusion_user`.

    Returns:
        Empty response. Type: flask.Response. The status is 204. The response
        includes the known `X-User-*` identity fields, `X-User-Roles` as
        `user`, `admin`, or `dev`, and disables caching.
    """
    identity = g.webfusion_user
    profile = None
    if identity["email"]:
        try:
            profile = service.get_current_user_profile(identity["email"])
        except Exception:
            current_app.logger.exception("webfusion_user_headers_role_lookup_failed")

    response = Response(status=204)
    for header_name, identity_key in RESPONSE_IDENTITY_HEADERS.items():
        response.headers[header_name] = identity.get(identity_key) or ""

    assigned_role = str(profile.get("NA_ROLE") or "") if profile else ""
    response.headers["X-User-Roles"] = API_ROLE_NAMES.get(assigned_role, "user")
    response.headers["Cache-Control"] = "no-store"
    return response


@users_api_bp.route("/login", methods=["GET"])
def login_probe() -> Response:
    """Return the stored user profile in a header without a response body.

    Args:
        None. Flask supplies the normalized identity through
            `g.webfusion_user`.

    Returns:
        Empty response. Type: flask.Response. The status is 200 and the
        `X-User-Profile` header contains the same JSON object returned by
        `/api/users/me`. The header contains `{}` when no profile is available.
    """
    user_email = g.webfusion_user["email"]
    profile = None
    if user_email:
        try:
            profile = service.get_current_user_profile(user_email)
        except Exception:
            current_app.logger.exception("webfusion_login_profile_lookup_failed")

    response = Response(status=HTTPStatus.OK)
    response.headers[USER_PROFILE_HEADER] = current_app.json.dumps(profile or {})
    response.headers["Cache-Control"] = "no-store"
    return response


@users_api_bp.route("/me", methods=["GET"])
def current_user() -> Response:
    """Return the stored profile for the current proxy identity.

    Args:
        None. Flask supplies the normalized identity through
            `g.webfusion_user`.

    Returns:
        JSON response. Type: flask.Response. A successful response contains
        every `USERS` field plus `NA_ROLE`, `IS_ADMIN`, and `IS_DEVELOPER`.
        Returns 401 without a proxy identity, 404 for an unregistered user,
        or 500 when the profile query fails.
    """
    user_email = g.webfusion_user["email"]
    if not user_email:
        response = jsonify({"error": "proxy_identity_required"})
        response.status_code = 401
        return response

    try:
        profile = service.get_current_user_profile(user_email)
    except Exception:
        current_app.logger.exception("webfusion_current_user_lookup_failed")
        response = jsonify({"error": "current_user_lookup_failed"})
        response.status_code = 500
        return response

    if profile is None:
        response = jsonify({"error": "user_not_found"})
        response.status_code = 404
        return response

    response = jsonify(profile)
    response.headers["Cache-Control"] = "no-store"
    return response


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


@users_admin_api_bp.route("/", methods=["POST"])
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


@users_admin_api_bp.route("/privileges", methods=["POST"])
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


@users_admin_api_bp.route("/delete", methods=["POST"])
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
