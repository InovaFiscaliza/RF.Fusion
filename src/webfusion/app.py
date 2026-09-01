"""Assemble the WebFusion Flask application.

This module owns only cross-feature routes:

- the landing page shell
- the summary-backed station-map APIs used by that page
- the container liveness and proxy-diagnostic endpoints

Feature pages and their JSON APIs live in blueprints under ``modules/``. The
application does not implement queue, catalog, or Zabbix rules directly; those
rules stay in their feature service modules.
"""

import os
import logging
import re
import sys
from pathlib import Path

# The running container mounts the repository but older images may not yet
# export PYTHONPATH. Keep the source-level Zabbix client importable in both
# old and rebuilt WebFusion containers.
SOURCE_ROOT = Path(__file__).resolve().parents[1]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from flask import Flask, Response, g, request, render_template, jsonify
from waitress import serve
from werkzeug.middleware.proxy_fix import ProxyFix
from modules.spectrum.routes import spectrum_bp
from modules.host.routes import host_bp
from modules.server.routes import server_bp
from modules.task.routes import task_bp
from modules.maintenance.routes import maintenance_bp
from modules.users.routes import users_bp
from modules.zabbix_configuration.routes import zabbix_configuration_bp
from modules.map.service import (
    get_station_map_points,
    get_station_map_site_detail,
)
from modules.server.usage_metrics import record_page_view
from db import get_access_role, register_observed_user


app = Flask(__name__)
# Nginx is the only upstream proxy and supplies the public `/rffusion` prefix.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_prefix=1)
app.logger.setLevel(logging.INFO)

ACCESS_RESTRICTED_BLUEPRINTS = frozenset(
    {
        "maintenance",
        "task",
        "users",
        "zabbix_configuration",
    }
)
ANONYMOUS_USER_LABEL = "Anônimo"
IDENTITY_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
OBSERVED_USER_PROFILES: dict[
    str,
    tuple[str | None, str | None, str | None, str | None],
] = {}

# Register feature blueprints first; the app-level routes below are kept only
# for the landing page and a few small cross-module helpers.

app.register_blueprint(spectrum_bp)
app.register_blueprint(host_bp)
app.register_blueprint(server_bp)
app.register_blueprint(task_bp)
app.register_blueprint(maintenance_bp)
app.register_blueprint(users_bp)
app.register_blueprint(zabbix_configuration_bp)


def _identity_header_value(name: str) -> str | None:
    """Return an identity header normalized after the WSGI header decode."""
    value = str(request.headers.get(name) or "").strip()
    if not value:
        return None

    try:
        return value.encode("latin-1").decode("utf-8")
    except UnicodeError:
        return value


def _request_identity() -> dict[str, str | None]:
    """Build the current identity from headers set by the authentication proxy."""
    header_email = _identity_header_value("X-User-Email")
    email = header_email.casefold() if _is_valid_identity_email(header_email) else None
    name = _normalize_identity_name(_identity_header_value("X-User-Name"), email)
    return {
        "name": name,
        "email": email,
        "job_title": _identity_header_value("X-User-Job-Title"),
        "department": _identity_header_value("X-User-Department"),
        "location": _identity_header_value("X-User-Location"),
        "label": name or email or ANONYMOUS_USER_LABEL,
        "role": None,
    }


def _is_valid_identity_email(value: str | None) -> bool:
    """Accept only one non-empty email address from the authentication proxy."""
    return bool(IDENTITY_EMAIL_PATTERN.fullmatch(str(value or "").strip()))


def _normalize_identity_name(name: str | None, email: str | None) -> str | None:
    """Remove an email duplicated inside a display name sent by the proxy."""
    normalized_name = str(name or "").strip()
    if not normalized_name or not email:
        return normalized_name or None

    if email not in normalized_name.casefold():
        return normalized_name

    normalized_name = re.sub(re.escape(email), "", normalized_name, flags=re.IGNORECASE)
    return normalized_name.strip(" -|") or None


def _record_observed_user(identity: dict[str, str | None]) -> None:
    """Register a newly observed F5 identity without blocking navigation."""
    user_email = identity["email"]
    user_profile = (
        identity["name"],
        identity["job_title"],
        identity["department"],
        identity["location"],
    )
    if not _is_valid_identity_email(user_email):
        app.logger.warning("webfusion_observed_user_ignored_invalid_email")
        return

    if OBSERVED_USER_PROFILES.get(user_email) == user_profile:
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
        app.logger.exception("webfusion_observed_user_registration_failed")
        return

    OBSERVED_USER_PROFILES[user_email] = user_profile


def _load_access_role(identity: dict[str, str | None]) -> None:
    """Load the role once when a route guard or template needs it."""
    user_email = identity["email"]
    if not user_email or identity["role"] is not None:
        return

    try:
        identity["role"] = get_access_role(user_email)
    except Exception:
        app.logger.exception("webfusion_access_role_lookup_failed")


@app.before_request
def load_request_identity() -> None:
    """Make the proxy identity available to route guards and base templates."""
    identity = _request_identity()
    endpoint = request.endpoint or ""
    blueprint_name = endpoint.partition(".")[0]
    if blueprint_name in ACCESS_RESTRICTED_BLUEPRINTS:
        _load_access_role(identity)
    g.webfusion_user = identity


@app.before_request
def require_restricted_access() -> Response | None:
    """Allow protected WebFusion modules only to configured active users."""
    endpoint = request.endpoint or ""
    blueprint_name = endpoint.partition(".")[0]
    if blueprint_name not in ACCESS_RESTRICTED_BLUEPRINTS:
        return None

    if g.webfusion_user["role"] is None:
        return Response("Acesso restrito.", 403)
    return None


@app.context_processor
def inject_request_identity() -> dict[str, dict[str, str | None]]:
    """Expose the current proxy identity to every rendered base template."""
    identity = getattr(g, "webfusion_user", _request_identity())
    _record_observed_user(identity)
    _load_access_role(identity)
    return {"current_user": identity}

@app.route("/")
def index():
    """Render the landing page shell.

    The first map snapshot is embedded in the page. This avoids a second
    browser request that can remain pending after a history-cache restore.
    """
    record_page_view()
    try:
        initial_map_points = get_station_map_points()
    except Exception:
        app.logger.exception("failed_to_build_initial_station_map")
        initial_map_points = []

    return render_template("index.html", initial_map_points=initial_map_points)


@app.route("/api/map/stations")
def map_stations():
    """Return the cached summary-backed station map payload.

    The page intentionally renders before this endpoint resolves, so failures
    should degrade to an empty map instead of breaking the whole landing page.
    """
    start_date = request.args.get("start_date") or None
    end_date = request.args.get("end_date") or None

    try:
        return jsonify(
            {
                "points": get_station_map_points(
                    start_date=start_date,
                    end_date=end_date,
                )
            }
        )
    except Exception:
        app.logger.exception("failed_to_build_station_map")
        # The UI treats an empty dataset as a degraded-but-usable state.
        return jsonify({"points": []})


@app.route("/api/map/stations/<int:site_id>")
def map_station_detail(site_id):
    """Return popup metadata for one map point.

    The popup is loaded on demand after the operator focuses a site, which
    keeps the initial map payload smaller than embedding every station detail
    into the first HTML response.
    """
    start_date = request.args.get("start_date") or None
    end_date = request.args.get("end_date") or None

    try:
        return jsonify(
            get_station_map_site_detail(
                site_id,
                start_date=start_date,
                end_date=end_date,
            )
        )
    except Exception:
        app.logger.exception("failed_to_build_station_map_site_detail", extra={"site_id": site_id})
        return jsonify(
            {
                "site_id": site_id,
                "stations": [],
                "has_online_host": False,
                "has_known_host": False,
            }
        )

@app.route("/health")
def health():
    """Return the minimal liveness response used by container health checks."""
    return {"status": "ok"}


@app.route("/debug/headers", methods=["GET"])
def debug_headers():
    """Return the identity headers forwarded by the upstream authentication proxy."""

    response = jsonify(
        {
            "X-User-Name": _identity_header_value("X-User-Name"),
            "X-User-Email": _identity_header_value("X-User-Email"),
            "X-User-Job-Title": _identity_header_value("X-User-Job-Title"),
            "X-User-Department": _identity_header_value("X-User-Department"),
            "X-User-Location": _identity_header_value("X-User-Location"),
        }
    )
    # Identity attributes must not be stored by browsers or proxies.
    response.headers["Cache-Control"] = "no-store"
    return response


if __name__ == "__main__":
    serve(
        app,
        host=os.getenv("WEBFUSION_HOST", "127.0.0.1"),
        port=int(os.getenv("WEBFUSION_PORT", "8000")),
        threads=int(os.getenv("WEBFUSION_THREADS", "8")),
        channel_timeout=int(os.getenv("WEBFUSION_CHANNEL_TIMEOUT", "300")),
        ident="RF.Fusion Web"
    )
