"""Assemble the WebFusion Flask application.

This module owns only cross-feature routes:

- the landing page shell
- the summary-backed station-map APIs used by that page
- small cross-module helpers

Feature pages and their JSON APIs live in blueprints under ``modules/``. The
application does not implement queue, catalog, or Zabbix rules directly; those
rules stay in their feature service modules.
"""

import os
import logging
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
from modules.task.routes import task_api_bp, task_bp
from modules.maintenance.routes import maintenance_api_bp, maintenance_bp
from modules.users.routes import users_admin_api_bp, users_api_bp, users_bp
from modules.zabbix_configuration.routes import (
    zabbix_configuration_api_bp,
    zabbix_configuration_bp,
)
from modules.alarms.routes import alarms_bp
from modules.map.service import (
    get_station_map_points,
    get_station_map_site_detail,
)
from modules.server.usage_metrics import record_page_view
from auth.service import AuthService


app = Flask(__name__)
# Nginx is the only upstream proxy and supplies the public `/rffusion` prefix.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_prefix=1)
app.logger.setLevel(logging.INFO)

AUTH_SERVICE = AuthService()

# Register feature blueprints first; the app-level routes below are kept only
# for the landing page and a few small cross-module helpers.

app.register_blueprint(spectrum_bp)
app.register_blueprint(host_bp)
app.register_blueprint(server_bp)
app.register_blueprint(task_bp)
app.register_blueprint(task_api_bp)
app.register_blueprint(maintenance_bp)
app.register_blueprint(maintenance_api_bp)
app.register_blueprint(users_bp)
app.register_blueprint(users_api_bp)
app.register_blueprint(users_admin_api_bp)
app.register_blueprint(zabbix_configuration_bp)
app.register_blueprint(zabbix_configuration_api_bp)
app.register_blueprint(alarms_bp)


@app.before_request
def load_request_identity() -> None:
    """Make the proxy identity available to route guards and base templates.

    Args:
        None. Flask supplies the current request context.

    Returns:
        None. Stores a normalized identity in `flask.g.webfusion_user`; for a
        restricted blueprint, also loads the active access role.
    """
    identity = AUTH_SERVICE.request_identity(request)
    endpoint = request.endpoint or ""
    blueprint_name = endpoint.partition(".")[0]
    if AUTH_SERVICE.is_restricted_blueprint(blueprint_name):
        AUTH_SERVICE.load_access_role(identity, app.logger)
    g.webfusion_user = identity


@app.before_request
def require_restricted_access() -> Response | None:
    """Allow protected WebFusion modules only to configured active users.

    Args:
        None. Flask supplies the current request and `flask.g.webfusion_user`.

    Returns:
        Access decision. Type: flask.Response | None. Returns a 403 response
        for protected modules without an active role; otherwise returns `None`
        so Flask continues the request.
    """
    endpoint = request.endpoint or ""
    blueprint_name = endpoint.partition(".")[0]
    if not AUTH_SERVICE.is_restricted_blueprint(blueprint_name):
        return None

    if g.webfusion_user["role"] is None:
        return Response("Acesso restrito.", 403)
    return None


@app.context_processor
def inject_request_identity() -> dict[str, dict[str, str | None]]:
    """Expose the current proxy identity to every rendered base template.

    Args:
        None. Flask supplies the current request context.

    Returns:
        Template context. Type: dict[str, dict[str, str | None]]. Contains the
        required key `current_user`, whose value has the identity keys `name`,
        `email`, `job_title`, `department`, `location`, `avatar_url`, `label`,
        and `role`.
    """
    identity = getattr(g, "webfusion_user", AUTH_SERVICE.request_identity(request))
    AUTH_SERVICE.record_observed_user(identity, app.logger)
    AUTH_SERVICE.load_access_role(identity, app.logger)
    return {"current_user": identity}

@app.route("/")
def index() -> str:
    """Render the landing page shell.

    The first map snapshot is embedded in the page. This avoids a second
    browser request that can remain pending after a history-cache restore.

    Args:
        None. Flask supplies the current request context.

    Returns:
        Rendered landing page HTML. Type: str. The template receives
        `initial_map_points` as list[dict[str, object]], or an empty list when
        the summary query fails.
    """
    record_page_view()
    try:
        initial_map_points = get_station_map_points()
    except Exception:
        app.logger.exception("failed_to_build_initial_station_map")
        initial_map_points = []

    return render_template("index.html", initial_map_points=initial_map_points)


@app.route("/api/map/stations")
def map_stations() -> Response:
    """Return the cached summary-backed station map payload.

    The page intentionally renders before this endpoint resolves, so failures
    should degrade to an empty map instead of breaking the whole landing page.

    Args:
        None. Flask supplies optional `start_date` and `end_date` query
        parameters as strings.

    Returns:
        JSON response. Type: flask.Response. Its object contains the required
        key `points` with value list[dict[str, object]], or an empty list when
        the summary query fails.
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
def map_station_detail(site_id: int) -> Response:
    """Return popup metadata for one map point.

    The popup is loaded on demand after the operator focuses a site, which
    keeps the initial map payload smaller than embedding every station detail
    into the first HTML response.

    Args:
        site_id: Identifier of the selected map site. Type: int.

    Returns:
        JSON response. Type: flask.Response. The object contains `site_id`,
        `stations`, `has_online_host`, and `has_known_host`; `stations` is a
        list[dict[str, object]] and is empty when the detail query fails.
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

if __name__ == "__main__":
    serve(
        app,
        host=os.getenv("WEBFUSION_HOST", "127.0.0.1"),
        port=int(os.getenv("WEBFUSION_PORT", "8000")),
        threads=int(os.getenv("WEBFUSION_THREADS", "8")),
        channel_timeout=int(os.getenv("WEBFUSION_CHANNEL_TIMEOUT", "300")),
        ident="RF.Fusion Web"
    )
