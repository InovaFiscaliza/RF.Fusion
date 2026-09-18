"""Assemble the WebFusion Flask application.

This module owns only cross-feature routes:

- the landing page shell
- small cross-module helpers

Feature pages and JSON APIs live in blueprints under ``modules/`` and ``api/``. The
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

from flask import Flask, Response, g, request, render_template
from waitress import serve
from werkzeug.middleware.proxy_fix import ProxyFix
from modules.spectrum.routes import spectrum_bp
from modules.host.routes import host_bp
from modules.server.routes import server_bp
from modules.tasks.routes import tasks_api_bp, tasks_bp
from modules.users.routes import users_admin_api_bp, users_api_bp, users_bp
from modules.configuration.routes import (
    configuration_api_bp,
    configuration_bp,
)
from modules.alarms.routes import alarms_bp
from api.map_api.service import get_station_map_dataset
from api.map_api.routes import map_api_bp
from modules.server.usage_metrics import record_page_view
from auth.service import AuthService
from api.appAnalise_api import appanalise_api_bp


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
app.register_blueprint(tasks_bp)
app.register_blueprint(tasks_api_bp)
app.register_blueprint(users_bp)
app.register_blueprint(users_api_bp)
app.register_blueprint(users_admin_api_bp)
app.register_blueprint(configuration_bp)
app.register_blueprint(configuration_api_bp)
app.register_blueprint(alarms_bp)
app.register_blueprint(appanalise_api_bp)
app.register_blueprint(map_api_bp)


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
    AUTH_SERVICE.load_profile_image(identity, app.logger)
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
        initial_map_points = get_station_map_dataset()["points"]
    except Exception:
        app.logger.exception("failed_to_build_initial_station_map")
        initial_map_points = []

    return render_template("index.html", initial_map_points=initial_map_points)


if __name__ == "__main__":
    serve(
        app,
        host=os.getenv("WEBFUSION_HOST", "127.0.0.1"),
        port=int(os.getenv("WEBFUSION_PORT", "8000")),
        threads=int(os.getenv("WEBFUSION_THREADS", "8")),
        channel_timeout=int(os.getenv("WEBFUSION_CHANNEL_TIMEOUT", "300")),
        ident="RF.Fusion Web"
    )
