"""Application entrypoint for the WebFusion web interface.

This module owns only the routes that do not fit cleanly inside one feature
package:

- the landing page shell
- the summary-backed station-map APIs used by that page
- the container health endpoint

All feature-specific pages live in blueprints under ``modules/``.
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

from flask import Flask, request, render_template, jsonify
from waitress import serve
from werkzeug.middleware.proxy_fix import ProxyFix
from modules.spectrum.routes import spectrum_bp
from modules.host.routes import host_bp
from modules.server.routes import server_bp
from modules.task.routes import task_bp
from modules.maintenance.routes import maintenance_bp
from modules.zabbix_configuration.routes import zabbix_configuration_bp
from modules.map.service import (
    get_station_map_points,
    get_station_map_site_detail,
)
from modules.server.usage_metrics import record_page_view


app = Flask(__name__)
# Nginx is the only upstream proxy and supplies the public `/rffusion` prefix.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_prefix=1)
app.logger.setLevel(logging.INFO)

# Register feature blueprints first; the app-level routes below are kept only
# for the landing page and a few small cross-module helpers.

app.register_blueprint(spectrum_bp)
app.register_blueprint(host_bp)
app.register_blueprint(server_bp)
app.register_blueprint(task_bp)
app.register_blueprint(maintenance_bp)
app.register_blueprint(zabbix_configuration_bp)

@app.route("/")
def index():
    """Render the landing page shell.

    The station data is loaded asynchronously by the browser so the page can
    appear quickly while the map API resolves in parallel.
    """
    record_page_view()
    return render_template("index.html")


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
            "X-User-Name": request.headers.get("X-User-Name"),
            "X-User-Email": request.headers.get("X-User-Email"),
            "X-User-Job-Title": request.headers.get("X-User-Job-Title"),
            "X-User-Department": request.headers.get("X-User-Department"),
            "X-User-Location": request.headers.get("X-User-Location"),
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
