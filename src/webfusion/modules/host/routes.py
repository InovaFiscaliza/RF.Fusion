from flask import Blueprint, render_template, request
from modules.host.service import get_all_hosts, get_host_statistics

host_bp = Blueprint("host", __name__)

@host_bp.route("/host", methods=["GET"])
def host():

    host_id = request.args.get("host_id")
    search = request.args.get("search") or None
    online_only = request.args.get("online_only", "1") == "1"

    hosts = get_all_hosts(online_only=online_only, search=search)
    stats = None

    if host_id:
        stats = get_host_statistics(host_id)

    return render_template(
        "host/host.html",
        hosts=hosts,
        stats=stats,
        selected_host=host_id,
        online_only=online_only,
        search=search,
    )
