from __future__ import annotations

import os
from typing import List, Optional

from flask import Flask, redirect, render_template, request, url_for, Response, jsonify

from .core import EC2Manager
from .utils import setup_logging
from . import DEFAULT_WEB_PORT
from .exceptions import AWSAuthError, OperationError


def get_manager() -> EC2Manager:
    region = (
        request.args.get("region")
        or os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or None
    )
    # Allow overriding profile via querystring ?profile=ec2
    profile = request.args.get("profile")
    if profile:
        os.environ["AWS_PROFILE"] = profile
    if not region:
        raise OperationError(
            "You must specify a region. Set AWS_REGION/AWS_DEFAULT_REGION or add ?region=us-east-1 to the URL."
        )
    return EC2Manager(region_name=region)


auth_user = os.getenv("EC2_MAN_WEB_USER")
auth_pass = os.getenv("EC2_MAN_WEB_PASS")


app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "templates"))
logger = setup_logging(verbosity=0)


def _require_auth() -> Optional[Response]:
    if not auth_user and not auth_pass:
        return None
    auth = request.authorization
    if not auth or auth.username != auth_user or auth.password != auth_pass:
        return Response("Unauthorized", 401, {"WWW-Authenticate": "Basic realm=EC2-Man"})
    return None


@app.before_request
def before_request():
    unauth = _require_auth()
    if unauth is not None:
        return unauth


@app.errorhandler(AWSAuthError)
def handle_auth_error(e: AWSAuthError):
    logger.error("AWSAuthError", extra={"error": str(e)})
    # If API request, return JSON error
    wants_json = request.path.startswith("/api/") or "application/json" in (request.headers.get("Accept") or "")
    if wants_json:
        return jsonify({"error": str(e), "type": "AWSAuthError"}), 401
    return render_template("error.html", message=str(e)), 401


@app.errorhandler(OperationError)
def handle_operation_error(e: OperationError):
    logger.error("OperationError", extra={"error": str(e)})
    wants_json = request.path.startswith("/api/") or "application/json" in (request.headers.get("Accept") or "")
    if wants_json:
        return jsonify({"error": str(e), "type": "OperationError"}), 400
    return render_template("error.html", message=str(e)), 400


@app.errorhandler(Exception)
def handle_general_error(e: Exception):
    logger.error("UnhandledError", extra={"error": str(e)})
    wants_json = request.path.startswith("/api/") or "application/json" in (request.headers.get("Accept") or "")
    if wants_json:
        return jsonify({"error": str(e), "type": "UnhandledError"}), 500
    return render_template("error.html", message="Internal error: " + str(e)), 500


@app.get("/")
def home():
    return render_template("home.html")


@app.get("/favicon.ico")
def favicon():
    return ("", 204)


@app.get("/instances")
def instances_page():
    mgr = get_manager()
    state = request.args.get("state")
    states = [state] if state else None
    data = mgr.list_instances(tags_filter=None, states=states)
    return render_template("instances.html", instances=data)


@app.post("/instances/stop")
def stop_instance():
    mgr = get_manager()
    instance_id = request.form["instance_id"]
    mgr.stop_instance(instance_id)
    return redirect(url_for("instances_page"))


@app.post("/instances/start")
def start_instance():
    mgr = get_manager()
    instance_id = request.form["instance_id"]
    mgr.start_instance(instance_id)
    return redirect(url_for("instances_page"))


@app.post("/instances/terminate")
def terminate_instance():
    mgr = get_manager()
    instance_id = request.form["instance_id"]
    mgr.terminate_instance(instance_id)
    return redirect(url_for("instances_page"))


@app.get("/volumes")
def volumes_page():
    mgr = get_manager()
    status = request.args.get("status")
    vols = mgr.list_volumes(status_filter=status)
    return render_template("volumes.html", volumes=vols)


@app.post("/volumes/set-delete-on-term")
def set_delete_on_term():
    mgr = get_manager()
    instance_id = request.form["instance_id"]
    device_name = request.form["device_name"]
    enable = request.form.get("enable", "true").lower() in {"1", "true", "yes", "on"}
    mgr.set_delete_on_termination(instance_id, device_name, enable)
    return redirect(url_for("volumes_page"))


@app.get("/reports/inventory")
def report_inventory():
    mgr = get_manager()
    regions = request.args.get("regions")
    region_list: Optional[List[str]] = [r.strip() for r in regions.split(",")] if regions else None
    data = mgr.generate_inventory_report(regions=region_list)
    return render_template("inventory.html", rows=data)


@app.get("/reports/cost-optimize")
def report_cost():
    mgr = get_manager()
    regions = request.args.get("regions")
    threshold = float(request.args.get("threshold", 5.0))
    region_list: Optional[List[str]] = [r.strip() for r in regions.split(",")] if regions else None
    data = mgr.find_wasteful_resources(regions=region_list, idle_cpu_threshold=threshold)
    return render_template("cost_optimize.html", report=data, threshold=threshold)


def run():
    port = int(os.getenv("PORT", DEFAULT_WEB_PORT))
    app.run(host="0.0.0.0", port=port)


# -------- JSON API for Terminal UI --------
@app.get("/api/instances")
def api_instances():
    mgr = get_manager()
    state = request.args.get("state")
    states = [state] if state else None
    data = mgr.list_instances(tags_filter=None, states=states)
    return jsonify(data)


@app.post("/api/instances/start")
def api_start_instance():
    mgr = get_manager()
    instance_id = request.json.get("instance_id") if request.is_json else request.form.get("instance_id")
    if not instance_id:
        return jsonify({"error": "instance_id is required"}), 400
    result = mgr.start_instance(instance_id)
    return jsonify(result)


@app.post("/api/instances/stop")
def api_stop_instance():
    mgr = get_manager()
    instance_id = request.json.get("instance_id") if request.is_json else request.form.get("instance_id")
    if not instance_id:
        return jsonify({"error": "instance_id is required"}), 400
    result = mgr.stop_instance(instance_id)
    return jsonify(result)


@app.post("/api/instances/terminate")
def api_terminate_instance():
    mgr = get_manager()
    instance_id = request.json.get("instance_id") if request.is_json else request.form.get("instance_id")
    if not instance_id:
        return jsonify({"error": "instance_id is required"}), 400
    result = mgr.terminate_instance(instance_id)
    return jsonify(result)


@app.get("/api/reports/inventory")
def api_report_inventory():
    mgr = get_manager()
    regions = request.args.get("regions")
    region_list: Optional[List[str]] = [r.strip() for r in regions.split(",")] if regions else None
    data = mgr.generate_inventory_report(regions=region_list)
    return jsonify(data)


@app.get("/api/reports/cost-optimize")
def api_report_cost():
    mgr = get_manager()
    regions = request.args.get("regions")
    threshold = float(request.args.get("threshold", 5.0))
    region_list: Optional[List[str]] = [r.strip() for r in regions.split(",")] if regions else None
    data = mgr.find_wasteful_resources(regions=region_list, idle_cpu_threshold=threshold)
    return jsonify(data)


if __name__ == "__main__":
    run()

