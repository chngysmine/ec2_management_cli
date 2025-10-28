from __future__ import annotations

import os
from typing import List, Optional

from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .core import EC2Manager
from .utils import setup_logging
from . import DEFAULT_WEB_PORT


TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)


def get_manager(request: Request) -> EC2Manager:
    region = request.query_params.get("region") or os.getenv("AWS_REGION") or None
    return EC2Manager(region_name=region)


app = FastAPI(title="EC2 Management Web UI")
logger = setup_logging(verbosity=0)


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


# -------- Instances --------
@app.get("/instances", response_class=HTMLResponse)
def instances_page(request: Request, state: Optional[str] = None, mgr: EC2Manager = Depends(get_manager)):
    states = [state] if state else None
    data = mgr.list_instances(tags_filter=None, states=states)
    return templates.TemplateResponse("instances.html", {"request": request, "instances": data})


@app.post("/instances/stop")
def stop_instance(instance_id: str = Form(...), mgr: EC2Manager = Depends(get_manager)):
    mgr.stop_instance(instance_id)
    return RedirectResponse(url="/instances", status_code=303)


@app.post("/instances/start")
def start_instance(instance_id: str = Form(...), mgr: EC2Manager = Depends(get_manager)):
    mgr.start_instance(instance_id)
    return RedirectResponse(url="/instances", status_code=303)


@app.post("/instances/terminate")
def terminate_instance(instance_id: str = Form(...), mgr: EC2Manager = Depends(get_manager)):
    mgr.terminate_instance(instance_id)
    return RedirectResponse(url="/instances", status_code=303)


# -------- Volumes --------
@app.get("/volumes", response_class=HTMLResponse)
def volumes_page(request: Request, status: Optional[str] = None, mgr: EC2Manager = Depends(get_manager)):
    vols = mgr.list_volumes(status_filter=status)
    return templates.TemplateResponse("volumes.html", {"request": request, "volumes": vols})


@app.post("/volumes/set-delete-on-term")
def set_delete_on_term(
    instance_id: str = Form(...),
    device_name: str = Form(...),
    enable: bool = Form(True),
    mgr: EC2Manager = Depends(get_manager),
):
    mgr.set_delete_on_termination(instance_id, device_name, enable)
    return RedirectResponse(url="/volumes", status_code=303)


# -------- Reports --------
@app.get("/reports/inventory", response_class=HTMLResponse)
def report_inventory(request: Request, regions: Optional[str] = None, mgr: EC2Manager = Depends(get_manager)):
    region_list: Optional[List[str]] = [r.strip() for r in regions.split(",")] if regions else None
    data = mgr.generate_inventory_report(regions=region_list)
    return templates.TemplateResponse("inventory.html", {"request": request, "rows": data})


@app.get("/reports/cost-optimize", response_class=HTMLResponse)
def report_cost(request: Request, regions: Optional[str] = None, threshold: float = 5.0, mgr: EC2Manager = Depends(get_manager)):
    region_list: Optional[List[str]] = [r.strip() for r in regions.split(",")] if regions else None
    data = mgr.find_wasteful_resources(regions=region_list, idle_cpu_threshold=threshold)
    return templates.TemplateResponse("cost_optimize.html", {"request": request, "report": data, "threshold": threshold})


def run():
    import uvicorn

    port = int(os.getenv("PORT", DEFAULT_WEB_PORT))
    uvicorn.run(app, host="0.0.0.0", port=port)





