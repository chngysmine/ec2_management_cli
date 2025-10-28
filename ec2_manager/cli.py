import json
import sys
from typing import List, Optional

import click

from .core import EC2Manager
from .utils import load_config, setup_logging
from .exceptions import AWSAuthError, OperationError


@click.group()
@click.option("--region", default=None, help="AWS region, e.g., us-east-1")
@click.option("-v", "--verbose", count=True, help="Increase verbosity (use -v)")
@click.pass_context
def main_cli(ctx: click.Context, region: Optional[str], verbose: int):
    """EC2 Management CLI."""
    logger = setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["region"] = region
    ctx.obj["logger"] = logger


# ------------- Instance group -------------
@main_cli.group(name="instance")
@click.pass_context
def instance_group(ctx: click.Context):
    """Manage EC2 instances."""


@instance_group.command(name="list")
@click.option("--tag", "tags", multiple=True, help="Filter by tag in the form Key=Value")
@click.option("--state", "states", multiple=True, help="Filter by instance state name")
@click.pass_context
def instance_list(ctx: click.Context, tags: List[str], states: List[str]):
    mgr = EC2Manager(region_name=ctx.obj.get("region"))
    tag_pairs = [tuple(t.split("=", 1)) for t in tags]
    try:
        data = mgr.list_instances(tags_filter=tag_pairs, states=list(states) or None)
    except AWSAuthError as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    click.echo(json.dumps(data, indent=2))


@instance_group.command(name="create")
@click.argument("config_path", type=click.Path(exists=True, dir_okay=False))
@click.pass_context
def instance_create(ctx: click.Context, config_path: str):
    cfg = load_config(config_path)
    mgr = EC2Manager(region_name=ctx.obj.get("region"))
    try:
        result = mgr.create_instance(cfg)
    except (AWSAuthError, OperationError) as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    click.echo(json.dumps(result, indent=2))


@instance_group.command(name="stop")
@click.argument("instance_id")
@click.pass_context
def instance_stop(ctx: click.Context, instance_id: str):
    mgr = EC2Manager(region_name=ctx.obj.get("region"))
    try:
        result = mgr.stop_instance(instance_id)
    except (AWSAuthError, OperationError) as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    click.echo(json.dumps(result, indent=2))


@instance_group.command(name="start")
@click.argument("instance_id")
@click.pass_context
def instance_start(ctx: click.Context, instance_id: str):
    mgr = EC2Manager(region_name=ctx.obj.get("region"))
    try:
        result = mgr.start_instance(instance_id)
    except (AWSAuthError, OperationError) as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    click.echo(json.dumps(result, indent=2))


@instance_group.command(name="terminate")
@click.argument("instance_id")
@click.pass_context
def instance_terminate(ctx: click.Context, instance_id: str):
    mgr = EC2Manager(region_name=ctx.obj.get("region"))
    try:
        result = mgr.terminate_instance(instance_id)
    except (AWSAuthError, OperationError) as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    click.echo(json.dumps(result, indent=2))


# ------------- Volume group -------------
@main_cli.group(name="volume")
@click.pass_context
def volume_group(ctx: click.Context):
    """Manage EBS volumes."""


@volume_group.command(name="list")
@click.option("--status", default=None, help="Filter by status, e.g., available")
@click.pass_context
def volume_list(ctx: click.Context, status: Optional[str]):
    mgr = EC2Manager(region_name=ctx.obj.get("region"))
    try:
        data = mgr.list_volumes(status_filter=status)
    except (AWSAuthError, OperationError) as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    click.echo(json.dumps(data, indent=2))


@volume_group.command(name="attach")
@click.argument("volume_id")
@click.argument("instance_id")
@click.argument("device_name")
@click.pass_context
def volume_attach(ctx: click.Context, volume_id: str, instance_id: str, device_name: str):
    mgr = EC2Manager(region_name=ctx.obj.get("region"))
    try:
        result = mgr.attach_volume(volume_id, instance_id, device_name)
    except (AWSAuthError, OperationError) as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    click.echo(json.dumps(result, indent=2))


@volume_group.command(name="detach")
@click.argument("volume_id")
@click.option("--force", is_flag=True, default=False, help="Force detach")
@click.pass_context
def volume_detach(ctx: click.Context, volume_id: str, force: bool):
    mgr = EC2Manager(region_name=ctx.obj.get("region"))
    try:
        result = mgr.detach_volume(volume_id, force=force)
    except (AWSAuthError, OperationError) as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    click.echo(json.dumps(result, indent=2))


@volume_group.command(name="set-delete-on-term")
@click.argument("instance_id")
@click.argument("device_name")
@click.option("--enable/--disable", "delete_on_term", default=True)
@click.pass_context
def volume_set_delete_on_term(ctx: click.Context, instance_id: str, device_name: str, delete_on_term: bool):
    mgr = EC2Manager(region_name=ctx.obj.get("region"))
    try:
        mgr.set_delete_on_termination(instance_id, device_name, delete_on_term)
    except (AWSAuthError, OperationError) as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    click.echo(json.dumps({"InstanceId": instance_id, "Device": device_name, "DeleteOnTermination": delete_on_term}, indent=2))


# ------------- Report group -------------
@main_cli.group(name="report")
@click.pass_context
def report_group(ctx: click.Context):
    """Reports and analysis."""


@report_group.command(name="inventory")
@click.option("--output", type=click.Choice(["json", "csv"]), default="json")
@click.option("--regions", multiple=True, help="Limit to specific regions")
@click.pass_context
def report_inventory(ctx: click.Context, output: str, regions: List[str]):
    mgr = EC2Manager(region_name=ctx.obj.get("region"))
    data = mgr.generate_inventory_report(regions=list(regions) or None)
    if output == "json":
        click.echo(json.dumps(data, indent=2))
    else:
        # CSV output to stdout
        import csv

        fieldnames = [
            "AccountId",
            "Region",
            "InstanceId",
            "InstanceType",
            "State",
            "AvailabilityZone",
            "PrivateIpAddress",
            "PublicIpAddress",
            "KeyName",
            "NameTag",
            "LaunchTime",
        ]
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow({k: row.get(k) for k in fieldnames})


@report_group.command(name="cost-optimize")
@click.option("--regions", multiple=True, help="Limit to specific regions")
@click.option("--idle-threshold", type=float, default=5.0, help="Average CPU% threshold for idle instances")
@click.pass_context
def report_cost(ctx: click.Context, regions: List[str], idle_threshold: float):
    mgr = EC2Manager(region_name=ctx.obj.get("region"))
    data = mgr.find_wasteful_resources(regions=list(regions) or None, idle_cpu_threshold=idle_threshold)
    click.echo(json.dumps(data, indent=2))
