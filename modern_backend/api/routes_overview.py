from __future__ import annotations

from datetime import datetime, timezone
from fastapi import APIRouter, Depends

from modern_backend.api.dependencies import get_ec2_repo
import os

from modern_backend.domain.overview import DashboardStats, OverviewPayload
from modern_backend.domain.instance import InstanceState
from modern_backend.adapters.mock_ec2 import MockEC2Adapter
from modern_backend.ports.repository import IEC2Repository

router = APIRouter(prefix="/api/v1/overview", tags=["overview"])


@router.get("", response_model=OverviewPayload, response_model_by_alias=False)
async def get_overview(repo: IEC2Repository = Depends(get_ec2_repo)) -> OverviewPayload:
    instances = await repo.list_instances()
    volumes = await repo.list_volumes()
    events = await repo.list_events(limit=80)
    metrics = list(await repo.unit_economics())

    running = sum(1 for inst in instances if inst.state == InstanceState.running)
    stopped = sum(1 for inst in instances if inst.state == InstanceState.stopped)
    total_gib = getattr(getattr(repo, "_STORE", None), "total_quota_gib", 1024)
    used_gib = sum(vol.size_gib for vol in volumes)

    stats = DashboardStats(
        total_instances=len(instances),
        running_instances=running,
        stopped_instances=stopped,
        total_volumes=len(volumes),
        available_volumes=sum(1 for vol in volumes if vol.state == "available"),
        quota_used_gib=used_gib,
        quota_total_gib=total_gib,
    )

    mode = "mock" if isinstance(repo, MockEC2Adapter) or getattr(repo, "use_localstack", False) else "live"
    region = getattr(getattr(repo, "_STORE", None), "region", os.getenv("AWS_REGION", "us-east-1"))

    return OverviewPayload(
        mode=mode,
        region=region,
        generated_at=datetime.now(timezone.utc),
        stats=stats,
        instances=instances,
        volumes=volumes,
        events=events,
        metrics=metrics,
    )


