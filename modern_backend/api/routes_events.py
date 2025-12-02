from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from modern_backend.api.dependencies import get_ec2_repo
from modern_backend.domain.event import EventEntry
from modern_backend.ports.repository import IEC2Repository

router = APIRouter(prefix="/api/v1/events", tags=["events"])


@router.get("", response_model=list[EventEntry])
async def list_events(
    limit: int = Query(50, ge=1, le=200),
    repo: IEC2Repository = Depends(get_ec2_repo),
) -> list[EventEntry]:
    return await repo.list_events(limit=limit)


