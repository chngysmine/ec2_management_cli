from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, status

from modern_backend.api.dependencies import get_ec2_repo
from modern_backend.domain.instance import Instance, UnitEconomicsPoint
from modern_backend.ports.repository import IEC2Repository

router = APIRouter(prefix="/api/v1", tags=["instances"])


@router.get("/instances", response_model=List[Instance])
async def list_instances(repo: IEC2Repository = Depends(get_ec2_repo)) -> List[Instance]:
    return await repo.list_instances()


@router.post(
    "/instances/{instance_id}/start",
    response_model=Instance,
    status_code=status.HTTP_202_ACCEPTED,
)
async def start_instance(
    instance_id: str,
    repo: IEC2Repository = Depends(get_ec2_repo),
) -> Instance:
    try:
        return await repo.start_instance(instance_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post(
    "/instances/{instance_id}/stop",
    response_model=Instance,
    status_code=status.HTTP_202_ACCEPTED,
)
async def stop_instance(
    instance_id: str,
    repo: IEC2Repository = Depends(get_ec2_repo),
) -> Instance:
    try:
        return await repo.stop_instance(instance_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post(
    "/instances/{instance_id}/terminate",
    response_model=Instance,
    status_code=status.HTTP_202_ACCEPTED,
)
async def terminate_instance(
    instance_id: str,
    repo: IEC2Repository = Depends(get_ec2_repo),
) -> Instance:
    try:
        return await repo.terminate_instance(instance_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.get("/economics", response_model=List[UnitEconomicsPoint])
async def get_unit_economics(
    repo: IEC2Repository = Depends(get_ec2_repo),
) -> List[UnitEconomicsPoint]:
    points = await repo.unit_economics()
    return list(points)


@router.get("/instances/throttled", response_model=List[Instance])
async def throttled_refresh(repo: IEC2Repository = Depends(get_ec2_repo)) -> List[Instance]:
    try:
        await repo.enforce_rate_limit()
        return await repo.list_instances()
    except RuntimeError as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc


