from __future__ import annotations

from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException, status

from modern_backend.api.dependencies import get_ec2_repo
from modern_backend.domain.volume import Volume
from modern_backend.ports.repository import IEC2Repository


class VolumeCreateRequest(BaseModel):
    size_gib: int = Field(..., ge=1, le=16384)
    availability_zone: str | None = None


class VolumeAttachRequest(BaseModel):
    volume_id: str
    instance_id: str
    device_name: str = "/dev/sdf"


class VolumeDetachRequest(BaseModel):
    volume_id: str
    force: bool = False


router = APIRouter(prefix="/api/v1/volumes", tags=["volumes"])


@router.get("", response_model=list[Volume])
async def list_volumes(repo: IEC2Repository = Depends(get_ec2_repo)) -> list[Volume]:
    return await repo.list_volumes()


@router.post("", response_model=Volume, status_code=status.HTTP_201_CREATED)
async def create_volume(
    payload: VolumeCreateRequest,
    repo: IEC2Repository = Depends(get_ec2_repo),
) -> Volume:
    try:
        return await repo.create_volume(payload.size_gib, payload.availability_zone)
    except RuntimeError as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc


@router.post("/attach", status_code=status.HTTP_202_ACCEPTED)
async def attach_volume(
    payload: VolumeAttachRequest,
    repo: IEC2Repository = Depends(get_ec2_repo),
) -> dict:
    try:
        return await repo.attach_volume(payload.volume_id, payload.instance_id, payload.device_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/detach", status_code=status.HTTP_202_ACCEPTED)
async def detach_volume(
    payload: VolumeDetachRequest,
    repo: IEC2Repository = Depends(get_ec2_repo),
) -> dict:
    try:
        return await repo.detach_volume(payload.volume_id, payload.force)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

