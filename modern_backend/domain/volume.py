from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class Volume(BaseModel):
    volume_id: str = Field(..., alias="VolumeId")
    size_gib: int = Field(..., alias="SizeGiB")
    state: str = Field(..., alias="State")
    volume_type: Optional[str] = Field(None, alias="VolumeType")
    availability_zone: Optional[str] = Field(None, alias="AvailabilityZone")
    throughput: Optional[int] = Field(None, alias="Throughput")
    iops: Optional[int] = Field(None, alias="Iops")
    attached_instances: List[str] = Field(default_factory=list, alias="AttachedInstances")

    model_config = {
        "populate_by_name": True,
    }


