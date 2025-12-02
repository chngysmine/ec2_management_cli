from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class InstanceState(str, Enum):
    pending = "pending"
    running = "running"
    stopping = "stopping"
    stopped = "stopped"
    shutting_down = "shutting-down"
    terminated = "terminated"
    impaired = "impaired"


class Instance(BaseModel):
    account_id: str = Field(..., alias="AccountId")
    instance_id: str = Field(..., alias="InstanceId")
    instance_type: str = Field(..., alias="InstanceType")
    state: InstanceState = Field(..., alias="State")
    availability_zone: Optional[str] = Field(None, alias="AvailabilityZone")
    private_ip: Optional[str] = Field(None, alias="PrivateIpAddress")
    public_ip: Optional[str] = Field(None, alias="PublicIpAddress")
    key_name: Optional[str] = Field(None, alias="KeyName")
    name_tag: Optional[str] = Field(None, alias="NameTag")
    launch_time: Optional[datetime] = Field(None, alias="LaunchTime")

    class Config:
        allow_population_by_field_name = True
        anystr_strip_whitespace = True


class UnitEconomicsPoint(BaseModel):
    timestamp: datetime
    cost_per_hour: float
    cpu_utilization: float


