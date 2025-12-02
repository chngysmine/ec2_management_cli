from __future__ import annotations

from datetime import datetime
from typing import List

from pydantic import BaseModel

from .instance import Instance, UnitEconomicsPoint
from .volume import Volume
from .event import EventEntry


class DashboardStats(BaseModel):
    total_instances: int
    running_instances: int
    stopped_instances: int
    total_volumes: int
    available_volumes: int
    quota_used_gib: int
    quota_total_gib: int


class OverviewPayload(BaseModel):
    mode: str
    region: str
    generated_at: datetime
    stats: DashboardStats
    instances: List[Instance]
    volumes: List[Volume]
    events: List[EventEntry]
    metrics: List[UnitEconomicsPoint]


