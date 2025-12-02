from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class EventLevel(str, Enum):
    info = "info"
    warning = "warning"
    error = "error"


class EventEntry(BaseModel):
    id: str
    timestamp: datetime
    level: EventLevel = Field(default=EventLevel.info)
    message: str
    context: dict | None = None

    model_config = {
        "populate_by_name": True,
    }


