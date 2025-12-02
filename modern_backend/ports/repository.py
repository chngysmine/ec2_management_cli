from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, List, Optional, Protocol

from modern_backend.domain.instance import Instance, UnitEconomicsPoint
from modern_backend.domain.volume import Volume
from modern_backend.domain.event import EventEntry


class IEC2Repository(Protocol):
    """Port/interface cho các adapter EC2 (AWS thật hoặc Mock)."""

    async def list_instances(self) -> List[Instance]:
        ...

    async def start_instance(self, instance_id: str) -> Instance:
        ...

    async def stop_instance(self, instance_id: str) -> Instance:
        ...

    async def terminate_instance(self, instance_id: str) -> Instance:
        ...

    async def unit_economics(self) -> Iterable[UnitEconomicsPoint]:
        ...

    # Volumes
    async def list_volumes(self) -> List[Volume]:
        ...

    async def create_volume(self, size_gib: int, az: Optional[str] = None) -> Volume:
        ...

    async def attach_volume(self, volume_id: str, instance_id: str, device_name: str) -> dict:
        ...

    async def detach_volume(self, volume_id: str, force: bool = False) -> dict:
        ...

    async def enforce_rate_limit(self) -> None:
        ...

    async def list_events(self, limit: int = 50) -> List[EventEntry]:
        ...


class AbstractEC2Repository(ABC):
    """Base class tiện cho adapter cụ thể kế thừa nếu muốn."""

    @abstractmethod
    async def list_instances(self) -> List[Instance]:
        raise NotImplementedError

    @abstractmethod
    async def start_instance(self, instance_id: str) -> Instance:
        raise NotImplementedError

    @abstractmethod
    async def stop_instance(self, instance_id: str) -> Instance:
        raise NotImplementedError

    @abstractmethod
    async def terminate_instance(self, instance_id: str) -> Instance:
        raise NotImplementedError

    @abstractmethod
    async def unit_economics(self) -> Iterable[UnitEconomicsPoint]:
        raise NotImplementedError

    @abstractmethod
    async def list_volumes(self) -> List[Volume]:
        raise NotImplementedError

    @abstractmethod
    async def create_volume(self, size_gib: int, az: Optional[str] = None) -> Volume:
        raise NotImplementedError

    @abstractmethod
    async def attach_volume(self, volume_id: str, instance_id: str, device_name: str) -> dict:
        raise NotImplementedError

    @abstractmethod
    async def detach_volume(self, volume_id: str, force: bool = False) -> dict:
        raise NotImplementedError

    @abstractmethod
    async def enforce_rate_limit(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def list_events(self, limit: int = 50) -> List[EventEntry]:
        raise NotImplementedError


