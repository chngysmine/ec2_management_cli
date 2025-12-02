from __future__ import annotations

import asyncio
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional

from modern_backend.domain.instance import Instance, InstanceState, UnitEconomicsPoint
from modern_backend.domain.volume import Volume
from modern_backend.domain.event import EventEntry, EventLevel
from modern_backend.ports.repository import AbstractEC2Repository


class _MockStateStore:
    """Stateful in-memory store mô phỏng EC2."""

    def __init__(self) -> None:
        now = datetime.now(timezone.utc)
        self.instances: Dict[str, Instance] = {
            "i-mock-001": Instance(
                AccountId="123456789012",
                InstanceId="i-mock-001",
                InstanceType="t3.micro",
                State=InstanceState.running,
                AvailabilityZone="us-east-1a",
                PrivateIpAddress="10.0.0.10",
                PublicIpAddress="54.1.1.10",
                KeyName="mock-key",
                NameTag="Mock-Instance-1",
                LaunchTime=now - timedelta(hours=12),
            ),
            "i-mock-002": Instance(
                AccountId="123456789012",
                InstanceId="i-mock-002",
                InstanceType="t3.medium",
                State=InstanceState.stopped,
                AvailabilityZone="us-east-1b",
                PrivateIpAddress="10.0.0.11",
                PublicIpAddress=None,
                KeyName="mock-key",
                NameTag="Mock-Instance-2",
                LaunchTime=now - timedelta(days=2),
            ),
        }
        # Volume mock đơn giản
        self.volumes: Dict[str, Dict[str, object]] = {
            "vol-mock-001": {
                "VolumeId": "vol-mock-001",
                "SizeGiB": 10,
                "State": "available",
                "AvailabilityZone": "us-east-1a",
            },
            "vol-mock-002": {
                "VolumeId": "vol-mock-002",
                "SizeGiB": 500,
                "State": "in-use",
                "AvailabilityZone": "us-east-1a",
                "InstanceId": "i-mock-001",
            },
        }
        self.total_quota_gib: int = 1024  # 1TB quota giả lập
        self.region: str = "us-east-1"
        self.events: List[EventEntry] = []
        self.instance_metrics: Dict[str, float] = {
            "i-mock-001": 40.0,
            "i-mock-002": 10.0,
        }
        self.metrics: List[UnitEconomicsPoint] = []
        self._record_event(EventLevel.info, "Mock environment initialized", {"region": self.region})

    def snapshot_instances(self) -> List[Instance]:
        return list(self.instances.values())

    def used_volume_gib(self) -> int:
        return sum(int(v.get("SizeGiB", 0)) for v in self.volumes.values())

    def _record_event(self, level: EventLevel, message: str, context: Optional[dict] = None) -> None:
        entry = EventEntry(
            id=str(uuid.uuid4()),
            timestamp=datetime.now(timezone.utc),
            level=level,
            message=message,
            context=context or {},
        )
        self.events.append(entry)
        if len(self.events) > 200:
            self.events = self.events[-200:]


_STORE = _MockStateStore()


class MockEC2Adapter(AbstractEC2Repository):
    """Adapter mô phỏng EC2 với state machine + latency."""

    async def list_instances(self) -> List[Instance]:
        # Mô phỏng độ trễ nhẹ
        await asyncio.sleep(0.05)
        return _STORE.snapshot_instances()

    async def start_instance(self, instance_id: str) -> Instance:
        inst = _STORE.instances.get(instance_id)
        if not inst:
            raise ValueError(f"Instance {instance_id} không tồn tại (mock)")

        if inst.state in (InstanceState.running, InstanceState.pending):
            return inst

        inst.state = InstanceState.pending
        _STORE._record_event(EventLevel.info, "Start command received", {"instance_id": instance_id})

        async def _transition() -> None:
            await asyncio.sleep(random.uniform(3.0, 6.0))
            # 1% mô phỏng lỗi IMPAIRED
            if random.random() < 0.01:
                inst.state = InstanceState.impaired
                _STORE._record_event(
                    EventLevel.error,
                    "Instance entered impaired state",
                    {"instance_id": instance_id},
                )
            else:
                inst.state = InstanceState.running
                _STORE._record_event(
                    EventLevel.info,
                    "Instance is now running",
                    {"instance_id": instance_id},
                )

        asyncio.create_task(_transition())
        return inst

    async def stop_instance(self, instance_id: str) -> Instance:
        inst = _STORE.instances.get(instance_id)
        if not inst:
            raise ValueError(f"Instance {instance_id} không tồn tại (mock)")

        if inst.state in (InstanceState.stopped, InstanceState.stopping):
            return inst

        inst.state = InstanceState.stopping
        _STORE._record_event(EventLevel.warning, "Stop command received", {"instance_id": instance_id})

        async def _transition() -> None:
            await asyncio.sleep(random.uniform(2.0, 4.0))
            inst.state = InstanceState.stopped
            _STORE._record_event(
                EventLevel.info,
                "Instance stopped",
                {"instance_id": instance_id},
            )

        asyncio.create_task(_transition())
        return inst

    async def terminate_instance(self, instance_id: str) -> Instance:
        inst = _STORE.instances.get(instance_id)
        if not inst:
            raise ValueError(f"Instance {instance_id} không tồn tại (mock)")

        inst.state = InstanceState.shutting_down
        _STORE._record_event(EventLevel.warning, "Terminate initiated", {"instance_id": instance_id})

        async def _transition() -> None:
            await asyncio.sleep(random.uniform(1.0, 2.0))
            inst.state = InstanceState.terminated
            _STORE._record_event(
                EventLevel.info,
                "Instance terminated",
                {"instance_id": instance_id},
            )

        asyncio.create_task(_transition())
        return inst

    async def unit_economics(self) -> Iterable[UnitEconomicsPoint]:
        """Trả về chuỗi điểm cost/CPU theo thời gian, phụ thuộc trạng thái hiện tại."""
        now = datetime.now(timezone.utc)
        running = sum(1 for inst in _STORE.instances.values() if inst.state == InstanceState.running)
        used_gib = _STORE.used_volume_gib()

        # Nếu chưa có dữ liệu, khởi tạo 24 điểm trong quá khứ với trạng thái hiện tại
        if not _STORE.metrics:
            cost = 0.0
            for i in range(24):
                ts = now - timedelta(hours=24 - i)
                cpu = min(100.0, running * 20.0)
                delta = running * 0.15 + used_gib * 0.005
                cost += delta
                _STORE.metrics.append(
                    UnitEconomicsPoint(
                        timestamp=ts,
                        cost_per_hour=round(cost, 2),
                        cpu_utilization=round(cpu, 1),
                    )
                )
        else:
            last = _STORE.metrics[-1]
            delta = running * 0.15 + used_gib * 0.005
            cost = max(0.0, last.cost_per_hour + delta)
            cpu = min(100.0, running * 20.0)
            _STORE.metrics.append(
                UnitEconomicsPoint(
                    timestamp=now,
                    cost_per_hour=round(cost, 2),
                    cpu_utilization=round(cpu, 1),
                )
            )
            if len(_STORE.metrics) > 48:
                _STORE.metrics = _STORE.metrics[-48:]

        await asyncio.sleep(0.01)
        return list(_STORE.metrics)

    # --------- Extensions cho volumes / quota / throttling (mock) ---------

    async def list_volumes(self) -> List[Volume]:
        """Trả về danh sách volumes mock."""
        await asyncio.sleep(0.05)
        normalized = []
        for v in _STORE.volumes.values():
            # Đảm bảo trường AttachedInstances luôn đồng bộ với InstanceId nếu có
            attached = v.get("AttachedInstances")
            instance_id = v.get("InstanceId")
            if attached is None:
                if instance_id:
                    v = {**v, "AttachedInstances": [instance_id]}
                else:
                    v = {**v, "AttachedInstances": []}
            normalized.append(Volume.parse_obj(v))
        return normalized

    async def create_volume(self, size_gib: int, az: Optional[str] = None) -> Volume:
        """Tạo volume với quota check đơn giản."""
        # Mô phỏng quota exceeded
        if _STORE.used_volume_gib() + size_gib > _STORE.total_quota_gib:
            raise RuntimeError("VolumeLimitExceeded: total allocated GiB exceeds quota")

        vol_id = f"vol-mock-{len(_STORE.volumes) + 1:03d}"
        vol = {
            "VolumeId": vol_id,
            "SizeGiB": size_gib,
            "State": "available",
            "AvailabilityZone": az or "us-east-1a",
        }
        _STORE.volumes[vol_id] = vol
        await asyncio.sleep(0.1)
        _STORE._record_event(EventLevel.info, "Created mock volume", {"volume_id": vol_id, "size": size_gib})
        return Volume.parse_obj(vol)

    async def attach_volume(self, volume_id: str, instance_id: str, device_name: str) -> dict:
        vol = _STORE.volumes.get(volume_id)
        if not vol:
            raise ValueError("Volume không tồn tại (mock)")
        vol["State"] = "in-use"
        vol["InstanceId"] = instance_id
        vol.setdefault("AttachedInstances", [])
        vol["AttachedInstances"].append(instance_id)
        await asyncio.sleep(0.1)
        _STORE._record_event(
            EventLevel.info,
            "Volume attached",
            {"volume_id": volume_id, "instance_id": instance_id, "device": device_name},
        )
        return {"VolumeId": volume_id, "InstanceId": instance_id, "Device": device_name, "State": "attached"}

    async def detach_volume(self, volume_id: str, force: bool = False) -> dict:
        vol = _STORE.volumes.get(volume_id)
        if not vol:
            raise ValueError("Volume không tồn tại (mock)")
        vol["State"] = "available"
        vol.pop("InstanceId", None)
        vol["AttachedInstances"] = []
        await asyncio.sleep(0.1)
        _STORE._record_event(
            EventLevel.warning,
            "Volume detached",
            {"volume_id": volume_id, "force": force},
        )
        return {"VolumeId": volume_id, "State": "detaching"}

    async def enforce_rate_limit(self) -> None:
        """Ngẫu nhiên ném RequestLimitExceeded."""
        if random.random() < 0.05:
            raise RuntimeError("RequestLimitExceeded: please slow down")

    async def list_events(self, limit: int = 50) -> List[EventEntry]:
        await asyncio.sleep(0.01)
        return list(_STORE.events[-limit:])



