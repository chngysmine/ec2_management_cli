from __future__ import annotations

from typing import Iterable, List, Optional

import anyio
import boto3
from boto3.session import Session as BotoSession

from modern_backend.domain.instance import Instance, InstanceState, UnitEconomicsPoint
from modern_backend.domain.volume import Volume
from modern_backend.domain.event import EventEntry
from modern_backend.ports.repository import AbstractEC2Repository
from ec2_manager.core import EC2Manager  # Tận dụng lớp hiện có cho tới khi tách hẳn


class AwsEC2Adapter(AbstractEC2Repository):
    """Adapter AWS thật – dùng EC2Manager đồng bộ, bọc bằng threadpool.

    Đây là bước chuyển tiếp: giữ nguyên logic hiện tại, chỉ bọc nó
    để không block event loop FastAPI.
    """

    def __init__(self, region_name: str | None = None) -> None:
        self._region = region_name
        self._mgr = EC2Manager(region_name=region_name)

    async def list_instances(self) -> List[Instance]:
        def _call() -> List[dict]:
            return self._mgr.list_instances(tags_filter=None, states=None)

        raw = await anyio.to_thread.run_sync(_call)
        return [Instance.parse_obj(item) for item in raw]

    async def start_instance(self, instance_id: str) -> Instance:
        def _call() -> dict:
            return self._mgr.start_instance(instance_id)

        result = await anyio.to_thread.run_sync(_call)
        # Chỉ có vài field, map tối thiểu
        return Instance(
            account_id="unknown",
            instance_id=result.get("InstanceId"),
            instance_type="unknown",
            state=InstanceState.running,
            availability_zone=None,
            private_ip=None,
            public_ip=None,
            key_name=None,
            name_tag=None,
            launch_time=None,
        )

    async def stop_instance(self, instance_id: str) -> Instance:
        def _call() -> dict:
            return self._mgr.stop_instance(instance_id)

        result = await anyio.to_thread.run_sync(_call)
        return Instance(
            account_id="unknown",
            instance_id=result.get("InstanceId"),
            instance_type="unknown",
            state=InstanceState.stopped,
            availability_zone=None,
            private_ip=None,
            public_ip=None,
            key_name=None,
            name_tag=None,
            launch_time=None,
        )

    async def terminate_instance(self, instance_id: str) -> Instance:
        def _call() -> dict:
            return self._mgr.terminate_instance(instance_id)

        result = await anyio.to_thread.run_sync(_call)
        return Instance(
            account_id="unknown",
            instance_id=result.get("InstanceId"),
            instance_type="unknown",
            state=InstanceState.terminated,
            availability_zone=None,
            private_ip=None,
            public_ip=None,
            key_name=None,
            name_tag=None,
            launch_time=None,
        )

    async def unit_economics(self) -> Iterable[UnitEconomicsPoint]:
        # Bản đầu: sinh dữ liệu giả dựa trên CPU utilization thật trong 24h
        # Có thể nâng cấp sau để dùng CloudWatch cost/fixed rate.
        instances = await self.list_instances()
        # Tạm thời trả list rỗng nếu không có instance
        return []

    async def list_volumes(self) -> List[Volume]:
        def _call() -> List[dict]:
            return self._mgr.list_volumes(status_filter=None)

        raw = await anyio.to_thread.run_sync(_call)
        return [Volume.parse_obj(item) for item in raw]

    async def create_volume(self, size_gib: int, az: Optional[str] = None) -> Volume:
        def _call() -> dict:
            client = self._mgr.ec2_client
            params = {"Size": size_gib, "AvailabilityZone": az or f"{self._region}a"}
            resp = client.create_volume(**params)
            return {
                "VolumeId": resp["VolumeId"],
                "SizeGiB": resp["Size"],
                "State": resp["State"],
                "AvailabilityZone": resp.get("AvailabilityZone"),
                "Throughput": resp.get("Throughput"),
                "Iops": resp.get("Iops"),
                "VolumeType": resp.get("VolumeType"),
                "AttachedInstances": [],
            }

        data = await anyio.to_thread.run_sync(_call)
        return Volume.parse_obj(data)

    async def attach_volume(self, volume_id: str, instance_id: str, device_name: str) -> dict:
        def _call() -> dict:
            return self._mgr.attach_volume(volume_id, instance_id, device_name)

        return await anyio.to_thread.run_sync(_call)

    async def detach_volume(self, volume_id: str, force: bool = False) -> dict:
        def _call() -> dict:
            return self._mgr.detach_volume(volume_id, force=force)

        return await anyio.to_thread.run_sync(_call)

    async def enforce_rate_limit(self) -> None:
        return

    async def list_events(self, limit: int = 50) -> List[EventEntry]:
        return []


