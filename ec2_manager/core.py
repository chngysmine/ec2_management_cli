from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple
import os

import boto3
from boto3 import session as boto3_session
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

from .exceptions import AWSAuthError, OperationError
from .utils import generate_client_token


DEFAULT_BOTO_CONFIG = BotoConfig(
    retries={"max_attempts": 10, "mode": "standard"},
    connect_timeout=5,
    read_timeout=60,
)


class EC2Manager:
    def __init__(self, region_name: Optional[str] = None):
        try:
            profile = os.getenv("AWS_PROFILE") or os.getenv("AWS_DEFAULT_PROFILE")
            # Use a dedicated Session to honor explicit profile
            session = (
                boto3_session.Session(profile_name=profile, region_name=region_name)
                if profile
                else boto3_session.Session(region_name=region_name)
            )
            self.ec2_client = session.client("ec2", config=DEFAULT_BOTO_CONFIG)
            self.ec2_res = session.resource("ec2", config=DEFAULT_BOTO_CONFIG)
            self.cw_client = session.client("cloudwatch", config=DEFAULT_BOTO_CONFIG)
            self.sts_client = session.client("sts", config=DEFAULT_BOTO_CONFIG)
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AWSAuthError("AWS credentials not found. Use IAM roles or SSO profiles.") from e

    # ---------- Instance operations ----------
    def list_instances(self, tags_filter: Optional[List[Tuple[str, str]]] = None, states: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        filters: List[Dict[str, Any]] = []
        if tags_filter:
            for key, value in tags_filter:
                filters.append({"Name": f"tag:{key}", "Values": [value]})
        if states:
            filters.append({"Name": "instance-state-name", "Values": states})

        try:
            paginator = self.ec2_client.get_paginator("describe_instances")
            page_it = paginator.paginate(Filters=filters) if filters else paginator.paginate()
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AWSAuthError("Unable to locate credentials. Check AWS_PROFILE/SSO.") from e

        results: List[Dict[str, Any]] = []
        for page in page_it:
            for reservation in page.get("Reservations", []):
                owner = reservation.get("OwnerId")
                for inst in reservation.get("Instances", []):
                    results.append(
                        {
                            "AccountId": owner,
                            "InstanceId": inst.get("InstanceId"),
                            "InstanceType": inst.get("InstanceType"),
                            "State": (inst.get("State") or {}).get("Name"),
                            "AvailabilityZone": (inst.get("Placement") or {}).get("AvailabilityZone"),
                            "PrivateIpAddress": inst.get("PrivateIpAddress"),
                            "PublicIpAddress": inst.get("PublicIpAddress"),
                            "KeyName": inst.get("KeyName"),
                            "NameTag": next((t.get("Value") for t in inst.get("Tags", []) if t.get("Key") == "Name"), None),
                            "LaunchTime": inst.get("LaunchTime").isoformat() if inst.get("LaunchTime") else None,
                        }
                    )
        return results

    def create_instance(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Create an instance using config mapping loaded from YAML.

        Requires keys: instance (dict), network (dict optional), user_data (str optional), tags (list optional)
        Idempotent via ClientToken.
        """
        instance_cfg = config.get("instance", {})
        network_cfg = config.get("network", {})
        user_data = config.get("user_data")
        tags = config.get("tags", [])

        client_token = generate_client_token()

        params: Dict[str, Any] = {
            "ImageId": instance_cfg.get("ImageId"),
            "InstanceType": instance_cfg.get("InstanceType"),
            "MinCount": int(instance_cfg.get("MinCount", 1)),
            "MaxCount": int(instance_cfg.get("MaxCount", 1)),
            "ClientToken": client_token,
        }
        if instance_cfg.get("KeyName"):
            params["KeyName"] = instance_cfg["KeyName"]
        if network_cfg.get("SubnetId"):
            params["SubnetId"] = network_cfg["SubnetId"]
        if network_cfg.get("SecurityGroupIds"):
            params["SecurityGroupIds"] = list(network_cfg["SecurityGroupIds"])
        if user_data:
            params["UserData"] = user_data
        if tags:
            params["TagSpecifications"] = [
                {
                    "ResourceType": "instance",
                    "Tags": tags,
                }
            ]

        try:
            resp = self.ec2_client.run_instances(**params)
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AWSAuthError("Unable to locate credentials. Check AWS_PROFILE/SSO.") from e
        except ClientError as e:
            raise OperationError(f"Failed to create instance: {e}") from e

        instances = resp.get("Instances", [])
        if not instances:
            raise OperationError("No instance created.")

        instance_id = instances[0]["InstanceId"]
        inst_res = self.ec2_res.Instance(instance_id)
        # Wait until running
        inst_res.wait_until_running()
        inst_res.reload()
        return {
            "InstanceId": inst_res.instance_id,
            "State": inst_res.state.get("Name"),
            "PrivateIpAddress": getattr(inst_res, "private_ip_address", None),
            "PublicIpAddress": getattr(inst_res, "public_ip_address", None),
        }

    def _get_instance_state(self, instance_id: str) -> str:
        try:
            inst = self.ec2_res.Instance(instance_id)
            inst.load()
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AWSAuthError("Unable to locate credentials. Check AWS_PROFILE/SSO.") from e
        return inst.state.get("Name")

    def stop_instance(self, instance_id: str) -> Dict[str, Any]:
        state = self._get_instance_state(instance_id)
        if state in ("stopped", "stopping"):
            waiter = self.ec2_client.get_waiter("instance_stopped")
            waiter.wait(InstanceIds=[instance_id]) if state == "stopping" else None
            return {"InstanceId": instance_id, "State": "stopped", "Message": "Already stopped"}
        if state != "running":
            return {"InstanceId": instance_id, "State": state, "Message": "Not in running state"}
        try:
            self.ec2_client.stop_instances(InstanceIds=[instance_id])
            waiter = self.ec2_client.get_waiter("instance_stopped")
            waiter.wait(InstanceIds=[instance_id])
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AWSAuthError("Unable to locate credentials. Check AWS_PROFILE/SSO.") from e
        except ClientError as e:
            raise OperationError(f"Failed to stop instance {instance_id}: {e}") from e
        return {"InstanceId": instance_id, "State": "stopped"}

    def start_instance(self, instance_id: str) -> Dict[str, Any]:
        state = self._get_instance_state(instance_id)
        if state in ("running", "pending"):
            waiter = self.ec2_client.get_waiter("instance_running")
            waiter.wait(InstanceIds=[instance_id]) if state == "pending" else None
            return {"InstanceId": instance_id, "State": "running", "Message": "Already running"}
        if state != "stopped":
            return {"InstanceId": instance_id, "State": state, "Message": "Not in stopped state"}
        try:
            self.ec2_client.start_instances(InstanceIds=[instance_id])
            waiter = self.ec2_client.get_waiter("instance_running")
            waiter.wait(InstanceIds=[instance_id])
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AWSAuthError("Unable to locate credentials. Check AWS_PROFILE/SSO.") from e
        except ClientError as e:
            raise OperationError(f"Failed to start instance {instance_id}: {e}") from e
        return {"InstanceId": instance_id, "State": "running"}

    def terminate_instance(self, instance_id: str) -> Dict[str, Any]:
        state = self._get_instance_state(instance_id)
        if state in ("shutting-down", "terminated"):
            waiter = self.ec2_client.get_waiter("instance_terminated")
            waiter.wait(InstanceIds=[instance_id]) if state == "shutting-down" else None
            return {"InstanceId": instance_id, "State": "terminated", "Message": "Already terminated"}
        try:
            self.ec2_client.terminate_instances(InstanceIds=[instance_id])
            waiter = self.ec2_client.get_waiter("instance_terminated")
            waiter.wait(InstanceIds=[instance_id])
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AWSAuthError("Unable to locate credentials. Check AWS_PROFILE/SSO.") from e
        except ClientError as e:
            raise OperationError(f"Failed to terminate instance {instance_id}: {e}") from e
        return {"InstanceId": instance_id, "State": "terminated"}

    # ---------- Volume operations ----------
    def list_volumes(self, status_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        filters: List[Dict[str, Any]] = []
        if status_filter:
            filters.append({"Name": "status", "Values": [status_filter]})

        try:
            paginator = self.ec2_client.get_paginator("describe_volumes")
            page_it = paginator.paginate(Filters=filters) if filters else paginator.paginate()
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AWSAuthError("Unable to locate credentials. Check AWS_PROFILE/SSO.") from e

        results: List[Dict[str, Any]] = []
        for page in page_it:
            for vol in page.get("Volumes", []):
                attachments = vol.get("Attachments", [])
                results.append(
                    {
                        "VolumeId": vol.get("VolumeId"),
                        "SizeGiB": vol.get("Size"),
                        "State": vol.get("State"),
                        "VolumeType": vol.get("VolumeType"),
                        "Iops": vol.get("Iops"),
                        "Throughput": vol.get("Throughput"),
                        "AvailabilityZone": vol.get("AvailabilityZone"),
                        "AttachedInstances": [a.get("InstanceId") for a in attachments],
                    }
                )
        return results

    def attach_volume(self, volume_id: str, instance_id: str, device_name: str) -> Dict[str, Any]:
        try:
            resp = self.ec2_client.attach_volume(VolumeId=volume_id, InstanceId=instance_id, Device=device_name)
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AWSAuthError("Unable to locate credentials. Check AWS_PROFILE/SSO.") from e
        except ClientError as e:
            raise OperationError(f"Failed to attach volume {volume_id} to {instance_id}: {e}") from e
        return resp

    def detach_volume(self, volume_id: str, force: bool = False) -> Dict[str, Any]:
        try:
            resp = self.ec2_client.detach_volume(VolumeId=volume_id, Force=force)
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AWSAuthError("Unable to locate credentials. Check AWS_PROFILE/SSO.") from e
        except ClientError as e:
            raise OperationError(f"Failed to detach volume {volume_id}: {e}") from e
        return resp

    def set_delete_on_termination(self, instance_id: str, device_name: str, delete_on_term: bool) -> None:
        try:
            self.ec2_client.modify_instance_attribute(
                InstanceId=instance_id,
                BlockDeviceMappings=[
                    {
                        "DeviceName": device_name,
                        "Ebs": {"DeleteOnTermination": {"Value": delete_on_term}},
                    }
                ],
            )
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AWSAuthError("Unable to locate credentials. Check AWS_PROFILE/SSO.") from e
        except ClientError as e:
            raise OperationError(
                f"Failed to set DeleteOnTermination on {instance_id} {device_name}: {e}"
            ) from e

    # ---------- Reports ----------
    def _all_regions(self) -> List[str]:
        try:
            resp = self.ec2_client.describe_regions(AllRegions=False)
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise AWSAuthError("Unable to locate credentials. Check AWS_PROFILE/SSO.") from e
        return [r["RegionName"] for r in resp.get("Regions", [])]

    def generate_inventory_report(self, regions: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        target_regions = regions or self._all_regions()
        results: List[Dict[str, Any]] = []
        for region in target_regions:
            try:
                profile = os.getenv("AWS_PROFILE") or os.getenv("AWS_DEFAULT_PROFILE")
                session = (
                    boto3_session.Session(profile_name=profile, region_name=region)
                    if profile
                    else boto3_session.Session(region_name=region)
                )
                ec2 = session.client("ec2", config=DEFAULT_BOTO_CONFIG)
            except (NoCredentialsError, PartialCredentialsError) as e:
                raise AWSAuthError("Unable to locate credentials. Check AWS_PROFILE/SSO.") from e
            paginator = ec2.get_paginator("describe_instances")
            for page in paginator.paginate():
                for reservation in page.get("Reservations", []):
                    owner = reservation.get("OwnerId")
                    for inst in reservation.get("Instances", []):
                        results.append(
                            {
                                "AccountId": owner,
                                "Region": region,
                                "InstanceId": inst.get("InstanceId"),
                                "InstanceType": inst.get("InstanceType"),
                                "State": (inst.get("State") or {}).get("Name"),
                                "AvailabilityZone": (inst.get("Placement") or {}).get("AvailabilityZone"),
                                "PrivateIpAddress": inst.get("PrivateIpAddress"),
                                "PublicIpAddress": inst.get("PublicIpAddress"),
                                "KeyName": inst.get("KeyName"),
                                "NameTag": next((t.get("Value") for t in inst.get("Tags", []) if t.get("Key") == "Name"), None),
                                "LaunchTime": inst.get("LaunchTime").isoformat() if inst.get("LaunchTime") else None,
                            }
                        )
        return results

    def _average_cpu_utilization(self, instance_id: str, region: str, days: int = 14) -> Optional[float]:
        end = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        start = end - dt.timedelta(days=days)
        profile = os.getenv("AWS_PROFILE") or os.getenv("AWS_DEFAULT_PROFILE")
        session = (
            boto3_session.Session(profile_name=profile, region_name=region)
            if profile
            else boto3_session.Session(region_name=region)
        )
        cw = session.client("cloudwatch", config=DEFAULT_BOTO_CONFIG)
        resp = cw.get_metric_statistics(
            Namespace="AWS/EC2",
            MetricName="CPUUtilization",
            Dimensions=[{"Name": "InstanceId", "Value": instance_id}],
            StartTime=start,
            EndTime=end,
            Period=3600 * 6,
            Statistics=["Average"],
        )
        datapoints = resp.get("Datapoints", [])
        if not datapoints:
            return None
        return sum(dp.get("Average", 0.0) for dp in datapoints) / len(datapoints)

    def find_wasteful_resources(self, regions: Optional[List[str]] = None, idle_cpu_threshold: float = 5.0) -> Dict[str, Any]:
        target_regions = regions or self._all_regions()
        report: Dict[str, Any] = {"idle_instances": [], "orphaned_volumes": []}

        for region in target_regions:
            profile = os.getenv("AWS_PROFILE") or os.getenv("AWS_DEFAULT_PROFILE")
            session = (
                boto3_session.Session(profile_name=profile, region_name=region)
                if profile
                else boto3_session.Session(region_name=region)
            )
            ec2 = session.client("ec2", config=DEFAULT_BOTO_CONFIG)
            # Orphaned volumes
            vol_paginator = ec2.get_paginator("describe_volumes")
            for page in vol_paginator.paginate(Filters=[{"Name": "status", "Values": ["available"]}]):
                for vol in page.get("Volumes", []):
                    report["orphaned_volumes"].append(
                        {
                            "Region": region,
                            "VolumeId": vol.get("VolumeId"),
                            "SizeGiB": vol.get("Size"),
                            "State": vol.get("State"),
                        }
                    )

            # Idle instances by CPUUtilization
            inst_paginator = ec2.get_paginator("describe_instances")
            for page in inst_paginator.paginate():
                for reservation in page.get("Reservations", []):
                    for inst in reservation.get("Instances", []):
                        if (inst.get("State") or {}).get("Name") != "running":
                            continue
                        instance_id = inst.get("InstanceId")
                        avg_cpu = self._average_cpu_utilization(instance_id, region)
                        if avg_cpu is not None and avg_cpu < idle_cpu_threshold:
                            report["idle_instances"].append(
                                {
                                    "Region": region,
                                    "InstanceId": instance_id,
                                    "AverageCPU14d": round(avg_cpu, 2),
                                    "InstanceType": inst.get("InstanceType"),
                                }
                            )
        return report



