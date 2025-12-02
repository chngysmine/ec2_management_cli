#!/usr/bin/env python3
"""
Chạy web server với mock mode - đơn giản và dễ dùng
Không cần Docker, không cần AWS account
"""

import os
import sys
from datetime import datetime

# Set environment
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
os.environ['EC2_MANAGER_MOCK_MODE'] = '1'

# Mock data
MOCK_INSTANCES = [
    {
        "AccountId": "123456789012",
        "InstanceId": "i-1234567890abcdef0",
        "ImageId": "ami-12345678",
        "InstanceType": "t2.micro",
        "State": "running",
        "Placement": {"AvailabilityZone": "us-east-1a"},
        "PrivateIpAddress": "10.0.1.10",
        "PublicIpAddress": "54.123.45.67",
        "LaunchTime": datetime.utcnow(),
        "KeyName": "test-key",
        "Tags": [
            {"Key": "Name", "Value": "Test-Instance-1"},
            {"Key": "Environment", "Value": "Development"}
        ]
    },
    {
        "AccountId": "123456789012",
        "InstanceId": "i-0987654321fedcba0",
        "ImageId": "ami-87654321",
        "InstanceType": "t2.small",
        "State": "stopped",
        "Placement": {"AvailabilityZone": "us-east-1b"},
        "PrivateIpAddress": "10.0.1.11",
        "PublicIpAddress": None,
        "LaunchTime": datetime.utcnow(),
        "KeyName": "test-key",
        "Tags": [
            {"Key": "Name", "Value": "Test-Instance-2"},
            {"Key": "Environment", "Value": "Production"}
        ]
    },
    {
        "AccountId": "123456789012",
        "InstanceId": "i-abcdef1234567890",
        "ImageId": "ami-abcdef12",
        "InstanceType": "t3.medium",
        "State": "running",
        "Placement": {"AvailabilityZone": "us-east-1a"},
        "PrivateIpAddress": "10.0.1.12",
        "PublicIpAddress": "54.123.45.68",
        "LaunchTime": datetime.utcnow(),
        "KeyName": "test-key-2",
        "Tags": [
            {"Key": "Name", "Value": "Test-Instance-3"}
        ]
    }
]

MOCK_VOLUMES = [
    {
        "VolumeId": "vol-1234567890abcdef0",
        "Size": 10,
        "State": "available",
        "VolumeType": "gp2",
        "Iops": None,
        "Throughput": None,
        "AvailabilityZone": "us-east-1a",
        "Attachments": []
    },
    {
        "VolumeId": "vol-0987654321fedcba0",
        "Size": 20,
        "State": "in-use",
        "VolumeType": "gp3",
        "Iops": 3000,
        "Throughput": 125,
        "AvailabilityZone": "us-east-1b",
        "Attachments": [
            {
                "VolumeId": "vol-0987654321fedcba0",
                "InstanceId": "i-1234567890abcdef0",
                "Device": "/dev/sdf",
                "State": "attached"
            }
        ]
    }
]

# Patch EC2Manager before importing web_flask
class MockEC2Manager:
    """Mock EC2Manager that returns test data"""
    
    def __init__(self, region_name=None):
        self.region_name = region_name or 'us-east-1'
    
    def list_instances(self, tags_filter=None, states=None):
        """Return mock instances"""
        result = []
        for inst in MOCK_INSTANCES:
            # Convert to format expected by templates
            item = {
                "AccountId": inst.get("AccountId"),
                "InstanceId": inst["InstanceId"],
                "InstanceType": inst["InstanceType"],
                "State": inst["State"],
                "AvailabilityZone": inst["Placement"]["AvailabilityZone"],
                "PrivateIpAddress": inst.get("PrivateIpAddress"),
                "PublicIpAddress": inst.get("PublicIpAddress"),
                "KeyName": inst.get("KeyName"),
                "NameTag": next((t["Value"] for t in inst.get("Tags", []) if t["Key"] == "Name"), None),
                "LaunchTime": inst["LaunchTime"].isoformat() if isinstance(inst["LaunchTime"], datetime) else inst.get("LaunchTime")
            }
            
            # Apply filters
            if states and item["State"] not in states:
                continue
            if tags_filter:
                inst_tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
                if not all(inst_tags.get(k) == v for k, v in tags_filter):
                    continue
            
            result.append(item)
        return result
    
    def list_volumes(self, status_filter=None):
        """Return mock volumes"""
        result = []
        for vol in MOCK_VOLUMES:
            item = {
                "VolumeId": vol["VolumeId"],
                "SizeGiB": vol["Size"],
                "State": vol["State"],
                "VolumeType": vol["VolumeType"],
                "Iops": vol.get("Iops"),
                "Throughput": vol.get("Throughput"),
                "AvailabilityZone": vol["AvailabilityZone"],
                "AttachedInstances": [a["InstanceId"] for a in vol.get("Attachments", [])]
            }
            
            if status_filter and item["State"] != status_filter:
                continue
            
            result.append(item)
        return result
    
    def generate_inventory_report(self, regions=None):
        """Return mock inventory"""
        result = []
        target_regions = regions or ["us-east-1"]
        for region in target_regions:
            for inst in MOCK_INSTANCES:
                result.append({
                    "AccountId": inst.get("AccountId"),
                    "Region": region,
                    "InstanceId": inst["InstanceId"],
                    "InstanceType": inst["InstanceType"],
                    "State": inst["State"],
                    "AvailabilityZone": inst["Placement"]["AvailabilityZone"],
                    "PrivateIpAddress": inst.get("PrivateIpAddress"),
                    "PublicIpAddress": inst.get("PublicIpAddress"),
                    "KeyName": inst.get("KeyName"),
                    "NameTag": next((t["Value"] for t in inst.get("Tags", []) if t["Key"] == "Name"), None),
                    "LaunchTime": inst["LaunchTime"].isoformat() if isinstance(inst["LaunchTime"], datetime) else inst.get("LaunchTime")
                })
        return result
    
    def find_wasteful_resources(self, regions=None, idle_cpu_threshold=5.0):
        """Return mock cost optimization report"""
        return {
            "idle_instances": [
                {
                    "Region": "us-east-1",
                    "InstanceId": "i-abcdef1234567890",
                    "AverageCPU14d": 2.5,
                    "InstanceType": "t3.medium"
                }
            ],
            "orphaned_volumes": [
                {
                    "Region": "us-east-1",
                    "VolumeId": "vol-1234567890abcdef0",
                    "SizeGiB": 10,
                    "State": "available"
                }
            ]
        }
    
    def start_instance(self, instance_id):
        return {"InstanceId": instance_id, "State": "running"}
    
    def stop_instance(self, instance_id):
        return {"InstanceId": instance_id, "State": "stopped"}
    
    def terminate_instance(self, instance_id):
        return {"InstanceId": instance_id, "State": "terminated"}
    
    def create_instance(self, config):
        return {
            "InstanceId": "i-new1234567890",
            "State": "running",
            "PrivateIpAddress": "10.0.1.100",
            "PublicIpAddress": "54.123.45.100"
        }
    
    def attach_volume(self, volume_id, instance_id, device_name):
        return {"VolumeId": volume_id, "InstanceId": instance_id, "Device": device_name, "State": "attaching"}
    
    def detach_volume(self, volume_id, force=False):
        return {"VolumeId": volume_id, "State": "detaching"}
    
    def set_delete_on_termination(self, instance_id, device_name, delete_on_term):
        pass

# Monkey patch EC2Manager before importing
import ec2_manager.core
ec2_manager.core.EC2Manager = MockEC2Manager

# Also patch in web_flask
import ec2_manager.web_flask
ec2_manager.web_flask.EC2Manager = MockEC2Manager

if __name__ == '__main__':
    import sys
    import io
    # Fix encoding for Windows console
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    
    print("="*60)
    print("EC2 Manager - Mock Mode")
    print("="*60)
    print("Using mock data (no AWS account needed)")
    print("Web Interface: http://localhost:8000?region=us-east-1")
    print("="*60)
    print(f"\nMock data includes:")
    print(f"   - {len(MOCK_INSTANCES)} test instances")
    print(f"   - {len(MOCK_VOLUMES)} test volumes")
    print("\nThis is MOCK MODE - no real AWS calls")
    print("="*60)
    print("\nStarting web server...\n")
    
    from ec2_manager.web_flask import run
    try:
        run()
    except KeyboardInterrupt:
        print("\n\nStopping...")
        print("Done")

