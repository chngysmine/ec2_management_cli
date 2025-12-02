from __future__ import annotations

import os
from functools import lru_cache

from fastapi import Depends

from modern_backend.adapters.aws_ec2 import AwsEC2Adapter
from modern_backend.adapters.mock_ec2 import MockEC2Adapter
from modern_backend.ports.repository import IEC2Repository


@lru_cache
def _get_repo() -> IEC2Repository:
    mode = os.getenv("APP_MODE", "MOCK").upper()
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    if mode == "LIVE":
        return AwsEC2Adapter(region_name=region)
    return MockEC2Adapter()


def get_ec2_repo() -> IEC2Repository:
    return _get_repo()


