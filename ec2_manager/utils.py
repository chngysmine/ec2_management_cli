import json
import logging
import sys
import uuid
from typing import Any, Dict, Optional

import yaml


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "time": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if hasattr(record, "extra") and isinstance(record.extra, dict):
            payload.update(record.extra)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(verbosity: int = 0) -> logging.Logger:
    """Configure root logger to emit structured JSON logs.

    verbosity: 0=INFO, 1=DEBUG
    """
    logger = logging.getLogger("ec2_manager")
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter())

    level = logging.DEBUG if verbosity > 0 else logging.INFO
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def get_env_bool(name: str, default: bool = False) -> bool:
    import os

    val = os.getenv(name)
    if val is None:
        return default
    return val.lower() in {"1", "true", "yes", "on"}


def load_config(filepath: str) -> Dict[str, Any]:
    """Load YAML configuration from file path and perform minimal validation."""
    with open(filepath, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError("Configuration file must define a YAML mapping at the top level")

    # Optional minimal checks; allow flexible schemas
    if "instance" not in data or not isinstance(data["instance"], dict):
        raise ValueError("Missing 'instance' section in configuration")

    return data


def generate_client_token() -> str:
    return str(uuid.uuid4())
