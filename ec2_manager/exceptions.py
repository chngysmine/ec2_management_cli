class ConfigError(Exception):
    """Raised when configuration is invalid or missing required values."""


class AWSAuthError(Exception):
    """Raised when AWS credentials are missing or invalid."""


class OperationError(Exception):
    """Raised when an operation cannot be completed safely."""



