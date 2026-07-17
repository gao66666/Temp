from enum import StrEnum


class BusinessTaskStatus(StrEnum):
    PENDING = "PENDING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class RuntimeTaskStatus(StrEnum):
    PREPARED = "PREPARED"
    PROCESSING = "PROCESSING"
    RECOVERING = "RECOVERING"
    RETRY_WAIT = "RETRY_WAIT"
    MEDIA_READY = "MEDIA_READY"
    FINALIZING = "FINALIZING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class CallbackStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

