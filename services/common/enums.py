from __future__ import annotations

from enum import Enum


class _StringEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class StorageMode(_StringEnum):
    LOCAL = "Local"
    REMOTE = "Remote"


class MediaType(_StringEnum):
    PHOTO = "Photo"
    VIDEO = "Video"


class SourcePlatform(_StringEnum):
    IOS = "iOS"
    ANDROID = "Android"
    WINDOWS = "Windows"
    LINUX_CLI = "LinuxCLI"
    WINDOWS_CLI = "WindowsCLI"


class StorageState(_StringEnum):
    LOCAL_ONLY = "LocalOnly"
    UPLOAD_PENDING = "UploadPending"
    UPLOADING = "Uploading"
    REMOTE_AVAILABLE = "RemoteAvailable"
    TRASHED = "Trashed"
    PURGED = "Purged"


class UserFacingState(_StringEnum):
    PREPARING = "Preparing"
    UPLOADING = "Uploading"
    PROCESSING = "Processing"
    READY = "Ready"
    WAITING_FOR_MONTHLY_QUOTA = "WaitingForMonthlyQuota"
    NEEDS_ATTENTION = "NeedsAttention"


class EnrichmentStatus(_StringEnum):
    NOT_REQUESTED = "NotRequested"
    QUEUED = "Queued"
    PROCESSING = "Processing"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    DEFERRED_QUOTA = "DeferredQuota"


class ProcessingJobStatus(_StringEnum):
    QUEUED = "Queued"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    DEFERRED_QUOTA = "DeferredQuota"
    CANCELLED = "Cancelled"
