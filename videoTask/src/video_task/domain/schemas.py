from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from video_task.domain.enums import BusinessTaskStatus, CallbackStatus


class TaskCreate(BaseModel):
    source_uri: str = Field(min_length=1)
    interval_seconds: float = Field(default=5.0, gt=0)
    result_version: int = Field(default=1, ge=1)


class TaskView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    task_id: str
    status: BusinessTaskStatus
    source_uri: str
    interval_seconds: float
    result_version: int
    created_at: datetime
    last_dispatched_at: datetime | None
    dispatch_count: int
    callback_received: bool
    completed_at: datetime | None
    result_manifest: str | None
    last_error: str | None
    queued: bool | None = None


class TaskCallback(BaseModel):
    event_id: str = Field(min_length=1)
    status: CallbackStatus
    result_manifest: str | None = None
    error: str | None = None


class RedispatchResult(BaseModel):
    task_id: str
    queued: bool

