from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from video_task.backend.db import Base
from video_task.domain.enums import BusinessTaskStatus


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TaskRecord(Base):
    __tablename__ = "tasks"

    task_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    status: Mapped[str] = mapped_column(String(20), default=BusinessTaskStatus.PENDING.value, index=True)
    source_uri: Mapped[str] = mapped_column(Text)
    interval_seconds: Mapped[float] = mapped_column(Float)
    result_version: Mapped[int] = mapped_column(Integer, default=1)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dispatch_count: Mapped[int] = mapped_column(Integer, default=0)

    callback_received: Mapped[bool] = mapped_column(Boolean, default=False)
    callback_event_id: Mapped[str | None] = mapped_column(String(160), nullable=True, unique=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    result_manifest: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

