from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from video_task.backend.models import TaskRecord
from video_task.domain.enums import BusinessTaskStatus
from video_task.domain.schemas import TaskCallback, TaskCreate


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TaskRepository:
    def __init__(self, session: Session):
        self.session = session

    def create(self, payload: TaskCreate) -> TaskRecord:
        task = TaskRecord(
            task_id=str(uuid4()),
            status=BusinessTaskStatus.PENDING.value,
            source_uri=payload.source_uri,
            interval_seconds=payload.interval_seconds,
            result_version=payload.result_version,
        )
        self.session.add(task)
        self.session.commit()
        self.session.refresh(task)
        return task

    def get(self, task_id: str) -> TaskRecord | None:
        return self.session.get(TaskRecord, task_id)

    def mark_dispatched(self, task_id: str) -> None:
        self.session.execute(
            update(TaskRecord)
            .where(TaskRecord.task_id == task_id, TaskRecord.status == BusinessTaskStatus.PENDING.value)
            .values(
                last_dispatched_at=utcnow(),
                dispatch_count=TaskRecord.dispatch_count + 1,
            )
        )
        self.session.commit()

    def claim_for_redispatch(self, task_id: str, older_than: datetime) -> bool:
        result = self.session.execute(
            update(TaskRecord)
            .where(
                TaskRecord.task_id == task_id,
                TaskRecord.status == BusinessTaskStatus.PENDING.value,
                or_(TaskRecord.last_dispatched_at.is_(None), TaskRecord.last_dispatched_at < older_than),
            )
            .values(
                last_dispatched_at=utcnow(),
                dispatch_count=TaskRecord.dispatch_count + 1,
            )
        )
        self.session.commit()
        return result.rowcount == 1

    def pending_for_redispatch(self, older_than: datetime, limit: int = 100) -> list[TaskRecord]:
        stmt = (
            select(TaskRecord)
            .where(
                TaskRecord.status == BusinessTaskStatus.PENDING.value,
                or_(TaskRecord.last_dispatched_at.is_(None), TaskRecord.last_dispatched_at < older_than),
            )
            .order_by(TaskRecord.created_at)
            .limit(limit)
        )
        return list(self.session.scalars(stmt))

    def apply_callback(self, task: TaskRecord, payload: TaskCallback) -> TaskRecord:
        if task.callback_received:
            if task.callback_event_id == payload.event_id:
                return task
            raise ValueError("task already completed by a different callback event")

        task.callback_received = True
        task.callback_event_id = payload.event_id
        task.status = payload.status.value
        task.completed_at = utcnow()
        task.result_manifest = payload.result_manifest
        task.last_error = payload.error
        try:
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            existing = self.get(task.task_id)
            if existing and existing.callback_event_id == payload.event_id:
                return existing
            raise
        self.session.refresh(task)
        return task


def redispatch_cutoff(seconds: int) -> datetime:
    return utcnow() - timedelta(seconds=seconds)

