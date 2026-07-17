from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from video_task.backend.db import Base
from video_task.backend.models import TaskRecord
from video_task.backend.repository import TaskRepository
from video_task.domain.enums import BusinessTaskStatus, CallbackStatus
from video_task.domain.schemas import TaskCallback, TaskCreate


def make_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return Session(engine)


def test_callback_is_idempotent_for_same_event() -> None:
    with make_session() as session:
        repository = TaskRepository(session)
        task = repository.create(TaskCreate(source_uri="sample.mp4"))
        payload = TaskCallback(
            event_id=f"{task.task_id}:media-ready:v1",
            status=CallbackStatus.SUCCEEDED,
            result_manifest="/output/manifest.json",
        )

        first = repository.apply_callback(task, payload)
        second = repository.apply_callback(first, payload)

        assert first.status == BusinessTaskStatus.SUCCEEDED.value
        assert second.callback_event_id == payload.event_id
        assert second.result_manifest == "/output/manifest.json"


def test_different_callback_event_is_rejected_after_completion() -> None:
    with make_session() as session:
        repository = TaskRepository(session)
        task = repository.create(TaskCreate(source_uri="sample.mp4"))
        repository.apply_callback(
            task,
            TaskCallback(event_id="event-1", status=CallbackStatus.FAILED, error="broken"),
        )

        try:
            repository.apply_callback(
                task,
                TaskCallback(event_id="event-2", status=CallbackStatus.SUCCEEDED),
            )
        except ValueError as exc:
            assert "different callback event" in str(exc)
        else:
            raise AssertionError("different terminal event should be rejected")

