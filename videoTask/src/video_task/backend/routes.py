from __future__ import annotations

from fastapi import APIRouter, HTTPException, Response, status
from redis.exceptions import RedisError

from video_task.backend.db import session_scope
from video_task.backend.repository import TaskRepository
from video_task.domain.schemas import RedispatchResult, TaskCallback, TaskCreate, TaskView
from video_task.redis_store.client import get_redis
from video_task.services.producer import TaskProducer


router = APIRouter(prefix="/api/v1")


def as_view(task, queued: bool | None = None) -> TaskView:
    view = TaskView.model_validate(task)
    return view.model_copy(update={"queued": queued})


@router.post("/tasks", response_model=TaskView, status_code=status.HTTP_202_ACCEPTED)
def create_task(payload: TaskCreate) -> TaskView:
    with session_scope() as session:
        repository = TaskRepository(session)
        task = repository.create(payload)
        queued = False
        try:
            TaskProducer(get_redis()).publish(task)
            repository.mark_dispatched(task.task_id)
            task = repository.get(task.task_id)
            queued = True
        except RedisError:
            # The committed PENDING row is the recovery source when Redis is down.
            pass
        return as_view(task, queued=queued)


@router.get("/tasks/{task_id}", response_model=TaskView)
def get_task(task_id: str) -> TaskView:
    with session_scope() as session:
        task = TaskRepository(session).get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return as_view(task)


@router.post("/tasks/{task_id}/callback", response_model=TaskView)
def callback(task_id: str, payload: TaskCallback) -> TaskView:
    with session_scope() as session:
        repository = TaskRepository(session)
        task = repository.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        try:
            task = repository.apply_callback(task, payload)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return as_view(task)


@router.post("/tasks/{task_id}/redispatch", response_model=RedispatchResult)
def redispatch(task_id: str) -> RedispatchResult:
    with session_scope() as session:
        repository = TaskRepository(session)
        task = repository.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        if task.callback_received:
            raise HTTPException(status_code=409, detail="terminal task cannot be redispatched")
        try:
            TaskProducer(get_redis()).publish(task)
            repository.mark_dispatched(task.task_id)
            return RedispatchResult(task_id=task_id, queued=True)
        except RedisError as exc:
            raise HTTPException(status_code=503, detail="redis unavailable") from exc


@router.get("/health/live", status_code=204)
def live() -> Response:
    return Response(status_code=204)


@router.get("/health/ready", status_code=204)
def ready() -> Response:
    try:
        get_redis().ping()
        with session_scope() as session:
            session.connection()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="dependency unavailable") from exc
    return Response(status_code=204)

