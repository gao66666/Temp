import time

import httpx
import pytest
from redis import Redis
from redis.exceptions import RedisError

from video_task.config import Settings
from video_task.domain.enums import RuntimeTaskStatus
from video_task.redis_store.client import ensure_consumer_group
from video_task.redis_store.keys import (
    DEADLINES_KEY,
    FINALIZE_PENDING_KEY,
    RECLAIM_KEY,
    finalize_key,
    runtime_key,
)
from video_task.redis_store.task_store import RedisTaskStore
from video_task.services.scheduler import Scheduler
from video_task.services.worker import VideoWorker


def integration_redis() -> tuple[Settings, Redis]:
    settings = Settings(
        redis_url="redis://localhost:6380/15",
        database_url="sqlite:///var/db/integration.db",
        progress_timeout_seconds=1,
        hard_deadline_seconds=10,
    )
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        redis.ping()
    except RedisError:
        pytest.skip("local Redis is not running")
    redis.flushdb()
    ensure_consumer_group(redis)
    return settings, redis


@pytest.mark.integration
def test_expired_worker_message_can_be_reclaimed() -> None:
    settings, redis = integration_redis()
    message_id = redis.xadd(
        settings.video_stream,
        {
            "task_id": "recovery-task",
            "source_uri": "sample.mp4",
            "interval_seconds": "5",
            "result_version": "1",
        },
    )
    delivered = redis.xreadgroup(
        settings.video_group,
        "dead-worker",
        {settings.video_stream: ">"},
        count=1,
    )
    assert delivered

    now = time.time()
    redis.hset(
        runtime_key("recovery-task"),
        mapping={
            "task_id": "recovery-task",
            "status": RuntimeTaskStatus.PROCESSING.value,
            "attempt": "1",
            "owner_worker_instance_id": "dead-worker",
            "source_stream_message_id": message_id,
            "source_uri": "sample.mp4",
            "interval_seconds": "5",
            "result_version": "1",
            "deadline_at": now - 1,
            "hard_deadline_at": now + 10,
        },
    )
    redis.zadd(DEADLINES_KEY, {"recovery-task": now - 1})

    scheduler = Scheduler(redis, settings)
    try:
        scheduler.recover_expired_tasks()
    finally:
        scheduler.close()

    runtime = redis.hgetall(runtime_key("recovery-task"))
    assert runtime["status"] == RuntimeTaskStatus.RECOVERING.value
    assert runtime["attempt"] == "2"
    assert redis.zscore(RECLAIM_KEY, "recovery-task") is not None

    worker = VideoWorker(redis, settings)
    claimed_message = worker._claim_recovery_task()
    assert claimed_message is not None
    claimed_id, fields = claimed_message
    assert claimed_id == message_id
    assert fields["task_id"] == "recovery-task"

    claim = RedisTaskStore(redis, settings).claim(
        task_id="recovery-task",
        worker_instance_id=worker.instance_id,
        message_id=claimed_id,
        source_uri=fields["source_uri"],
        interval_seconds=float(fields["interval_seconds"]),
        result_version=int(fields["result_version"]),
    )
    assert claim.claimed is True
    assert claim.attempt == 2

    pending = redis.xpending_range(settings.video_stream, settings.video_group, "-", "+", 10)
    assert pending[0]["consumer"] == worker.instance_id
    redis.flushdb()


@pytest.mark.integration
def test_completed_finalization_deletes_source_stream_message() -> None:
    settings, redis = integration_redis()
    task_id = "finalized-task"
    message_id = redis.xadd(
        settings.video_stream,
        {
            "task_id": task_id,
            "source_uri": "sample.mp4",
            "interval_seconds": "5",
            "result_version": "1",
        },
    )
    assert redis.xreadgroup(
        settings.video_group,
        "test-worker",
        {settings.video_stream: ">"},
        count=1,
    )

    redis.hset(
        finalize_key(task_id),
        mapping={
            "task_id": task_id,
            "source_stream_key": settings.video_stream,
            "source_consumer_group": settings.video_group,
            "source_stream_message_id": message_id,
            "ack_done": "0",
            "callback_done": "0",
            "callback_status": "SUCCEEDED",
            "callback_event_id": f"{task_id}:media-ready:v1",
            "callback_attempt": "0",
            "result_manifest": "/tmp/manifest.json",
            "last_error": "",
        },
    )
    redis.zadd(FINALIZE_PENDING_KEY, {task_id: time.time()})

    scheduler = Scheduler(redis, settings)
    scheduler.http.close()
    scheduler.http = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"status": "SUCCEEDED"}))
    )
    try:
        scheduler._finalize_one(task_id)
    finally:
        scheduler.close()

    assert redis.xpending(settings.video_stream, settings.video_group)["pending"] == 0
    assert redis.xlen(settings.video_stream) == 0
    assert redis.exists(finalize_key(task_id)) == 0
    assert redis.zscore(FINALIZE_PENDING_KEY, task_id) is None
    assert redis.hget(runtime_key(task_id), "status") == RuntimeTaskStatus.COMPLETED.value
    redis.flushdb()
