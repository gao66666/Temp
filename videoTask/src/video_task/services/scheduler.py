from __future__ import annotations

import logging
import os
import signal
import time
from datetime import timedelta
from uuid import uuid4

import httpx
from redis import Redis
from redis.exceptions import RedisError

from video_task.backend.db import create_schema, session_scope
from video_task.backend.repository import TaskRepository, redispatch_cutoff
from video_task.config import Settings, get_settings
from video_task.domain.enums import RuntimeTaskStatus
from video_task.redis_store.client import ensure_consumer_group
from video_task.redis_store.keys import (
    DEADLINES_KEY,
    FINALIZE_PENDING_KEY,
    RECLAIM_KEY,
    RETRY_KEY,
    finalize_key,
    finalize_lease_key,
    runtime_key,
)
from video_task.redis_store.task_store import RedisTaskStore
from video_task.services.producer import TaskProducer


logger = logging.getLogger(__name__)


ACK_AND_MARK_SCRIPT = """
redis.call('XACK', ARGV[1], ARGV[2], ARGV[3])
redis.call('HSET', KEYS[1], 'ack_done', '1')
return 1
"""


class Scheduler:
    def __init__(self, redis: Redis, settings: Settings | None = None):
        self.redis = redis
        self.settings = settings or get_settings()
        self.instance_id = f"scheduler-{uuid4()}"
        self.task_store = RedisTaskStore(redis, self.settings)
        # Internal callbacks must not be routed through workstation proxy settings.
        self.http = httpx.Client(timeout=10, trust_env=False)

    def close(self) -> None:
        self.http.close()

    def run_once(self) -> None:
        self.recover_expired_tasks()
        self.dispatch_due_retries()
        self.finalize_due_tasks()
        self.redispatch_pending_database_tasks()

    def recover_expired_tasks(self) -> None:
        now = time.time()
        for task_id in self.redis.zrangebyscore(DEADLINES_KEY, "-inf", now, start=0, num=100):
            runtime = self.task_store.get_runtime(task_id)
            if runtime.get("status") != RuntimeTaskStatus.PROCESSING.value:
                self.redis.zrem(DEADLINES_KEY, task_id)
                continue
            owner = runtime.get("owner_worker_instance_id", "")
            alive = self.task_store.worker_alive(owner)
            hard_deadline = float(runtime.get("hard_deadline_at", 0) or 0)
            reason = "worker heartbeat expired" if not alive else "ffmpeg progress timeout"
            if alive and hard_deadline and now >= hard_deadline:
                reason = "ffmpeg hard deadline exceeded"
            invalidated = self.task_store.invalidate_for_recovery(task_id, reason)
            if invalidated is None:
                continue
            self._terminate_recorded_process(runtime)
            logger.warning("task queued for PEL reclaim task_id=%s reason=%s", task_id, reason)

    def _terminate_recorded_process(self, runtime: dict[str, str]) -> None:
        try:
            pgid = int(runtime.get("ffmpeg_pgid", "0"))
        except ValueError:
            return
        if pgid <= 0:
            return
        try:
            os.killpg(pgid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            return

    def dispatch_due_retries(self) -> None:
        now = time.time()
        for task_id in self.redis.zrangebyscore(RETRY_KEY, "-inf", now, start=0, num=100):
            key = runtime_key(task_id)
            runtime = self.redis.hgetall(key)
            if runtime.get("status") != RuntimeTaskStatus.RETRY_WAIT.value:
                self.redis.zrem(RETRY_KEY, task_id)
                continue
            fields = {
                "task_id": task_id,
                "source_uri": runtime["source_uri"],
                "interval_seconds": runtime["interval_seconds"],
                "result_version": runtime["result_version"],
            }
            with self.redis.pipeline(transaction=True) as pipe:
                pipe.xadd(self.settings.video_stream, fields)
                pipe.hset(key, mapping={"status": RuntimeTaskStatus.PREPARED.value, "next_retry_at": ""})
                pipe.zrem(RETRY_KEY, task_id)
                pipe.execute()
            logger.info("retry task returned to stream task_id=%s", task_id)

    def finalize_due_tasks(self) -> None:
        now = time.time()
        for task_id in self.redis.zrangebyscore(FINALIZE_PENDING_KEY, "-inf", now, start=0, num=100):
            lease_key = finalize_lease_key(task_id)
            if not self.redis.set(lease_key, self.instance_id, nx=True, ex=30):
                continue
            try:
                self._finalize_one(task_id)
            finally:
                if self.redis.get(lease_key) == self.instance_id:
                    self.redis.delete(lease_key)

    def _finalize_one(self, task_id: str) -> None:
        key = finalize_key(task_id)
        record = self.redis.hgetall(key)
        if not record:
            self.redis.zrem(FINALIZE_PENDING_KEY, task_id)
            return

        if record.get("ack_done") != "1":
            self.redis.eval(
                ACK_AND_MARK_SCRIPT,
                1,
                key,
                record["source_stream_key"],
                record["source_consumer_group"],
                record["source_stream_message_id"],
            )
            record["ack_done"] = "1"

        if record.get("callback_done") != "1":
            payload = {
                "event_id": record["callback_event_id"],
                "status": record["callback_status"],
                "result_manifest": record.get("result_manifest") or None,
                "error": record.get("last_error") or None,
            }
            try:
                response = self.http.post(
                    f"{self.settings.callback_base_url}/api/v1/tasks/{task_id}/callback",
                    json=payload,
                )
                response.raise_for_status()
                self.redis.hset(key, mapping={"callback_done": "1", "last_error": ""})
                record["callback_done"] = "1"
            except Exception as exc:
                attempt = self.redis.hincrby(key, "callback_attempt", 1)
                next_retry = time.time() + self.settings.finalize_retry_seconds
                self.redis.hset(key, mapping={"next_retry_at": next_retry, "last_error": str(exc)[:2000]})
                self.redis.zadd(FINALIZE_PENDING_KEY, {task_id: next_retry})
                logger.warning("callback failed task_id=%s attempt=%s error=%s", task_id, attempt, exc)
                return

        if record.get("ack_done") == "1" and record.get("callback_done") == "1":
            with self.redis.pipeline(transaction=True) as pipe:
                pipe.hset(
                    runtime_key(task_id),
                    mapping={"status": RuntimeTaskStatus.COMPLETED.value, "completed_at": time.time()},
                )
                pipe.xdel(record["source_stream_key"], record["source_stream_message_id"])
                pipe.zrem(FINALIZE_PENDING_KEY, task_id)
                pipe.delete(key)
                results = pipe.execute()
            logger.info(
                "task finalization complete task_id=%s stream_message_deleted=%s",
                task_id,
                bool(results[1]),
            )

    def redispatch_pending_database_tasks(self) -> None:
        create_schema()
        cutoff = redispatch_cutoff(self.settings.redispatch_after_seconds)
        with session_scope() as session:
            repository = TaskRepository(session)
            task_ids = [task.task_id for task in repository.pending_for_redispatch(cutoff)]
        for task_id in task_ids:
            with session_scope() as session:
                repository = TaskRepository(session)
                if not repository.claim_for_redispatch(task_id, cutoff):
                    continue
                task = repository.get(task_id)
                try:
                    TaskProducer(self.redis, self.settings).publish(task)
                    logger.info("database task redispatched task_id=%s", task_id)
                except RedisError:
                    logger.warning("redispatch failed task_id=%s", task_id, exc_info=True)
