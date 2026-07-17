from __future__ import annotations

import logging
import threading
import time
from uuid import uuid4

from redis import Redis
from redis.exceptions import RedisError

from video_task.config import Settings, get_settings
from video_task.redis_store.client import ensure_consumer_group
from video_task.redis_store.keys import RECLAIM_KEY, reclaim_lease_key
from video_task.redis_store.task_store import RedisTaskStore
from video_task.services.ffmpeg_runner import FFmpegError, FFmpegRunner, StaleAttempt


logger = logging.getLogger(__name__)


class VideoWorker:
    def __init__(self, redis: Redis, settings: Settings | None = None):
        self.redis = redis
        self.settings = settings or get_settings()
        self.instance_id = f"worker-{uuid4()}"
        self.task_store = RedisTaskStore(redis, self.settings)
        self.ffmpeg = FFmpegRunner(self.settings)
        self.stop_event = threading.Event()

    def run_forever(self) -> None:
        ensure_consumer_group(self.redis)
        heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat.start()
        logger.info("worker started instance_id=%s", self.instance_id)
        try:
            while not self.stop_event.is_set():
                try:
                    item = self._claim_recovery_task() or self._read_new_task()
                    if item is not None:
                        self._process(*item)
                except RedisError as exc:
                    logger.warning("redis unavailable; worker will retry: %s", exc)
                    self.stop_event.wait(2)
                except Exception:
                    logger.exception("unexpected worker error")
                    self.stop_event.wait(1)
        finally:
            self.stop_event.set()
            heartbeat.join(timeout=2)

    def stop(self) -> None:
        self.stop_event.set()

    def _heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.task_store.heartbeat(self.instance_id)
            except RedisError as exc:
                logger.warning("failed to publish worker heartbeat: %s", exc)
            self.stop_event.wait(self.settings.worker_heartbeat_seconds)

    def _read_new_task(self):
        result = self.redis.xreadgroup(
            self.settings.video_group,
            self.instance_id,
            {self.settings.video_stream: ">"},
            count=1,
            block=self.settings.worker_block_ms,
        )
        if not result:
            return None
        _, messages = result[0]
        return messages[0] if messages else None

    def _claim_recovery_task(self):
        task_ids = self.redis.zrange(RECLAIM_KEY, 0, 0)
        if not task_ids:
            return None
        task_id = task_ids[0]
        lease_key = reclaim_lease_key(task_id)
        if not self.redis.set(lease_key, self.instance_id, nx=True, ex=15):
            return None
        try:
            runtime = self.task_store.get_runtime(task_id)
            message_id = runtime.get("source_stream_message_id")
            if not message_id:
                self.redis.zrem(RECLAIM_KEY, task_id)
                return None
            messages = self.redis.xclaim(
                self.settings.video_stream,
                self.settings.video_group,
                self.instance_id,
                min_idle_time=0,
                message_ids=[message_id],
            )
            self.redis.zrem(RECLAIM_KEY, task_id)
            return messages[0] if messages else None
        finally:
            if self.redis.get(lease_key) == self.instance_id:
                self.redis.delete(lease_key)

    def _process(self, message_id: str, fields: dict[str, str]) -> None:
        task_id = fields["task_id"]
        source_uri = fields["source_uri"]
        interval_seconds = float(fields.get("interval_seconds", self.settings.default_interval_seconds))
        result_version = int(fields.get("result_version", 1))
        claim = self.task_store.claim(
            task_id=task_id,
            worker_instance_id=self.instance_id,
            message_id=message_id,
            source_uri=source_uri,
            interval_seconds=interval_seconds,
            result_version=result_version,
        )
        if not claim.claimed:
            # This stream entry is a duplicate of an active or completed business task.
            self.redis.xack(self.settings.video_stream, self.settings.video_group, message_id)
            logger.info("acked duplicate message task_id=%s message_id=%s", task_id, message_id)
            return

        attempt = claim.attempt
        logger.info("processing task_id=%s attempt=%s", task_id, attempt)
        try:
            manifest = self.ffmpeg.run(
                task_id=task_id,
                attempt=attempt,
                source_uri=source_uri,
                interval_seconds=interval_seconds,
                result_version=result_version,
                on_process=lambda pid, pgid: self.task_store.set_process_identity(
                    task_id, attempt, self.instance_id, pid, pgid
                ),
                on_progress=lambda snapshot: self.task_store.update_progress(
                    task_id=task_id,
                    attempt=attempt,
                    worker_instance_id=self.instance_id,
                    frame=snapshot.frame,
                    out_time_us=snapshot.out_time_us,
                ),
            )
            if self.task_store.complete_success(
                task_id=task_id,
                attempt=attempt,
                worker_instance_id=self.instance_id,
                message_id=message_id,
                result_manifest=manifest,
                result_version=result_version,
            ):
                logger.info("task media ready task_id=%s attempt=%s", task_id, attempt)
            else:
                logger.warning("discarded stale success task_id=%s attempt=%s", task_id, attempt)
        except StaleAttempt:
            logger.warning("task ownership changed task_id=%s attempt=%s", task_id, attempt)
        except FFmpegError as exc:
            error = str(exc)
            if attempt >= self.settings.max_attempts:
                stored = self.task_store.complete_failure(
                    task_id=task_id,
                    attempt=attempt,
                    worker_instance_id=self.instance_id,
                    message_id=message_id,
                    error=error,
                )
                logger.error("task sent to DLQ task_id=%s stored=%s error=%s", task_id, stored, error)
            else:
                stored = self.task_store.schedule_retry(
                    task_id=task_id,
                    attempt=attempt,
                    worker_instance_id=self.instance_id,
                    message_id=message_id,
                    error=error,
                )
                logger.warning("task scheduled for retry task_id=%s stored=%s error=%s", task_id, stored, error)
