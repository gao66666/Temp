from __future__ import annotations

import time
from dataclasses import dataclass

from redis import Redis
from redis.exceptions import WatchError

from video_task.config import Settings, get_settings
from video_task.domain.enums import CallbackStatus, RuntimeTaskStatus
from video_task.redis_store.keys import (
    DEADLINES_KEY,
    FINALIZE_PENDING_KEY,
    RECLAIM_KEY,
    RETRY_KEY,
    finalize_key,
    heartbeat_key,
    runtime_key,
)


CLAIM_SCRIPT = """
local status = redis.call('HGET', KEYS[1], 'status')
local now = tonumber(ARGV[1])
if status == 'PROCESSING' then
  return {0, tonumber(redis.call('HGET', KEYS[1], 'attempt') or '0')}
end
if status == 'MEDIA_READY' or status == 'FINALIZING' or status == 'COMPLETED' or status == 'FAILED' then
  return {2, tonumber(redis.call('HGET', KEYS[1], 'attempt') or '0')}
end
if status == 'RETRY_WAIT' then
  local retry_at = tonumber(redis.call('HGET', KEYS[1], 'next_retry_at') or '0')
  if retry_at > now then
    return {0, tonumber(redis.call('HGET', KEYS[1], 'attempt') or '0')}
  end
end
local attempt
if status == 'RECOVERING' then
  attempt = tonumber(redis.call('HGET', KEYS[1], 'attempt') or '1')
else
  attempt = redis.call('HINCRBY', KEYS[1], 'attempt', 1)
end
redis.call('HSET', KEYS[1],
  'task_id', ARGV[2],
  'status', 'PROCESSING',
  'owner_worker_instance_id', ARGV[3],
  'source_stream_message_id', ARGV[4],
  'source_uri', ARGV[5],
  'interval_seconds', ARGV[6],
  'result_version', ARGV[7],
  'started_at', ARGV[1],
  'last_progress_at', ARGV[1],
  'deadline_at', ARGV[8],
  'hard_deadline_at', ARGV[9],
  'last_error', '')
redis.call('ZADD', KEYS[2], ARGV[8], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[2])
return {1, attempt}
"""


PROGRESS_SCRIPT = """
if redis.call('HGET', KEYS[1], 'status') ~= 'PROCESSING' then return 0 end
if redis.call('HGET', KEYS[1], 'attempt') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'owner_worker_instance_id') ~= ARGV[2] then return 0 end
local hard = tonumber(redis.call('HGET', KEYS[1], 'hard_deadline_at') or ARGV[4])
local next_deadline = tonumber(ARGV[4])
if next_deadline > hard then next_deadline = hard end
redis.call('HSET', KEYS[1],
  'last_progress_at', ARGV[3],
  'frame', ARGV[5],
  'out_time_us', ARGV[6],
  'deadline_at', next_deadline)
redis.call('ZADD', KEYS[2], next_deadline, ARGV[7])
return 1
"""


SUCCESS_SCRIPT = """
if redis.call('HGET', KEYS[1], 'status') ~= 'PROCESSING' then return 0 end
if redis.call('HGET', KEYS[1], 'attempt') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'owner_worker_instance_id') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[1],
  'status', 'FINALIZING',
  'result_manifest', ARGV[4],
  'finished_at', ARGV[3])
redis.call('ZREM', KEYS[2], ARGV[5])
redis.call('HSET', KEYS[3],
  'task_id', ARGV[5],
  'source_stream_key', ARGV[6],
  'source_consumer_group', ARGV[7],
  'source_stream_message_id', ARGV[8],
  'task_attempt', ARGV[1],
  'result_manifest', ARGV[4],
  'ack_done', '0',
  'callback_done', '0',
  'callback_status', 'SUCCEEDED',
  'callback_event_id', ARGV[9],
  'callback_attempt', '0',
  'next_retry_at', ARGV[3],
  'last_error', '')
redis.call('ZADD', KEYS[4], ARGV[3], ARGV[5])
return 1
"""


RETRY_SCRIPT = """
if redis.call('HGET', KEYS[1], 'status') ~= 'PROCESSING' then return 0 end
if redis.call('HGET', KEYS[1], 'attempt') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'owner_worker_instance_id') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[1],
  'status', 'RETRY_WAIT',
  'next_retry_at', ARGV[3],
  'last_error', ARGV[4],
  'owner_worker_instance_id', '')
redis.call('ZREM', KEYS[2], ARGV[5])
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[5])
redis.call('XACK', ARGV[6], ARGV[7], ARGV[8])
return 1
"""


FAILURE_SCRIPT = """
if redis.call('HGET', KEYS[1], 'status') ~= 'PROCESSING' then return 0 end
if redis.call('HGET', KEYS[1], 'attempt') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'owner_worker_instance_id') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[1], 'status', 'FINALIZING', 'last_error', ARGV[4], 'finished_at', ARGV[3])
redis.call('ZREM', KEYS[2], ARGV[5])
redis.call('XADD', ARGV[6], '*', 'task_id', ARGV[5], 'attempt', ARGV[1], 'error', ARGV[4])
redis.call('HSET', KEYS[3],
  'task_id', ARGV[5],
  'source_stream_key', ARGV[7],
  'source_consumer_group', ARGV[8],
  'source_stream_message_id', ARGV[9],
  'task_attempt', ARGV[1],
  'result_manifest', '',
  'ack_done', '0',
  'callback_done', '0',
  'callback_status', 'FAILED',
  'callback_event_id', ARGV[10],
  'callback_attempt', '0',
  'next_retry_at', ARGV[3],
  'last_error', ARGV[4])
redis.call('ZADD', KEYS[4], ARGV[3], ARGV[5])
return 1
"""


INVALIDATE_SCRIPT = """
if redis.call('HGET', KEYS[1], 'status') ~= 'PROCESSING' then return 0 end
local deadline = tonumber(redis.call('HGET', KEYS[1], 'deadline_at') or '0')
if deadline > tonumber(ARGV[1]) then return 0 end
local attempt = redis.call('HINCRBY', KEYS[1], 'attempt', 1)
redis.call('HSET', KEYS[1],
  'status', 'RECOVERING',
  'last_error', ARGV[3],
  'owner_worker_instance_id', '')
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('ZADD', KEYS[3], ARGV[1], ARGV[2])
return attempt
"""


@dataclass(frozen=True)
class ClaimResult:
    claimed: bool
    attempt: int
    terminal_or_active: bool = False


class RedisTaskStore:
    def __init__(self, redis: Redis, settings: Settings | None = None):
        self.redis = redis
        self.settings = settings or get_settings()

    def heartbeat(self, instance_id: str) -> None:
        self.redis.set(
            heartbeat_key(instance_id),
            str(time.time()),
            ex=self.settings.worker_heartbeat_ttl_seconds,
        )

    def claim(
        self,
        *,
        task_id: str,
        worker_instance_id: str,
        message_id: str,
        source_uri: str,
        interval_seconds: float,
        result_version: int,
    ) -> ClaimResult:
        now = time.time()
        hard_deadline = now + self.settings.hard_deadline_seconds
        next_deadline = min(now + self.settings.progress_timeout_seconds, hard_deadline)
        result = self.redis.eval(
            CLAIM_SCRIPT,
            3,
            runtime_key(task_id),
            DEADLINES_KEY,
            RECLAIM_KEY,
            now,
            task_id,
            worker_instance_id,
            message_id,
            source_uri,
            interval_seconds,
            result_version,
            next_deadline,
            hard_deadline,
        )
        code, attempt = int(result[0]), int(result[1])
        return ClaimResult(claimed=code == 1, attempt=attempt, terminal_or_active=code in (0, 2))

    def update_progress(
        self,
        *,
        task_id: str,
        attempt: int,
        worker_instance_id: str,
        frame: int,
        out_time_us: int,
    ) -> bool:
        now = time.time()
        next_deadline = now + self.settings.progress_timeout_seconds
        result = self.redis.eval(
            PROGRESS_SCRIPT,
            2,
            runtime_key(task_id),
            DEADLINES_KEY,
            attempt,
            worker_instance_id,
            now,
            next_deadline,
            frame,
            out_time_us,
            task_id,
        )
        return bool(result)

    def set_process_identity(self, task_id: str, attempt: int, instance_id: str, pid: int, pgid: int) -> bool:
        key = runtime_key(task_id)
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(key)
                    values = pipe.hmget(key, "status", "attempt", "owner_worker_instance_id")
                    if values != [RuntimeTaskStatus.PROCESSING.value, str(attempt), instance_id]:
                        pipe.unwatch()
                        return False
                    pipe.multi()
                    pipe.hset(key, mapping={"ffmpeg_pid": pid, "ffmpeg_pgid": pgid})
                    pipe.execute()
                    return True
                except WatchError:
                    continue

    def complete_success(
        self,
        *,
        task_id: str,
        attempt: int,
        worker_instance_id: str,
        message_id: str,
        result_manifest: str,
        result_version: int,
    ) -> bool:
        now = time.time()
        event_id = f"{task_id}:media-ready:v{result_version}"
        result = self.redis.eval(
            SUCCESS_SCRIPT,
            4,
            runtime_key(task_id),
            DEADLINES_KEY,
            finalize_key(task_id),
            FINALIZE_PENDING_KEY,
            attempt,
            worker_instance_id,
            now,
            result_manifest,
            task_id,
            self.settings.video_stream,
            self.settings.video_group,
            message_id,
            event_id,
        )
        return bool(result)

    def schedule_retry(
        self,
        *,
        task_id: str,
        attempt: int,
        worker_instance_id: str,
        message_id: str,
        error: str,
    ) -> bool:
        delay_index = min(max(attempt - 1, 0), len(self.settings.retry_delays) - 1)
        retry_at = time.time() + self.settings.retry_delays[delay_index]
        result = self.redis.eval(
            RETRY_SCRIPT,
            3,
            runtime_key(task_id),
            DEADLINES_KEY,
            RETRY_KEY,
            attempt,
            worker_instance_id,
            retry_at,
            error[:2000],
            task_id,
            self.settings.video_stream,
            self.settings.video_group,
            message_id,
        )
        return bool(result)

    def complete_failure(
        self,
        *,
        task_id: str,
        attempt: int,
        worker_instance_id: str,
        message_id: str,
        error: str,
    ) -> bool:
        now = time.time()
        event_id = f"{task_id}:failed"
        result = self.redis.eval(
            FAILURE_SCRIPT,
            4,
            runtime_key(task_id),
            DEADLINES_KEY,
            finalize_key(task_id),
            FINALIZE_PENDING_KEY,
            attempt,
            worker_instance_id,
            now,
            error[:2000],
            task_id,
            self.settings.dlq_stream,
            self.settings.video_stream,
            self.settings.video_group,
            message_id,
            event_id,
        )
        return bool(result)

    def invalidate_for_recovery(self, task_id: str, reason: str) -> int | None:
        result = self.redis.eval(
            INVALIDATE_SCRIPT,
            3,
            runtime_key(task_id),
            DEADLINES_KEY,
            RECLAIM_KEY,
            time.time(),
            task_id,
            reason[:2000],
        )
        return int(result) if result else None

    def get_runtime(self, task_id: str) -> dict[str, str]:
        return self.redis.hgetall(runtime_key(task_id))

    def worker_alive(self, instance_id: str) -> bool:
        return bool(instance_id and self.redis.exists(heartbeat_key(instance_id)))
