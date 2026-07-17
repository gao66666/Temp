from __future__ import annotations

from redis import Redis

from video_task.config import Settings, get_settings
from video_task.backend.models import TaskRecord


class TaskProducer:
    def __init__(self, redis: Redis, settings: Settings | None = None):
        self.redis = redis
        self.settings = settings or get_settings()

    def publish(self, task: TaskRecord) -> str:
        return self.redis.xadd(
            self.settings.video_stream,
            {
                "task_id": task.task_id,
                "source_uri": task.source_uri,
                "interval_seconds": str(task.interval_seconds),
                "result_version": str(task.result_version),
            },
        )

