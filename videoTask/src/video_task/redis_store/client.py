from __future__ import annotations

from functools import lru_cache

from redis import Redis

from video_task.config import get_settings


@lru_cache
def get_redis() -> Redis:
    return Redis.from_url(get_settings().redis_url, decode_responses=True)


def ensure_consumer_group(redis: Redis) -> None:
    settings = get_settings()
    try:
        redis.xgroup_create(settings.video_stream, settings.video_group, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise

