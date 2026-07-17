from __future__ import annotations

import logging
import signal
import threading

from redis.exceptions import RedisError

from video_task.config import get_settings
from video_task.redis_store.client import ensure_consumer_group, get_redis
from video_task.services.scheduler import Scheduler


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = get_settings()
    redis = get_redis()
    ensure_consumer_group(redis)
    scheduler = Scheduler(redis, settings)
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    try:
        while not stopped.is_set():
            try:
                scheduler.run_once()
            except RedisError as exc:
                logging.warning("redis unavailable; scheduler will retry: %s", exc)
            except Exception:
                logging.exception("scheduler cycle failed")
            stopped.wait(settings.scheduler_interval_seconds)
    finally:
        scheduler.close()


if __name__ == "__main__":
    main()
