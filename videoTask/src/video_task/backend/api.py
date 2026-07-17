from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from video_task.backend.db import create_schema
from video_task.backend.routes import router
from video_task.config import get_settings
from video_task.redis_store.client import ensure_consumer_group, get_redis


@asynccontextmanager
async def lifespan(_: FastAPI):
    get_settings().ensure_runtime_directories()
    create_schema()
    try:
        ensure_consumer_group(get_redis())
    except Exception:
        # API creation remains available while Redis is down; PENDING rows are retried later.
        pass
    yield


app = FastAPI(title="Video Task Mock Backend", version="0.1.0", lifespan=lifespan)
app.include_router(router)

