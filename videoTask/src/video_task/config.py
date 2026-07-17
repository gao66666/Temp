from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="VIDEO_",
        extra="ignore",
    )

    redis_url: str = "redis://localhost:6380/0"
    database_url: str = "sqlite:///var/db/tasks.db"
    callback_base_url: str = "http://127.0.0.1:8000"

    video_stream: str = "video:jobs"
    video_group: str = "video:workers"
    dlq_stream: str = "video:dlq"

    input_root: Path = Path("var/input")
    output_root: Path = Path("var/output")
    temp_root: Path = Path("var/tmp")

    default_interval_seconds: float = Field(default=5.0, gt=0)
    worker_block_ms: int = Field(default=5_000, ge=100)
    worker_heartbeat_seconds: int = Field(default=5, ge=1)
    worker_heartbeat_ttl_seconds: int = Field(default=20, ge=2)
    progress_timeout_seconds: int = Field(default=60, ge=1)
    hard_deadline_seconds: int = Field(default=1_800, ge=1)
    redispatch_after_seconds: int = Field(default=3_600, ge=1)
    scheduler_interval_seconds: float = Field(default=1.0, gt=0)
    finalize_retry_seconds: int = Field(default=10, ge=1)
    retry_delays_seconds: str = "60,240,540"
    max_attempts: int = Field(default=4, ge=1)

    @property
    def retry_delays(self) -> tuple[int, ...]:
        values = tuple(int(item.strip()) for item in self.retry_delays_seconds.split(",") if item.strip())
        if not values:
            raise ValueError("retry_delays_seconds must contain at least one integer")
        return values

    def ensure_runtime_directories(self) -> None:
        for path in (self.input_root, self.output_root, self.temp_root):
            path.mkdir(parents=True, exist_ok=True)
        if self.database_url.startswith("sqlite:///"):
            Path(self.database_url.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_runtime_directories()
    return settings
