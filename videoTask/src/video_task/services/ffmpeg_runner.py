from __future__ import annotations

import json
import os
import queue
import shutil
import signal
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from video_task.config import Settings, get_settings


class FFmpegError(RuntimeError):
    pass


class FFmpegStalled(FFmpegError):
    pass


class StaleAttempt(FFmpegError):
    pass


@dataclass(frozen=True)
class ProgressSnapshot:
    frame: int = 0
    out_time_us: int = 0
    progress: str = "continue"


class ProgressParser:
    def __init__(self) -> None:
        self._current: dict[str, str] = {}

    def feed(self, line: str) -> ProgressSnapshot | None:
        key, separator, value = line.strip().partition("=")
        if not separator:
            return None
        self._current[key] = value
        if key != "progress":
            return None
        snapshot = ProgressSnapshot(
            frame=_safe_int(self._current.get("frame")),
            out_time_us=_progress_time_us(self._current),
            progress=value,
        )
        self._current = {}
        return snapshot


def _safe_int(value: str | None) -> int:
    try:
        return int(value or 0)
    except ValueError:
        return 0


def _progress_time_us(values: dict[str, str]) -> int:
    if "out_time_us" in values:
        return _safe_int(values["out_time_us"])
    if "out_time_ms" in values:
        # FFmpeg historically names this field out_time_ms while reporting microseconds.
        return _safe_int(values["out_time_ms"])
    return 0


def resolve_local_source(source_uri: str, input_root: Path) -> Path:
    raw = source_uri.removeprefix("file://")
    path = Path(raw)
    if not path.is_absolute():
        path = input_root / path
    resolved = path.resolve()
    root = input_root.resolve()
    if not resolved.is_relative_to(root):
        raise FFmpegError(f"source must be inside input root: {root}")
    if not resolved.is_file():
        raise FFmpegError(f"source video not found: {resolved}")
    return resolved


class FFmpegRunner:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()

    def run(
        self,
        *,
        task_id: str,
        attempt: int,
        source_uri: str,
        interval_seconds: float,
        result_version: int,
        on_process: Callable[[int, int], bool],
        on_progress: Callable[[ProgressSnapshot], bool],
    ) -> str:
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            raise FFmpegError("ffmpeg executable not found")

        source = resolve_local_source(source_uri, self.settings.input_root)
        output_dir = self.settings.output_root / task_id / f"v{result_version}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_pattern = output_dir / "frame_%06d.jpg"

        command = [
            ffmpeg,
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-nostats",
            "-stats_period",
            "1",
            "-progress",
            "pipe:1",
            "-i",
            str(source),
            "-vf",
            f"fps=1/{interval_seconds}",
            "-q:v",
            "2",
            "-y",
            str(output_pattern),
        ]

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        pgid = os.getpgid(process.pid)
        if not on_process(process.pid, pgid):
            _terminate_process_group(process, pgid)
            raise StaleAttempt("task ownership changed before FFmpeg started")

        stdout_queue: queue.Queue[str | None] = queue.Queue()
        stderr_tail: deque[str] = deque(maxlen=100)
        threading.Thread(target=_read_lines, args=(process.stdout, stdout_queue), daemon=True).start()
        threading.Thread(target=_drain_stderr, args=(process.stderr, stderr_tail), daemon=True).start()

        parser = ProgressParser()
        last_frame = 0
        last_out_time = 0
        last_real_progress = time.monotonic()
        started = time.monotonic()

        try:
            while True:
                if process.poll() is not None and stdout_queue.empty():
                    break
                try:
                    line = stdout_queue.get(timeout=1)
                except queue.Empty:
                    line = None

                if line:
                    snapshot = parser.feed(line)
                    if snapshot is not None:
                        advanced = snapshot.frame > last_frame or snapshot.out_time_us > last_out_time
                        if advanced:
                            last_frame = max(last_frame, snapshot.frame)
                            last_out_time = max(last_out_time, snapshot.out_time_us)
                            last_real_progress = time.monotonic()
                            if not on_progress(snapshot):
                                raise StaleAttempt("task ownership changed during FFmpeg processing")

                now = time.monotonic()
                if now - started > self.settings.hard_deadline_seconds:
                    raise FFmpegStalled("FFmpeg exceeded hard deadline")
                if now - last_real_progress > self.settings.progress_timeout_seconds:
                    raise FFmpegStalled("FFmpeg stopped making progress")
        except Exception:
            _terminate_process_group(process, pgid)
            raise
        finally:
            if process.poll() is None:
                _terminate_process_group(process, pgid)

        return_code = process.wait()
        if return_code != 0:
            error = "".join(stderr_tail).strip() or f"ffmpeg exited with status {return_code}"
            raise FFmpegError(error)

        frames = sorted(str(path.resolve()) for path in output_dir.glob("frame_*.jpg"))
        if not frames:
            raise FFmpegError("ffmpeg completed without producing frames")
        manifest = output_dir / "manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "task_id": task_id,
                    "attempt": attempt,
                    "result_version": result_version,
                    "source_uri": source_uri,
                    "interval_seconds": interval_seconds,
                    "frames": frames,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return str(manifest.resolve())


def _read_lines(stream, output: queue.Queue[str | None]) -> None:
    if stream is None:
        output.put(None)
        return
    for line in stream:
        output.put(line)
    output.put(None)


def _drain_stderr(stream, tail: deque[str]) -> None:
    if stream is None:
        return
    for line in stream:
        tail.append(line)


def _terminate_process_group(process: subprocess.Popen, pgid: int, grace_seconds: float = 5.0) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()

