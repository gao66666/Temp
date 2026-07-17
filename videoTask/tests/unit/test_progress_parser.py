from pathlib import Path

import pytest

from video_task.services.ffmpeg_runner import FFmpegError, ProgressParser, resolve_local_source


def test_progress_parser_emits_complete_snapshot() -> None:
    parser = ProgressParser()
    assert parser.feed("frame=12\n") is None
    assert parser.feed("out_time_us=5000000\n") is None
    snapshot = parser.feed("progress=continue\n")

    assert snapshot is not None
    assert snapshot.frame == 12
    assert snapshot.out_time_us == 5_000_000
    assert snapshot.progress == "continue"


def test_progress_parser_resets_between_blocks() -> None:
    parser = ProgressParser()
    parser.feed("frame=12")
    parser.feed("progress=continue")
    snapshot = parser.feed("progress=end")

    assert snapshot is not None
    assert snapshot.frame == 0
    assert snapshot.progress == "end"


def test_source_must_stay_inside_input_root(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    input_root.mkdir()
    video = input_root / "sample.mp4"
    video.write_bytes(b"video")

    assert resolve_local_source("sample.mp4", input_root) == video.resolve()

    outside = tmp_path / "outside.mp4"
    outside.write_bytes(b"video")
    with pytest.raises(FFmpegError):
        resolve_local_source(str(outside), input_root)

