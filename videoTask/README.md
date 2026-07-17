# Video Task

Python prototype for a Redis Streams based video frame-extraction queue. It includes:

- a FastAPI + SQLite mock backend;
- a Redis Streams worker that runs FFmpeg;
- a scheduler for task recovery, delayed retry, XACK, callback, and database redispatch;
- deterministic local output and callback idempotency.

The architecture and current design decisions are documented in `.codex/MEMORY/video-frame-extraction-task-queue.md`.

## Local setup

```bash
conda env create -f environment.yml
conda run -n videoTask python -m pip install -e '.[dev]'
docker compose up -d redis
```

Create runtime configuration if needed:

```bash
cp .env.example .env
```

Generate a local sample video inside the allowed input directory:

```bash
mkdir -p var/input
conda run -n videoTask ffmpeg \
  -f lavfi -i testsrc=size=320x240:rate=25 \
  -t 12 -pix_fmt yuv420p -y var/input/sample.mp4
```

Run the three processes in separate terminals:

```bash
conda run -n videoTask video-api
conda run -n videoTask video-worker
conda run -n videoTask video-scheduler
```

Create a task:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/tasks \
  -H 'content-type: application/json' \
  -d '{"source_uri":"sample.mp4","interval_seconds":5,"result_version":1}'
```

Query the returned `task_id`:

```bash
curl http://127.0.0.1:8000/api/v1/tasks/<task_id>
```

## Tests

```bash
conda run -n videoTask pytest
```

