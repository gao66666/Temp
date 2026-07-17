from __future__ import annotations


DEADLINES_KEY = "task:deadlines"
RETRY_KEY = "task:retry"
RECLAIM_KEY = "task:reclaim"
FINALIZE_PENDING_KEY = "task:finalize:pending"


def runtime_key(task_id: str) -> str:
    return f"task:runtime:{task_id}"


def finalize_key(task_id: str) -> str:
    return f"task:finalize:{task_id}"


def finalize_lease_key(task_id: str) -> str:
    return f"task:finalize:lease:{task_id}"


def reclaim_lease_key(task_id: str) -> str:
    return f"task:reclaim:lease:{task_id}"


def heartbeat_key(instance_id: str) -> str:
    return f"worker:heartbeat:{instance_id}"

