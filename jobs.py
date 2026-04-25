"""
Background job registry — per-session, per-kind job tracking via ThreadPoolExecutor.

Each Streamlit session gets a unique user_token (UUID). Jobs are scoped by
(user_token, kind) so one user's results are never visible to another.
"""

import threading
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional

# Completed jobs older than this are evicted from the registry on the next submit().
_JOB_TTL_SECONDS = 3600


class JobStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"


@dataclass
class Job:
    key: str
    kind: str
    params: dict
    status: JobStatus = JobStatus.QUEUED
    result: Any = None
    error: Optional[str] = None
    events: deque = field(default_factory=lambda: deque(maxlen=50))
    cancel_evt: threading.Event = field(default_factory=threading.Event)
    future: Any = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class JobRegistry:
    def __init__(self, max_workers: int = 4):
        self._lock = threading.RLock()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._jobs: dict[str, dict[str, Job]] = {}

    def _evict_stale(self) -> None:
        """Remove user_token entries where every job is terminal and finished over TTL ago."""
        terminal = {JobStatus.DONE, JobStatus.ERROR, JobStatus.CANCELLED}
        cutoff = datetime.now() - timedelta(seconds=_JOB_TTL_SECONDS)
        stale = [
            token
            for token, jobs in self._jobs.items()
            if all(
                j.status in terminal and j.finished_at is not None and j.finished_at < cutoff
                for j in jobs.values()
            )
        ]
        for token in stale:
            del self._jobs[token]

    def submit(self, user_token: str, kind: str, params: dict, worker_fn: Callable) -> Job:
        """Cancel any running same-kind job for this user, then submit a new one."""
        with self._lock:
            self._evict_stale()
            user_jobs = self._jobs.setdefault(user_token, {})
            old = user_jobs.get(kind)
            if old is not None and old.status in (JobStatus.QUEUED, JobStatus.RUNNING):
                old.cancel_evt.set()
                old.status = JobStatus.CANCELLED
            job = Job(key=str(uuid.uuid4()), kind=kind, params=dict(params))
            user_jobs[kind] = job
            job.future = self._executor.submit(self._run_job, job, worker_fn)
            return job

    def _run_job(self, job: Job, worker_fn: Callable) -> None:
        job.started_at = datetime.now()
        job.status = JobStatus.RUNNING

        def emit(level: str, msg: str) -> None:
            job.events.append({"level": level, "msg": msg})

        try:
            job.result = worker_fn(job.params, emit, job.cancel_evt)
            job.status = JobStatus.CANCELLED if job.cancel_evt.is_set() else JobStatus.DONE
        except Exception as exc:
            job.error = str(exc)
            job.status = JobStatus.ERROR
        finally:
            job.finished_at = datetime.now()

    def latest(self, user_token: str, kind: str) -> Optional[Job]:
        """Return the most recent job for (user_token, kind), or None."""
        with self._lock:
            return self._jobs.get(user_token, {}).get(kind)


registry = JobRegistry(max_workers=4)
