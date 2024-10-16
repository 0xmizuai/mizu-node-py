import json
from random import randrange

from fastapi import requests
from pymongo import MongoClient
from redis import Redis

from mizu_node.constants import (
    FINISH_JOB_CALLBACK_URL,
    VERIFY_JOB_URL,
    VERIFY_JOB_CALLBACK_URL,
    ASSIGNED_JOB_EXPIRE_TTL_SECONDS,
    REDIS_PENDING_JOBS_QUEUE,
    REDIS_TOTAL_ASSIGNED_JOB,
    REDIS_ASSIGNED_JOB_PREFIX,
    SHADOW_KEY_PREFIX,
    BLOCKED_WORKER_PREFIX,
    VERIFICATION_RATIO_BASE,
)
from mizu_node.types import (
    PendingJob,
    AssignedJob,
    FinishedJob,
    WorkerJob,
    WorkerJobResult,
)
from mizu_node.utils import epoch


def _add_new_jobs(rclient: Redis, jobs: list[PendingJob]):
    jobs_json = [job.model_dump_json() for job in jobs]
    rclient.lpush(REDIS_PENDING_JOBS_QUEUE, *jobs_json)


def _take_new_job(rclient: Redis) -> PendingJob:
    job_json = rclient.rpop(REDIS_PENDING_JOBS_QUEUE)
    if job_json is None:
        raise ValueError("no job available")
    return PendingJob.model_validate_json(job_json)


def _gen_assigned_job_key(job_id: str) -> str:
    return REDIS_ASSIGNED_JOB_PREFIX + job_id


def _add_assigned_job(rclient: Redis, job_id: str, serialized_job: str):
    key = _gen_assigned_job_key(job_id)
    rclient.set(key, serialized_job)
    rclient.set(
        SHADOW_KEY_PREFIX + key,
        job_id,
        ex=ASSIGNED_JOB_EXPIRE_TTL_SECONDS,
    )
    rclient.incr(REDIS_TOTAL_ASSIGNED_JOB)


def _get_assigned_job(rclient: Redis, job_id: str) -> AssignedJob:
    job_json = rclient.get(_gen_assigned_job_key(job_id))
    if job_json is None:
        raise ValueError("job expired or not exists")
    return AssignedJob.model_validate_json(job_json)


def _remove_assigned_job(rclient: Redis, job_id: str):
    rclient.decr(REDIS_TOTAL_ASSIGNED_JOB)
    rclient.delete(REDIS_ASSIGNED_JOB_PREFIX + job_id)
    rclient.delete(SHADOW_KEY_PREFIX + REDIS_ASSIGNED_JOB_PREFIX + job_id)


def _save_finished_job(mdb: MongoClient, result: FinishedJob):
    to_insert = vars(result)
    to_insert["_id"] = result.job_id
    mdb.jobs.insert_one(to_insert)


def _block_worker(rclient: Redis, worker: str):
    rclient.set(
        BLOCKED_WORKER_PREFIX + worker,
        json.dumps({"blocked": True, "updated_at": epoch()}),
    )


def _is_worker_blocked(rclient: Redis, worker: str) -> bool:
    return rclient.exists(BLOCKED_WORKER_PREFIX + worker)


def _should_verify(result: WorkerJobResult = None) -> bool:
    return randrange(0, VERIFICATION_RATIO_BASE) == 1


def _request_verify_job(job: WorkerJob):
    requests.post(VERIFY_JOB_URL, json=job.model_dump_json())


def handle_new_jobs(rclient: Redis, jobs: list[PendingJob]):
    _add_new_jobs(rclient, jobs)


def handle_take_job(rclient: Redis, worker: str) -> WorkerJob | None:
    if _is_worker_blocked(rclient, worker):
        raise ValueError("worker is blocked")

    pending = _take_new_job(rclient)
    assigned = AssignedJob.from_pending_job(pending, worker)
    _add_assigned_job(
        rclient,
        pending.input,
        assigned.model_dump_json(),
    )
    return WorkerJob.from_pending_job(pending, FINISH_JOB_CALLBACK_URL)


def handle_finish_job(rclient: Redis, mdb: MongoClient, result: WorkerJobResult):
    assigned = _get_assigned_job(rclient, result.job_id)
    # worker mismatch
    if assigned.worker != result.worker:
        raise ValueError("worker mismatch")
    # already expired
    if assigned.assigned_at < epoch() - ASSIGNED_JOB_EXPIRE_TTL_SECONDS:
        raise ValueError("job expired")

    finished = FinishedJob.from_assigned_job(assigned, result.output)
    _save_finished_job(mdb, finished)
    _remove_assigned_job(rclient, assigned.job_id)
    if _should_verify(finished):
        _request_verify_job(
            WorkerJob.from_pending_job(assigned, VERIFY_JOB_CALLBACK_URL)
        )


def handle_verify_job_result(rclient: Redis, mdb: MongoClient, result: WorkerJobResult):
    job = mdb.jobs.find_one({"_id": result.job_id})
    if job is None:
        raise ValueError("invalid job")

    if len(result.output) != len(job["output"]) or set(result.output) != set(
        job["output"]
    ):
        _block_worker(rclient, job["worker"])


def get_pending_jobs_num(rclient: Redis) -> int:
    return rclient.llen(REDIS_PENDING_JOBS_QUEUE)


def get_assigned_jobs_num(rclient: Redis) -> int:
    return rclient.get(REDIS_TOTAL_ASSIGNED_JOB) or 0
