from random import randrange
import time

from fastapi import requests
from pymongo import MongoClient
import redis

from mizu_node.db.constants import (
    FINISH_JOB_CALLBACK_URL,
    VERIFY_JOB_URL,
    VERIFY_JOB_CALLBACK_URL,
    MONGO_DB_NAME,
    MONGO_URL,
    PROCESSING_JOB_EXPIRE_TTL_SECONDS,
    REDIS_PENDING_JOBS_QUEUE,
    REDIS_PROCESSING_JOB_PREFIX,
    REDIS_TOTAL_PROCESSING_JOB,
    REDIS_URL,
    SHADOW_KEY_PREFIX,
    BLOCKED_WORKER_PREFIX,
    VERIFICATION_RATIO_BASE,
)
from mizu_node.db.types import (
    ClassificationJobForWorker,
    ClassificationJobFromPublisher,
    ClassificationJobResult,
    ClassificationJobResult,
    ProcessingJob,
)


rclient = redis.Redis(REDIS_URL)
mclient = MongoClient(MONGO_URL)
mdb = mclient[MONGO_DB_NAME]


def now():
    return int(time.time())


def _add_processing_job(client: redis.Redis, _id: str, serialized_job: str):
    client.set(REDIS_PROCESSING_JOB_PREFIX + _id, serialized_job)
    client.set(
        SHADOW_KEY_PREFIX + REDIS_PROCESSING_JOB_PREFIX + _id,
        serialized_job,
        ex=PROCESSING_JOB_EXPIRE_TTL_SECONDS,
    )
    client.incr(REDIS_TOTAL_PROCESSING_JOB)


def _remove_processing_job(client: redis.Redis, _id: str):
    client.decr(REDIS_TOTAL_PROCESSING_JOB)
    client.delete(REDIS_PROCESSING_JOB_PREFIX + _id)
    client.delete(SHADOW_KEY_PREFIX + REDIS_PROCESSING_JOB_PREFIX + _id)


def _block_worker(worker: str):
    rclient.set(BLOCKED_WORKER_PREFIX + worker, 1)


def _is_worker_blocked(worker: str) -> bool:
    return rclient.get(BLOCKED_WORKER_PREFIX + worker) == 1


def _should_verify() -> bool:
    return randrange(0, VERIFICATION_RATIO_BASE) == 1


def handle_new_job(jobs: list[ClassificationJobFromPublisher]) -> str:
    jobs_json = [job.model_dump_json() for job in jobs]
    for job in jobs_json:
        rclient.lpush(REDIS_PENDING_JOBS_QUEUE, job)


def handle_take_job(worker: str) -> ClassificationJobForWorker | None:
    if _is_worker_blocked(worker):
        raise ValueError("Worker is blocked")

    job_json = rclient.rpop(REDIS_PENDING_JOBS_QUEUE)
    if job_json is None:
        raise ValueError("No job available")

    job = ClassificationJobFromPublisher.model_validate_json(job_json)
    processing_job = ProcessingJob(
        _id=job._id,
        publisher=job.publisher,
        created_at=job.created_at,
        worker=worker,
        assigned_at=now(),
    )
    _add_processing_job(
        rclient,
        job._id,
        processing_job.model_dump_json(),
    )
    return ClassificationJobForWorker(
        _id=job._id,
        callback_url=FINISH_JOB_CALLBACK_URL,
    )


def handle_finish_job(result: ClassificationJobResult):
    processing_job_json = rclient.get(REDIS_PROCESSING_JOB_PREFIX + result._id)
    if processing_job_json is None:  # job expired or not exists
        return None

    job = ProcessingJob.model_validate_json(processing_job_json)
    # worker mismatch
    if job.worker != result.worker:
        return None
    # already expired
    if job.assigned_at < now() - PROCESSING_JOB_EXPIRE_TTL_SECONDS:
        return None  # already expired

    result = ClassificationJobResult(
        _id=job._id,
        publisher=job.publisher,
        created_at=job.created_at,
        worker=job.worker,
        assigned_at=job.assigned_at,
        finished_at=now(),
        tags=job.tags,
    )
    mdb.jobs.insert_one(vars(result))
    _remove_processing_job(rclient, job._id)
    if _should_verify(result):
        verify_job = ClassificationJobForWorker(
            _id=job._id,
            callback_url=VERIFY_JOB_CALLBACK_URL,
        )
        requests.post(VERIFY_JOB_URL, json=verify_job.model_dump_json())


def handle_verify_job_result(result: ClassificationJobResult):
    job = mdb.jobs.find_one({"_id": result._id})
    if job is None:
        raise ValueError("Job not found")

    if len(result.tags) != len(job["tags"]) or set(result.tags) != set(job["tags"]):
        _block_worker(job["worker"])


def get_pending_jobs_num():
    return rclient.llen(REDIS_PENDING_JOBS_QUEUE)


def get_processing_jobs_num():
    return rclient.get(REDIS_TOTAL_PROCESSING_JOB)
