import json
from random import randrange
import time

from fastapi import requests
from pymongo import MongoClient
from redis import Redis

from mizu_node.constants import (
    FINISH_JOB_CALLBACK_URL,
    VERIFY_JOB_URL,
    VERIFY_JOB_CALLBACK_URL,
    PROCESSING_JOB_EXPIRE_TTL_SECONDS,
    REDIS_PENDING_JOBS_QUEUE,
    REDIS_PROCESSING_JOB_PREFIX,
    REDIS_TOTAL_PROCESSING_JOB,
    SHADOW_KEY_PREFIX,
    BLOCKED_WORKER_PREFIX,
    VERIFICATION_RATIO_BASE,
)
from mizu_node.types import (
    ClassificationJobDBResult,
    ClassificationJobForWorker,
    ClassificationJobFromPublisher,
    ClassificationJobResult,
    ClassificationJobResult,
    ProcessingJob,
)


def now():
    return int(time.time())


def _add_new_jobs(rclient: Redis, jobs: list[ClassificationJobFromPublisher]):
    jobs_json = [job.model_dump_json() for job in jobs]
    rclient.lpush(REDIS_PENDING_JOBS_QUEUE, *jobs_json)


def _take_new_job(rclient: Redis) -> ClassificationJobFromPublisher:
    job_json = rclient.rpop(REDIS_PENDING_JOBS_QUEUE)
    if job_json is None:
        raise ValueError("no job available")
    return ClassificationJobFromPublisher.model_validate_json(job_json)


def _add_processing_job(rclient: Redis, key: str, serialized_job: str):
    rclient.set(REDIS_PROCESSING_JOB_PREFIX + key, serialized_job)
    rclient.set(
        SHADOW_KEY_PREFIX + REDIS_PROCESSING_JOB_PREFIX + key,
        serialized_job,
        ex=PROCESSING_JOB_EXPIRE_TTL_SECONDS,
    )
    rclient.incr(REDIS_TOTAL_PROCESSING_JOB)


def _get_processing_job(rclient: Redis, key: str) -> ProcessingJob:
    processing_job_json = rclient.get(REDIS_PROCESSING_JOB_PREFIX + key)
    if processing_job_json is None:
        return ValueError("job expired or not exists")
    return ProcessingJob.model_validate_json(processing_job_json)


def _remove_processing_job(rclient: Redis, key: str):
    rclient.decr(REDIS_TOTAL_PROCESSING_JOB)
    rclient.delete(REDIS_PROCESSING_JOB_PREFIX + key)
    rclient.delete(SHADOW_KEY_PREFIX + REDIS_PROCESSING_JOB_PREFIX + key)


def _save_job_result(mdb: MongoClient, result: ClassificationJobDBResult):
    to_insert = vars(result)
    to_insert["_id"] = result.key
    del to_insert["key"]
    mdb.jobs.insert_one(to_insert)


def _block_worker(rclient: Redis, worker: str):
    rclient.set(
        BLOCKED_WORKER_PREFIX + worker,
        json.dumps({"blocked": True, "updated_at": now()}),
    )


def _is_worker_blocked(rclient: Redis, worker: str) -> bool:
    return rclient.exists(BLOCKED_WORKER_PREFIX + worker)


def _should_verify(result: ClassificationJobResult = None) -> bool:
    return randrange(0, VERIFICATION_RATIO_BASE) == 1


def _request_verify_job(job: ClassificationJobForWorker):
    requests.post(VERIFY_JOB_URL, json=job.model_dump_json())


def handle_new_jobs(rclient: Redis, jobs: list[ClassificationJobFromPublisher]):
    _add_new_jobs(rclient, jobs)


def handle_take_job(rclient: Redis, worker: str) -> ClassificationJobForWorker | None:
    if _is_worker_blocked(rclient, worker):
        raise ValueError("worker is blocked")

    job = _take_new_job(rclient)
    processing_job = ProcessingJob(
        key=job.key,
        publisher=job.publisher,
        created_at=job.created_at,
        worker=worker,
        assigned_at=now(),
    )
    _add_processing_job(
        rclient,
        job.key,
        processing_job.model_dump_json(),
    )
    return ClassificationJobForWorker(
        key=job.key,
        callback_url=FINISH_JOB_CALLBACK_URL,
    )


def handle_finish_job(
    rclient: Redis, mdb: MongoClient, result: ClassificationJobResult
):
    job = _get_processing_job(rclient, result.key)
    # worker mismatch
    if job.worker != result.worker:
        raise ValueError("worker mismatch")
    # already expired
    if job.assigned_at < now() - PROCESSING_JOB_EXPIRE_TTL_SECONDS:
        raise ValueError("job expired")

    jresult = ClassificationJobDBResult(
        key=job.key,
        publisher=job.publisher,
        created_at=job.created_at,
        worker=job.worker,
        assigned_at=job.assigned_at,
        finished_at=now(),
        tags=result.tags,
    )
    _save_job_result(mdb, jresult)
    _remove_processing_job(rclient, job.key)
    if _should_verify(jresult):
        _request_verify_job(
            ClassificationJobForWorker(
                key=job.key,
                callback_url=VERIFY_JOB_CALLBACK_URL,
            )
        )


def handle_verify_job_result(
    rclient: Redis, mdb: MongoClient, result: ClassificationJobResult
):
    job = mdb.jobs.find_one({"_id": result.key})
    if job is None:
        raise ValueError("invalid job")

    if len(result.tags) != len(job["tags"]) or set(result.tags) != set(job["tags"]):
        _block_worker(rclient, job["worker"])


def get_pending_jobs_num(rclient: Redis):
    return rclient.llen(REDIS_PENDING_JOBS_QUEUE)


def get_processing_jobs_num(rclient: Redis):
    return rclient.get(REDIS_TOTAL_PROCESSING_JOB)
