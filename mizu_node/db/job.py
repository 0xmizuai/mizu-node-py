import time

from pymongo import MongoClient
import redis

from mizu_node.db.constants import (
    FINISH_JOB_CALLBACK_URL,
    MONGO_DB_NAME,
    MONGO_URL,
    PROCESSING_JOB_EXPIRE_TTL_SECONDS,
    REDIS_PENDING_JOBS_QUEUE,
    REDIS_PROCESSING_JOB_PREFIX,
    REDIS_TOTAL_PROCESSING_JOB,
    REDIS_URL,
    SHADOW_KEY_PREFIX,
)
from mizu_node.db.types import (
    AIRuntimeConfig,
    ClassificationJobForWorker,
    ClassificationJobFromPublisher,
    ClassificationJobResult,
    ClassificationJobResultFromWorker,
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


def handle_new_job(job: ClassificationJobFromPublisher) -> str:
    rclient.lpush(REDIS_PENDING_JOBS_QUEUE, job.model_dump_json())


def handle_take_job(worker: str) -> ClassificationJobForWorker | None:
    job_json = rclient.rpop(REDIS_PENDING_JOBS_QUEUE)
    if job_json is None:
        return None
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
        config=AIRuntimeConfig(callback_url=FINISH_JOB_CALLBACK_URL),
    )


def handle_finish_job(result: ClassificationJobResultFromWorker):
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


def get_pending_jobs_num():
    return rclient.llen(REDIS_PENDING_JOBS_QUEUE)


def get_processing_jobs_num():
    return rclient.get(REDIS_TOTAL_PROCESSING_JOB)
