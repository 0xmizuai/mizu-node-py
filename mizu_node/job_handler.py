import json
from random import randrange
import time

from pymongo import MongoClient
from pymongo.database import Database
from redis import Redis
from fastapi import status

from mizu_node.constants import (
    REDIS_JOB_QUEUE_NAME,
    VERIFICATION_MODE,
    BLOCKED_WORKER_PREFIX,
    VERIFICATION_RATIO_BASE,
    VERIFY_JOB_CALLBACK_URL,
    VERIFY_JOB_QUEUE_NAME,
    ASSIGNED_JOB_EXPIRE_TTL_SECONDS,
)

from mizu_node.error_handler import MizuError
from mizu_node.types.common import JobType, VerificationMode
from mizu_node.types.data_job import (
    DataJob,
    FinishedJob,
    PublishJobRequest,
    WorkerJob,
    WorkerJobResult,
    build_data_job,
    build_worker_job,
)
from mizu_node.types.job_queue import JobQueue, QueueItem
from mizu_node.types.key_prefix import KeyPrefix
from mizu_node.utils import epoch

VALID_JOB_TYPES = [JobType.classify, JobType.pow]
job_queues = {
    job_type: JobQueue(KeyPrefix(REDIS_JOB_QUEUE_NAME + ":" + job_type + ":"))
    for job_type in VALID_JOB_TYPES
}


def queue_clean(rclient: Redis):
    while True:
        for queue in job_queues.values():
            queue.light_clean(rclient)
        time.sleep(60)


def _block_worker(rclient: Redis, worker: str):
    rclient.set(
        BLOCKED_WORKER_PREFIX + worker,
        json.dumps({"blocked": True, "updated_at": epoch()}),
    )


def _is_worker_blocked(rclient: Redis, worker: str) -> bool:
    return rclient.exists(BLOCKED_WORKER_PREFIX + worker)


def handle_publish_jobs(
    rclient: Redis, publisher: str, req: PublishJobRequest
) -> list[str]:
    jobs = [build_data_job(publisher, job) for job in req.data]
    classify_queue_items = [
        QueueItem(job.job_id, job.model_dump_json())
        for job in jobs
        if job.job_type == JobType.classify
    ]
    pow_queue_items = [
        QueueItem(job.job_id, job.model_dump_json())
        for job in jobs
        if job.job_type == JobType.pow
    ]

    job_queues[JobType.classify].add_items(rclient, classify_queue_items)
    job_queues[JobType.pow].add_items(rclient, pow_queue_items)
    return [j.job_id for j in jobs]


def handle_take_job(rclient: Redis, worker: str, job_type: JobType) -> WorkerJob | None:
    if _is_worker_blocked(rclient, worker):
        raise MizuError(
            status=status.HTTP_401_UNAUTHORIZED, message="worker is blocked"
        )

    job_json = job_queues[job_type].lease(rclient, ASSIGNED_JOB_EXPIRE_TTL_SECONDS)
    if job_json is not None:
        job = DataJob.model_validate_json(job_json)
        return build_worker_job(job)
    return None


def handle_finish_job(
    rclient: Redis, mdb: Database, worker: str, result: WorkerJobResult
):
    queue = job_queues[result.job_type]
    if not queue.lease_exists(rclient, result.job_id):
        raise MizuError(
            status=status.HTTP_422_UNPROCESSABLE_ENTITY,
            message="job expired or not exists",
        )

    job_json = queue.get_item_data(rclient, result.job_id)
    job = DataJob.model_validate_json(job_json)
    finished = FinishedJob(worker, job, result)
    mdb["jobs"].insert_one(finished.__dict__)
    queue.complete(rclient, job.job_id)
