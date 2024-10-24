import json
import time

from bson import BSON
from fastapi.encoders import jsonable_encoder
from pymongo.database import Collection
from redis import Redis
from fastapi import HTTPException, status

from mizu_node.constants import (
    REDIS_JOB_QUEUE_NAME,
    ASSIGNED_JOB_EXPIRE_TTL_SECONDS,
)

from mizu_node.security import is_worker_blocked
from mizu_node.types.common import JobType
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

job_queues = {
    job_type: JobQueue(KeyPrefix(REDIS_JOB_QUEUE_NAME + ":" + str(job_type) + ":"))
    for job_type in [JobType.classify, JobType.pow]
}


def queue_clean(rclient: Redis):
    while True:
        for queue in job_queues.values():
            queue.light_clean(rclient)
        time.sleep(60)


def handle_publish_jobs(
    rclient: Redis, publisher: str, req: PublishJobRequest
) -> list[str]:
    jobs = [build_data_job(publisher, job) for job in req.data]
    classify_queue_items = [
        QueueItem(job.job_id, job.model_dump_json())
        for job in jobs
        if job.job_type == JobType.classify
    ]
    if len(classify_queue_items) > 0:
        job_queues[JobType.classify].add_items(rclient, classify_queue_items)

    pow_queue_items = [
        QueueItem(job.job_id, job.model_dump_json())
        for job in jobs
        if job.job_type == JobType.pow
    ]
    if len(pow_queue_items) > 0:
        job_queues[JobType.pow].add_items(rclient, pow_queue_items)
    return [j.job_id for j in jobs]


def handle_take_job(rclient: Redis, worker: str, job_type: JobType) -> WorkerJob | None:
    if is_worker_blocked(rclient, worker):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="worker is blocked"
        )

    job_json = job_queues[job_type].lease(rclient, ASSIGNED_JOB_EXPIRE_TTL_SECONDS)
    if job_json is not None:
        job = DataJob.model_validate_json(job_json)
        return build_worker_job(job)
    return None


def handle_finish_job(
    rclient: Redis, mdb: Collection, worker: str, result: WorkerJobResult
):
    queue = job_queues[result.job_type]
    if not queue.lease_exists(rclient, result.job_id):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="job expired or not exists",
        )
    job_json = queue.get_item_data(rclient, result.job_id)
    job = DataJob.model_validate_json(job_json)
    finished = FinishedJob.from_models(worker, job, result)
    mdb.insert_one(jsonable_encoder(finished))
    queue.complete(rclient, job.job_id)


def handle_queue_len(rclient: Redis, job_type: JobType) -> int:
    return job_queues[job_type].queue_len(rclient)
