import json
from random import randrange

from pymongo import MongoClient
from redis import Redis

from mizu_node.constants import (
    VERIFICATION_MODE,
    BLOCKED_WORKER_PREFIX,
    VERIFICATION_RATIO_BASE,
    VERIFY_JOB_CALLBACK_URL,
    VERIFY_JOB_QUEUE_NAME,
)
from mizu_node.job_queue import VALID_JOB_TYPES, JobQueue
from mizu_node.types import (
    DataJob,
    JobType,
    FinishedJob,
    PublishJobRequest,
    VerificationMode,
    WorkerJob,
    WorkerJobResult,
)
from mizu_node.utils import epoch
from mizu_node.job_queue import job_queues


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
    if VERIFICATION_MODE == VerificationMode.always:
        return True
    elif VERIFICATION_MODE == VerificationMode.random:
        return randrange(0, VERIFICATION_RATIO_BASE) == 1
    elif VERIFICATION_MODE == VerificationMode.none:
        return False
    else:
        raise ValueError("invalid verification mode")


def handle_publish_jobs(rclient: Redis, req: PublishJobRequest) -> list[str]:
    jobs = [DataJob.from_payload(job, req.job_type) for job in jobs]
    job_queues[req.job_type].add_jobs(rclient, jobs)
    return [j.job_id for j in jobs]


def handle_take_job(
    rclient: Redis, worker: str, job_types: list[JobType] | None
) -> WorkerJob | None:
    if not job_types:
        job_types = VALID_JOB_TYPES
    if _is_worker_blocked(rclient, worker):
        raise ValueError("worker is blocked")

    for job_type in job_types:
        assigned = job_queues[job_type].lease(rclient, worker)
        if assigned:
            return WorkerJob.from_data_job(assigned)


def handle_finish_job(rclient: Redis, mdb: MongoClient, result: WorkerJobResult):
    job = job_queues[result.job_type].get_job(rclient, result.job_id)
    finished = FinishedJob.from_job_result(job, result)
    _save_finished_job(mdb, finished)
    job_queues[result.job_type].complete(rclient, job.job_id)
    if _should_verify(finished):
        job = WorkerJob.from_data_job(job, VERIFY_JOB_CALLBACK_URL)
        rclient.lpush(VERIFY_JOB_QUEUE_NAME, job)


def handle_verify_job_result(rclient: Redis, mdb: MongoClient, result: WorkerJobResult):
    job = mdb.jobs.find_one({"_id": result.job_id})
    if job is None:
        raise ValueError("invalid job")

    if len(result.output) != len(job["output"]) or set(result.output) != set(
        job["output"]
    ):
        _block_worker(rclient, job["worker"])
