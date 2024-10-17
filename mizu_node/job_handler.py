import json
from random import randrange
from typing import Union

from pymongo import MongoClient
from redis import Redis

from mizu_node.constants import (
    VERIFICATION_MODE,
    ASSIGNED_JOB_EXPIRE_TTL_SECONDS,
    REDIS_PENDING_JOBS_QUEUE,
    REDIS_TOTAL_ASSIGNED_JOBS,
    REDIS_ASSIGNED_JOB_PREFIX,
    SHADOW_KEY_PREFIX,
    BLOCKED_WORKER_PREFIX,
    VERIFICATION_RATIO_BASE,
)
from mizu_node.types import (
    JobType,
    PendingJob,
    AssignedJob,
    FinishedJob,
    PendingJobRequest,
    VerificationMode,
    WorkerJob,
    WorkerJobResult,
)
from mizu_node.utils import epoch
from mizu_node.worker.worker import job_worker
from mizu_node.worker.queue import verify_job_queue

VALID_JOB_TYPES = [JobType.classification, JobType.pow]


def _gen_pending_job_key(jtype: JobType) -> str:
    return REDIS_PENDING_JOBS_QUEUE + ":" + jtype


def _take_new_job(rclient: Redis, job_types: list[JobType]) -> PendingJob:
    for jtype in job_types or VALID_JOB_TYPES:
        if rclient.llen(_gen_pending_job_key(jtype)) > 0:
            job_json = rclient.rpop(_gen_pending_job_key(jtype))
            if job_json is not None:
                return PendingJob.model_validate_json(job_json)
    raise ValueError("no job available")


def _gen_assigned_job_key(job_id: str) -> str:
    return REDIS_ASSIGNED_JOB_PREFIX + job_id


def _add_assigned_job(rclient: Redis, job: AssignedJob):
    key = _gen_assigned_job_key(job.job_id)
    rclient.set(key, job.model_dump_json())
    # when key expires event is triggered, the value will be gone
    # so we use the shadow key to triggrer the event, while use
    # the real key to get the value
    rclient.set(
        SHADOW_KEY_PREFIX + key,
        job.job_id,
        ex=ASSIGNED_JOB_EXPIRE_TTL_SECONDS,
    )
    rclient.incr(REDIS_TOTAL_ASSIGNED_JOBS)


def _get_assigned_job(rclient: Redis, job_id: str) -> AssignedJob:
    job_json = rclient.get(_gen_assigned_job_key(job_id))
    if job_json is None:
        raise ValueError("job expired or not exists")
    return AssignedJob.model_validate_json(job_json)


def _remove_assigned_job(rclient: Redis, job_id: str):
    key = _gen_assigned_job_key(job_id)
    rclient.decr(REDIS_TOTAL_ASSIGNED_JOBS)
    rclient.delete(key)
    rclient.delete(SHADOW_KEY_PREFIX + key)


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


def _request_verify_job(job: WorkerJob):
    verify_job_queue.enqueue(job_worker, job)


def handle_publish_jobs(rclient: Redis, req: PendingJobRequest) -> list[str]:
    pendings = [PendingJob.from_payload(job, req.job_type) for job in req.jobs]
    json = [job.model_dump_json() for job in pendings]
    rclient.lpush(_gen_pending_job_key(req.job_type), *json)
    return [job.job_id for job in pendings]


def handle_take_job(
    rclient: Redis, worker: str, job_types: list[JobType] = []
) -> Union[WorkerJob, None]:
    if _is_worker_blocked(rclient, worker):
        raise ValueError("worker is blocked")

    pending = _take_new_job(rclient, job_types)
    assigned = AssignedJob.from_pending_job(pending, worker)
    _add_assigned_job(rclient, assigned)
    return WorkerJob.from_pending_job(pending)


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
        _request_verify_job(WorkerJob.from_pending_job(assigned))


def handle_verify_job_result(rclient: Redis, mdb: MongoClient, result: WorkerJobResult):
    job = mdb.jobs.find_one({"_id": result.job_id})
    if job is None:
        raise ValueError("invalid job")

    if len(result.output) != len(job["output"]) or set(result.output) != set(
        job["output"]
    ):
        _block_worker(rclient, job["worker"])


def get_pending_jobs_num(rclient: Redis, job_type: JobType = None) -> int:
    if job_type is not None:
        return rclient.llen(_gen_pending_job_key(job_type))
    return sum(rclient.llen(_gen_pending_job_key(jtype)) for jtype in VALID_JOB_TYPES)


def get_assigned_jobs_num(rclient: Redis) -> int:
    return rclient.get(REDIS_TOTAL_ASSIGNED_JOBS) or 0
