from hashlib import sha512
import os
import time
from typing import Iterator

from pymongo.database import Collection
from redis import Redis
from fastapi import HTTPException, status
import requests


from mizu_node.constants import DEFAULT_POW_DIFFICULTY
from mizu_node.security import is_worker_blocked
from mizu_node.types.job import (
    DataJob,
    JobType,
    PowContext,
    PublishJobRequest,
    QueryJobRequest,
    WorkerJob,
    WorkerJobResult,
    build_worker_job,
)
from mizu_node.types.job_queue import job_queue


def handle_publish_jobs(
    jobs_coll: Collection, publisher: str, req: PublishJobRequest
) -> Iterator[str]:
    jobs = [DataJob.from_job_payload(publisher, job) for job in req.data]
    jobs_coll.insert_many([job.model_dump(by_alias=True) for job in jobs])
    for job in jobs:
        with job_queue(job.job_type, mode="producer") as q:
            q.add_item(build_worker_job(job))
            yield job.job_id


def handle_query_job(
    mdb: Collection, publisher: str, req: QueryJobRequest
) -> list[WorkerJobResult] | None:
    job_ids = req.job_ids
    if not job_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="job_ids is required"
        )
    jobs = list(
        mdb.find(
            {"_id": {"$in": job_ids[0:1000]}, "publisher": publisher},
            {
                "_id": 1,
                "jobType": 1,
                "powResult": 1,
                "classifyResult": 1,
                "batchClassifyResult": 1,
                "finishedAt": 1,
            },
        )
    )
    if not jobs:
        return None
    return [WorkerJobResult(**job) for job in jobs]


def handle_take_job(rclient: Redis, worker: str, job_type: JobType) -> WorkerJob | None:
    if is_worker_blocked(rclient, worker):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="worker is blocked"
        )
    with job_queue(job_type) as q:
        return q.get(rclient)


def handle_finish_job(
    rclient: Redis, jobs: Collection, user: str, job_result: WorkerJobResult
):
    update_data = _validate_job_result(jobs, job_result)
    with job_queue(job_result.job_type) as q:
        if not q.ack(rclient, job_result.job_id):
            raise HTTPException(status_code=status.HTTP_410_GONE, detail="job expired")
    jobs.update_one(
        {"_id": job_result.job_id},
        {
            "$set": {
                "worker": user,
                "finishedAt": int(time.time()),
                **update_data,
            }
        },
    )
    requests.post(
        os.environ["BACKEND_SERVICE_URL"] + "/settle_rewards",
        json={
            "job_id": job_result.job_id,
            "job_type": job_result.job_type,
            "worker": user,
        },
        headers={"Authorization": f"Bearer {os.environ['API_SECRET_KEY']}"},
    )


def handle_queue_len(job_type: JobType) -> int:
    with job_queue(job_type) as q:
        return q.queue_len()


def _validate_batch_classify_result(result: WorkerJobResult):
    if result.batch_classify_result is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="batch_classify_result is required",
        )
    filtered = [
        x.model_dump(by_alias=True) for x in result.batch_classify_result if x.labels
    ]
    return {"batchClassifyResult": filtered}


def _validate_pow_result(ctx: PowContext, result: WorkerJobResult):
    if result.pow_result is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="pow_result is required",
        )
    hash_output = sha512((ctx.seed + result.pow_result).encode("utf-8")).hexdigest()
    if not all(b == "0" for b in hash_output[:DEFAULT_POW_DIFFICULTY]):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="invalid pow_result: hash does not meet difficulty requirement",
        )
    return {"powResult": result.pow_result}


def _validate_job_result(jobs: Collection, result: WorkerJobResult):
    doc = jobs.find_one({"_id": result.job_id})
    if doc is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="job not found"
        )

    if doc.get("finishedAt", None) is not None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="job already finished",
        )

    if doc["jobType"] != result.job_type:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="job type mismatch"
        )

    if doc["jobType"] == JobType.batch_classify:
        return _validate_batch_classify_result(result)
    elif doc["jobType"] == JobType.pow:
        return _validate_pow_result(PowContext(**doc["powCtx"]), result)
    elif doc["jobType"] == JobType.classify:
        if result.classify_result is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="classify_result is required",
            )
        return {"classifyResult": result.classify_result}
    else:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="invalid job type",
        )
