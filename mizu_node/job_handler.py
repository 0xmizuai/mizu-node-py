import os
import time
from typing import Iterator

from fastapi.encoders import jsonable_encoder
from pymongo.database import Collection
from redis import Redis
from fastapi import HTTPException, status
import requests


from mizu_node.security import is_worker_blocked
from mizu_node.types.job import (
    DataJob,
    JobType,
    PublishJobRequest,
    QueryJobRequest,
    WorkerJob,
    WorkerJobResult,
    build_worker_job,
)
from mizu_node.types.job_queue import JobQueueV2


job_queues = {
    job_type: JobQueueV2("job_queue_" + str(job_type))
    for job_type in [JobType.classify, JobType.pow, JobType.batch_classify]
}


def job_queue(job_type: JobType):
    return job_queues[job_type]


def handle_publish_jobs(
    jobs_coll: Collection, publisher: str, req: PublishJobRequest
) -> Iterator[str]:
    jobs = [DataJob.from_job_payload(publisher, job) for job in req.data]
    jobs_coll.insert_many([jsonable_encoder(job) for job in jobs])
    for job in jobs:
        worker_job = build_worker_job(job)
        job_queue(job.job_type).add_item(jsonable_encoder(worker_job))
        yield job.job_id


def handle_query_job(
    mdb: Collection, req: QueryJobRequest
) -> list[WorkerJobResult] | None:
    job_ids = req.job_ids
    if not job_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="job_ids is required"
        )
    jobs = mdb.find(
        {"job_id": {"$in": job_ids.slice(1, 1000)}},
        {
            "_id": 1,
            "jobType": 1,
            "powResult": 1,
            "classifyResult": 1,
            "batchClassifyResult": 1,
            "finishedAt": 1,
        },
    )
    if not jobs:
        return None
    return [WorkerJobResult(**job) for job in jobs]


def handle_take_job(rclient: Redis, worker: str, job_type: JobType) -> WorkerJob | None:
    if is_worker_blocked(rclient, worker):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="worker is blocked"
        )
    return job_queue(job_type).get(rclient)


def handle_finish_job(
    rclient: Redis, jobs: Collection, worker: str, result: WorkerJobResult
):
    doc = jobs.find_one_and_update(
        {"_id": result.job_id},
        {
            "$set": {
                "finishedAt": int(time.time()),
                "worker": worker,
                "classifyResult": result.classify_result,
                "powResult": result.pow_result,
                "batchClassifyResult": result.batch_classify_result,
            }
        },
    )
    if doc is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="job not found"
        )
    if doc.get("finishedAt", None) is not None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="job already finished",
        )
    job_queue(result.job_type).ack(rclient, str(doc["_id"]))
    requests.post(
        os.environ["BACKEND_SERVICE_URL"] + "/settle_rewards",
        json=jsonable_encoder(
            {"job_id": doc["_id"], "job_type": doc["jobType"], "worker": worker}
        ),
        headers={"Authorization": f"Bearer {os.environ['SHARED_SECRET']}"},
    )


def handle_queue_len(job_type: JobType) -> int:
    return job_queue(job_type).queue_len()
