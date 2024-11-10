from hashlib import sha512
import os
import time
from typing import Iterator

from bson import ObjectId
from pymongo.database import Database, Collection
from redis import Redis
from fastapi import HTTPException, status
import requests


from mizu_node.constants import (
    ASSIGNED_JOB_EXPIRE_TTL_SECONDS,
    CLASSIFIER_COLLECTION,
    DEFAULT_POW_DIFFICULTY,
    JOBS_COLLECTION,
)
from mizu_node.security import is_worker_blocked
from mizu_node.types.job import (
    DataJobPayload,
    JobType,
    PowContext,
    PublishJobRequest,
    QueryJobRequest,
    WorkerJob,
    WorkerJobResult,
)
from mizu_node.types.job_queue import job_queue


def handle_publish_jobs(
    rclient: Redis, mdb: Database, publisher: str, req: PublishJobRequest
) -> Iterator[str]:
    _validate_classifiers(mdb, req.data)
    # Convert Pydantic objects to dictionaries before inserting
    documents = [
        DataJobPayload(
            job_type=payload.job_type,
            classify_ctx=payload.classify_ctx,
            pow_ctx=payload.pow_ctx,
            batch_classify_ctx=payload.batch_classify_ctx,
            published_at=int(time.time()),
            publisher=publisher,
        ).model_dump(by_alias=True)
        for payload in req.data
    ]
    result = mdb[JOBS_COLLECTION].insert_many(documents)
    for job_id, payload in zip(result.inserted_ids, req.data):
        worker_job = WorkerJob(
            job_id=str(job_id),
            job_type=payload.job_type,
            classify_ctx=payload.classify_ctx,
            pow_ctx=payload.pow_ctx,
            batch_classify_ctx=payload.batch_classify_ctx,
        )
        job_queue(payload.job_type).add_item(
            rclient, worker_job.job_id, worker_job.model_dump_json(by_alias=True)
        )
    return [str(id) for id in result.inserted_ids]


def handle_query_job(
    mdb: Collection, req: QueryJobRequest
) -> list[WorkerJobResult] | None:
    job_ids = [ObjectId(id) for id in req.job_ids]
    if not job_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="job_ids is required"
        )
    jobs = list(mdb.find({"_id": {"$in": job_ids[0:1000]}}))
    if not jobs:
        return None
    return [
        WorkerJobResult.model_validate({**job, "_id": str(job["_id"])}) for job in jobs
    ]


def handle_take_job(rclient: Redis, worker: str, job_type: JobType) -> WorkerJob | None:
    if is_worker_blocked(rclient, worker):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="worker is blocked"
        )
    job = job_queue(job_type).lease(rclient, ASSIGNED_JOB_EXPIRE_TTL_SECONDS)
    return WorkerJob.model_validate_json(job) if job else None


def handle_finish_job(
    rclient: Redis, jobs: Collection, user: str, job_result: WorkerJobResult
):
    update_data = _validate_job_result(jobs, job_result)
    job_queue(job_result.job_type).complete(rclient, job_result.job_id)
    jobs.update_one(
        {"_id": ObjectId(job_result.job_id)},
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


def handle_queue_len(rclient: Redis, job_type: JobType) -> int:
    return job_queue(job_type).queue_len(rclient)


def _validate_classifiers(mdb: Database, jobs: list[DataJobPayload]):
    cids = list(
        set(
            [
                job.batch_classify_ctx.classifer_id
                for job in jobs
                if job.job_type == JobType.batch_classify
            ]
        )
    )
    for cid in cids:
        if not ObjectId.is_valid(cid):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"invalid classifier {cid}",
            )

    docs = list(
        mdb[CLASSIFIER_COLLECTION].find(
            {"_id": {"$in": [ObjectId(cid) for cid in cids]}}, {"_id": 1}
        )
    )
    find = set(str(doc["_id"]) for doc in docs)
    missing = [cid for cid in cids if cid not in find]
    if len(missing) > 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"classifier {','.join(missing)} not found",
        )


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
    doc = jobs.find_one({"_id": ObjectId(result.job_id)})
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
