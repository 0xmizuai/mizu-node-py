from hashlib import sha512
import logging
import os
from typing import Iterator

from bson import ObjectId
from prometheus_client import Histogram
from pymongo.database import Database, Collection
from redis import Redis
from fastapi import HTTPException, status
import requests


from mizu_node.common import epoch, epoch_ms
from mizu_node.constants import (
    CLASSIFIER_COLLECTION,
    DEFAULT_POW_DIFFICULTY,
    JOBS_COLLECTION,
    LATENCY_BUCKETS,
    MAX_RETRY_ALLOWED,
    MIZU_ADMIN_USER,
)
from mizu_node.security import (
    get_lease_ttl,
    validate_worker,
)
from mizu_node.stats import (
    record_claim_event,
    record_mined_points,
    record_reward_event,
    total_mined_points_in_past_n_hour_per_worker,
    try_remove_reward_record,
)
from mizu_node.types.job_queue_legacy import job_queue_legacy
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import (
    BatchClassifyContext,
    ClassifyContext,
    DataJobInputNoId,
    DataJobQueryResult,
    DataJobResultNoId,
    ErrorCode,
    ErrorResult,
    JobStatus,
    JobType,
    PowContext,
    RewardContext,
    WorkerJob,
    WorkerJobResult,
)
from mizu_node.types.job_queue import job_queue
from mizu_node.types.service import SettleRewardRequest

logging.basicConfig(level=logging.INFO)  # Set the desired logging level


def handle_publish_jobs(
    conn: Connections,
    publisher: str,
    job_type: JobType,
    contexts: (
        list[PowContext]
        | list[RewardContext]
        | list[BatchClassifyContext]
        | list[ClassifyContext]
    ),
) -> Iterator[str]:
    if len(contexts) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="data cannot be empty"
        )

    jobs = [
        DataJobInputNoId(
            job_type=job_type,
            classify_ctx=ctx if job_type == JobType.classify else None,
            pow_ctx=ctx if job_type == JobType.pow else None,
            reward_ctx=ctx if job_type == JobType.reward else None,
            batch_classify_ctx=ctx if job_type == JobType.batch_classify else None,
            publisher=publisher,
            status=JobStatus.pending,
            published_at=epoch(),
        ).model_dump(by_alias=True, exclude_none=True)
        for ctx in contexts
    ]
    result = conn.mdb[JOBS_COLLECTION].insert_many(jobs)
    ids = [str(id) for id in result.inserted_ids]
    worker_jobs = [
        WorkerJob(
            job_id=id,
            job_type=job_type,
            classify_ctx=ctx if job_type == JobType.classify else None,
            pow_ctx=ctx if job_type == JobType.pow else None,
            reward_ctx=ctx if job_type == JobType.reward else None,
            batch_classify_ctx=ctx if job_type == JobType.batch_classify else None,
        ).model_dump_json(exclude_none=True)
        for id, ctx in zip(ids, contexts)
    ]
    job_queue(job_type).add_items(conn.postgres, ids, worker_jobs)
    return ids


def handle_query_job(
    mdb: Collection, job_ids: list[str]
) -> list[DataJobQueryResult] | None:
    job_ids = [ObjectId(id) for id in job_ids]
    if not job_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="job_ids is required"
        )
    jobs = list(mdb.find({"_id": {"$in": job_ids[0:1000]}}))
    if not jobs:
        return None
    return [
        DataJobQueryResult.model_validate({**job, "_id": str(job["_id"])})
        for job in jobs
    ]


HANDLE_TAKE_JOB_LATENCY = Histogram(
    "handle_take_job_latency_ms",
    "Detailed latency of handle_take_job function",
    ["job_type", "step"],
    buckets=LATENCY_BUCKETS,
)


def handle_take_job(
    conn: Connections, worker: str, job_type: JobType
) -> WorkerJob | None:
    start_time = epoch_ms()
    validate_worker(conn.redis, worker, job_type)
    HANDLE_TAKE_JOB_LATENCY.labels(job_type.name, "validate").observe(
        epoch_ms() - start_time
    )
    after_validation = epoch_ms()
    result = job_queue(job_type).lease(conn.postgres, get_lease_ttl(job_type), worker)
    HANDLE_TAKE_JOB_LATENCY.labels(job_type.name, "lease").observe(
        epoch_ms() - after_validation
    )
    if result is None:
        return None

    (item_id, retry, job) = result
    parsed = WorkerJob.model_validate_json(job)
    if retry > MAX_RETRY_ALLOWED:
        try:
            handle_finish_job(
                conn,
                worker,
                WorkerJobResult(
                    job_id=item_id,
                    job_type=job_type,
                    error_result=ErrorResult(code=ErrorCode.max_retry_exceeded),
                ),
            )
        except HTTPException as e:
            logging.warning(f"failed to retire job {item_id} with error {e.detail}")
            pass
        return handle_take_job(conn, worker, job_type)
    else:
        if job_type == JobType.reward:
            record_reward_event(conn.redis, worker, parsed)
        return parsed


HANDLE_FINISH_JOB_LATENCY = Histogram(
    "handle_finish_job_latency_ms",
    "Detailed latency of handle_finish_job function",
    ["job_type", "step"],
    buckets=LATENCY_BUCKETS,
)


def get_legacy_leaser(conn: Connections, job_type: JobType, job_id: str) -> str | None:
    if epoch() - 3600 * 12 > int(os.environ.get("MIGRATION_START_TIME", 0)):
        return None
    return job_queue_legacy(job_type).get_lease(conn.redis, job_id)


def handle_finish_job(
    conn: Connections, worker: str, job_result: WorkerJobResult
) -> float:
    reward_points = 0
    start_time = epoch_ms()
    job_type = job_result.job_type
    try:
        if job_queue(job_type).get_lease(conn.postgres, job_result.job_id) != worker:
            if get_legacy_leaser(conn, job_type, job_result.job_id) != worker:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="lease not exists"
                )
        doc = conn.mdb[JOBS_COLLECTION].find_one({"_id": ObjectId(job_result.job_id)})
        if doc is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="job not found"
            )
        parsed = DataJobInputNoId.model_validate(doc)
        job_status = _validate_job_result(parsed, job_result)
        HANDLE_FINISH_JOB_LATENCY.labels(job_type.name, "validate").observe(
            epoch_ms() - start_time
        )
        after_validation = epoch_ms()
        if job_status == JobStatus.finished:
            settle_reward = _calculate_reward(conn.redis, worker, parsed, job_result)
            response = requests.post(
                os.environ["BACKEND_SERVICE_URL"] + "/api/settle_reward",
                json=settle_reward.model_dump(exclude_none=True),
                headers={"x-api-secret": os.environ["API_SECRET_KEY"]},
            )
            if response.status_code != 200:
                logging.warning(
                    "failed to call settle_reward: code=%d, msg=%s, input=%s",
                    response.status_code,
                    response.text,
                    settle_reward.model_dump(),
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"failed to settle reward",
                )
            if settle_reward.token is None:
                reward_points = float(settle_reward.amount)
            HANDLE_FINISH_JOB_LATENCY.labels(job_type.name, "settle").observe(
                epoch_ms() - after_validation
            )

        after_settle_reward = epoch_ms()
        if job_type == JobType.reward:
            record_claim_event(conn.redis, worker, job_result.job_id, parsed.reward_ctx)
        elif reward_points > 0:
            record_mined_points(conn.redis, worker, reward_points)
        HANDLE_FINISH_JOB_LATENCY.labels(job_type.name, "record").observe(
            epoch_ms() - after_settle_reward
        )

        after_record = epoch_ms()
        conn.mdb[JOBS_COLLECTION].update_one(
            {"_id": ObjectId(job_result.job_id)},
            {
                "$set": DataJobResultNoId(
                    worker=worker,
                    finished_at=epoch(),
                    status=job_status,
                    **job_result.model_dump(by_alias=True, exclude=set(["job_id"])),
                ).model_dump(by_alias=True),
            },
        )
        job_queue(job_type).complete(conn.postgres, job_result.job_id)
        HANDLE_FINISH_JOB_LATENCY.labels(job_type.name, "execute").observe(
            epoch_ms() - after_record
        )
        return reward_points
    except HTTPException as e:
        if job_type == JobType.reward and e.status_code == status.HTTP_404_NOT_FOUND:
            try_remove_reward_record(conn.redis, worker, job_result.job_id)
        raise e


def handle_queue_len(conn: Connections, job_type: JobType) -> int:
    return job_queue(job_type).queue_len(conn.postgres)


def validate_admin_job(publisher: str):
    if publisher != MIZU_ADMIN_USER:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized"
        )


def validate_classifiers(mdb: Database, contexts: list[BatchClassifyContext]):
    cids = list(set([ctx.classifier_id for ctx in contexts]))
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


def _validate_job_result(job: DataJobInputNoId, result: WorkerJobResult) -> JobStatus:
    if job.status != JobStatus.pending:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="job already finished",
        )

    if job.job_type != result.job_type:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="job type mismatch"
        )

    if result.error_result is not None:
        return JobStatus.error

    if result.job_type == JobType.pow:
        hash_output = sha512(
            (job.pow_ctx.seed + result.pow_result).encode("utf-8")
        ).hexdigest()
        if not all(b == "0" for b in hash_output[:DEFAULT_POW_DIFFICULTY]):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="invalid pow_result: hash does not meet difficulty requirement",
            )
    elif result.job_type == JobType.batch_classify:
        result.batch_classify_result = [
            result for result in result.batch_classify_result if len(result.labels) > 0
        ]
    return JobStatus.finished


def _calculate_reward(
    rclient: Redis, worker: str, job: DataJobInputNoId, result: WorkerJobResult
) -> SettleRewardRequest:
    if job.job_type == JobType.reward:
        return SettleRewardRequest(
            job_id=result.job_id,
            job_type=job.job_type,
            worker=worker,
            token=job.reward_ctx.token,
            amount=str(job.reward_ctx.amount),
            recipient=result.reward_result.recipient,
        )

    past_24h_points = total_mined_points_in_past_n_hour_per_worker(rclient, worker, 24)
    if past_24h_points < 2500:
        factor = 1
    elif past_24h_points < 5000:
        factor = (5000 - past_24h_points) / 5000
    else:
        factor = 0
    return SettleRewardRequest(
        job_id=result.job_id,
        job_type=job.job_type,
        worker=worker,
        amount=str(0.5 * factor),
    )
