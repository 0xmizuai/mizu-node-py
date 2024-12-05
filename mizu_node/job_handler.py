from hashlib import sha512
import logging
import os

from prometheus_client import Counter, Histogram
from pydantic import BaseModel
from redis import Redis
from fastapi import HTTPException, status
import requests


from mizu_node.db.classifier import list_configs
from mizu_node.common import epoch_ms
from mizu_node.constants import (
    DEFAULT_POW_DIFFICULTY,
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
    total_mined_points_in_past_n_hour_per_worker,
)
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import (
    BatchClassifyContext,
    DataJobContext,
    DataJobResult,
    ErrorCode,
    ErrorResult,
    JobStatus,
    JobType,
    WorkerJob,
    WorkerJobResult,
)
from mizu_node.db.job_queue import (
    add_jobs,
    complete_job,
    get_jobs_info,
    get_job_lease,
    lease_job,
    queue_len,
)
from mizu_node.types.service import DataJobQueryResult, SettleRewardRequest
from psycopg2.extensions import connection

logging.basicConfig(level=logging.INFO)  # Set the desired logging level


def build_data_job_context(job_type: JobType, ctx: BaseModel) -> DataJobContext:
    if job_type == JobType.reward:
        return DataJobContext(reward_ctx=ctx)
    elif job_type == JobType.classify:
        return DataJobContext(classify_ctx=ctx)
    elif job_type == JobType.pow:
        return DataJobContext(pow_ctx=ctx)
    elif job_type == JobType.batch_classify:
        return DataJobContext(batch_classify_ctx=ctx)


def handle_publish_jobs(
    pg_conn: connection,
    publisher: str,
    job_type: JobType,
    contexts: list[BaseModel],
) -> list[int]:
    if len(contexts) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="data cannot be empty"
        )
    return add_jobs(
        pg_conn,
        job_type,
        publisher,
        [build_data_job_context(job_type, ctx) for ctx in contexts],
    )


def handle_query_job(
    pg_conn: connection, job_ids: list[int]
) -> list[DataJobQueryResult] | None:
    if not job_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="job_ids is required"
        )
    job_ids = [int(id) for id in job_ids]
    return get_jobs_info(pg_conn, job_ids)


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
    with conn.get_pg_connection() as pg_conn:
        validate_worker(conn.redis, pg_conn, worker, job_type)
        HANDLE_TAKE_JOB_LATENCY.labels(job_type.name, "validate").observe(
            epoch_ms() - start_time
        )
        after_validation = epoch_ms()
        result = lease_job(pg_conn, job_type, get_lease_ttl(job_type), worker)
        HANDLE_TAKE_JOB_LATENCY.labels(job_type.name, "lease").observe(
            epoch_ms() - after_validation
        )
        if result is None:
            return None

        (item_id, retry, ctx) = result
        if retry > MAX_RETRY_ALLOWED:
            try:
                complete_job(
                    pg_conn,
                    item_id,
                    JobStatus.error,
                    DataJobResult(
                        error_result=ErrorResult(code=ErrorCode.max_retry_exceeded)
                    ),
                )
            except HTTPException as e:
                logging.warning(f"failed to retire job {item_id} with error {e.detail}")
                pass
            return handle_take_job(conn, worker, job_type)
        else:
            job = WorkerJob(
                job_id=item_id,
                job_type=job_type,
                **ctx.model_dump(exclude_none=True),
            )
            return job


HANDLE_FINISH_JOB_LATENCY = Histogram(
    "handle_finish_job_latency_ms",
    "Detailed latency of handle_finish_job function",
    ["job_type", "step"],
    buckets=LATENCY_BUCKETS,
)

HANDLE_FINISH_JOB_404_COUNTER = Counter(
    "handle_finish_job_404",
    "total requests of handle_finish_job 404 cases",
)


def build_data_job_result(job_result: WorkerJobResult) -> DataJobResult:
    if job_result.error_result is not None:
        return DataJobResult(error_result=job_result.error_result)
    if job_result.job_type == JobType.reward:
        return DataJobResult(reward_result=job_result.reward_result)
    elif job_result.job_type == JobType.pow:
        return DataJobResult(pow_result=job_result.pow_result)
    elif job_result.job_type == JobType.batch_classify:
        return DataJobResult(batch_classify_result=job_result.batch_classify_result)
    raise ValueError(f"unsupported job type: {job_result.job_type}")


def handle_finish_job(
    conn: Connections, worker: str, job_result: WorkerJobResult
) -> float:
    reward_points = 0
    start_time = epoch_ms()
    job_type = job_result.job_type
    with conn.get_pg_connection() as pg_conn:
        job_id = int(job_result.job_id)
        ctx, assigner = get_job_lease(pg_conn, job_id, job_type)
        if assigner != worker:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="lease not exists"
            )
        job_status = _validate_job_result(ctx, job_result)
        HANDLE_FINISH_JOB_LATENCY.labels(job_type.name, "validate").observe(
            epoch_ms() - start_time
        )
        after_validation = epoch_ms()
        if job_status == JobStatus.finished:
            settle_reward = _calculate_reward(conn.redis, worker, ctx, job_result)
            response = requests.post(
                os.environ["BACKEND_SERVICE_URL"] + "/api/settle_reward",
                json=settle_reward.model_dump(exclude_none=True),
                headers={"x-api-secret": os.environ["API_SECRET_KEY"]},
            )
            if response.status_code != 200:
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
            record_claim_event(conn.redis, ctx.reward_ctx)
        elif reward_points > 0:
            record_mined_points(conn.redis, worker, reward_points)
        HANDLE_FINISH_JOB_LATENCY.labels(job_type.name, "record").observe(
            epoch_ms() - after_settle_reward
        )

        after_record = epoch_ms()
        complete_job(
            pg_conn, job_result.job_id, job_status, build_data_job_result(job_result)
        )
        HANDLE_FINISH_JOB_LATENCY.labels(job_type.name, "execute").observe(
            epoch_ms() - after_record
        )
    return reward_points


def handle_queue_len(pg_conn: connection, job_type: JobType) -> int:
    return queue_len(pg_conn, job_type)


def validate_admin_job(publisher: str):
    if publisher != MIZU_ADMIN_USER:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized"
        )


def validate_classifiers(pg_conn: connection, contexts: list[BatchClassifyContext]):
    cids = list(set([ctx.classifier_id for ctx in contexts]))
    configs = list_configs(pg_conn, cids)
    if len(configs) != len(cids):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"got invalid classifier ids",
        )


def _validate_job_result(ctx: DataJobContext, result: WorkerJobResult) -> JobStatus:
    if result.error_result is not None:
        return JobStatus.error

    if result.job_type == JobType.pow:
        if os.environ.get("POW_VALIDATION_ENABLED", "true") == "true":
            hash_output = sha512(
                (ctx.pow_ctx.seed + result.pow_result).encode("utf-8")
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
    rclient: Redis, worker: str, ctx: DataJobContext, result: WorkerJobResult
) -> SettleRewardRequest:
    if result.job_type == JobType.reward:
        return SettleRewardRequest(
            job_id=result.job_id,
            job_type=result.job_type,
            worker=worker,
            token=ctx.reward_ctx.token,
            amount=str(ctx.reward_ctx.amount),
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
        job_type=result.job_type,
        worker=worker,
        amount=str(0.5 * factor),
    )
