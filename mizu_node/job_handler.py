from hashlib import sha512
import logging
import os

from prometheus_client import Histogram
from redis import Redis
from fastapi import HTTPException, status


from mizu_node.common import epoch_ms
from mizu_node.constants import (
    DEFAULT_POW_DIFFICULTY,
    LATENCY_BUCKETS,
    MAX_RETRY_ALLOWED,
)
from mizu_node.security import (
    validate_worker,
)
from mizu_node.stats import (
    record_claim_event,
    record_mined_points,
    total_mined_points_in_past_n_hour_per_worker,
)
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import (
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
    complete_job,
    get_job_lease,
    lease_job,
    update_job_worker,
)
from mizu_node.types.service import SettleRewardRequest

logging.basicConfig(level=logging.INFO)  # Set the desired logging level


HANDLE_TAKE_JOB_LATENCY = Histogram(
    "handle_take_job_latency_ms",
    "Detailed latency of handle_take_job function",
    ["job_type", "step"],
    buckets=LATENCY_BUCKETS,
)


def handle_take_job(
    conn: Connections, worker: str, job_type: JobType, reference_id: int
) -> WorkerJob | None:
    start_time = epoch_ms()
    with conn.get_pg_connection() as pg_conn:
        validate_worker(conn.redis, pg_conn, worker, job_type)
        HANDLE_TAKE_JOB_LATENCY.labels(job_type.name, "validate").observe(
            epoch_ms() - start_time
        )
        after_validation = epoch_ms()
        result = lease_job(pg_conn, conn.redis, job_type, reference_id or 0, worker)
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
            return None
        else:
            job = WorkerJob(
                job_id=item_id,
                job_type=job_type,
                reference_id=reference_id,
                **ctx.model_dump(exclude_none=True),
            )
            return job


HANDLE_FINISH_JOB_LATENCY = Histogram(
    "handle_finish_job_latency_ms",
    "Detailed latency of handle_finish_job function",
    ["job_type", "step"],
    buckets=LATENCY_BUCKETS,
)


def handle_finish_job_v2(
    conn: Connections, worker: str, job_result: WorkerJobResult
) -> SettleRewardRequest | None:
    with conn.get_pg_connection() as pg_conn:
        job_id = int(job_result.job_id)
        ctx, assigners = get_job_lease(pg_conn, job_id)
        if worker not in assigners:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="lease not exists"
            )
        job_status = _validate_job_result(ctx, job_result)
        data_job_result = DataJobResult(
            **job_result.model_dump(exclude={"job_id", "job_type"})
        )
        assigners.remove(worker)
        if assigners:
            update_job_worker(pg_conn, job_id, assigners)
        else:
            complete_job(pg_conn, job_id, job_status, data_job_result)
        return (
            _calculate_reward_v2(conn.redis, worker, ctx, job_result)
            if job_status == JobStatus.finished
            else None
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
            result
            for result in result.batch_classify_result
            if result.uri and result.text
        ]
    return JobStatus.finished


def _calculate_reward_v2(
    rclient: Redis, worker: str, ctx: DataJobContext, result: WorkerJobResult
) -> SettleRewardRequest:
    if result.job_type == JobType.reward:
        record_claim_event(rclient, ctx.reward_ctx)
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
    rewarded = 0.5 * factor
    record_mined_points(rclient, worker, rewarded)
    return SettleRewardRequest(
        job_id=result.job_id,
        job_type=result.job_type,
        worker=worker,
        amount=str(rewarded),
        mining_points=rewarded,
    )
