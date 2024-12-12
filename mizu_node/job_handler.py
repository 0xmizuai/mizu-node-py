from hashlib import sha512
import logging
import os

from redis.asyncio import Redis
from fastapi import HTTPException, status
import requests


from mizu_node.constants import (
    DEFAULT_POW_DIFFICULTY,
    MAX_RETRY_ALLOWED,
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
)
from mizu_node.types.node_service import SettleRewardRequest

logging.basicConfig(level=logging.INFO)


async def handle_take_job(
    conn: Connections, worker: str, job_type: JobType
) -> WorkerJob | None:
    async with conn.get_job_db_session() as session:
        await validate_worker(conn.redis, session, worker, job_type)
        result = await lease_job(session, job_type, get_lease_ttl(job_type), worker)
        if result is None:
            return None

        (item_id, retry, ctx) = result
        if retry > MAX_RETRY_ALLOWED:
            try:
                await complete_job(
                    session,
                    item_id,
                    JobStatus.error,
                    DataJobResult(
                        error_result=ErrorResult(code=ErrorCode.max_retry_exceeded)
                    ),
                )
            except HTTPException as e:
                logging.warning(f"failed to retire job {item_id} with error {e.detail}")
                pass
            return await handle_take_job(conn, worker, job_type)
        else:
            job = WorkerJob(
                job_id=item_id,
                job_type=job_type,
                **ctx.model_dump(exclude_none=True),
            )
            return job


async def handle_finish_job_v2(
    conn: Connections, worker: str, job_result: WorkerJobResult
) -> SettleRewardRequest | None:
    async with conn.get_job_db_session() as session:
        job_id = int(job_result.job_id)
        ctx, assigner = await get_job_lease(session, job_id, job_result.job_type)
        if assigner != worker:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="lease not exists"
            )
        job_status = _validate_job_result(ctx, job_result)
        await complete_job(
            session, job_result.job_id, job_status, build_data_job_result(job_result)
        )
        if job_result.job_type == JobType.batch_classify:
            api_key = os.environ["API_SECRET_KEY"]
            requests.post(
                os.environ["WORKFLOW_SERVER_URL"] + "/save_query_result",
                json=job_result.model_dump(exclude_none=True, by_alias=True),
                headers={"Authorization": f"Bearer {api_key}"},
            )
        return (
            await _calculate_reward_v2(conn.redis, worker, ctx, job_result)
            if job_status == JobStatus.finished
            else None
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


async def _calculate_reward_v2(
    rclient: Redis, worker: str, ctx: DataJobContext, result: WorkerJobResult
) -> SettleRewardRequest:
    if result.job_type == JobType.reward:
        await record_claim_event(rclient, ctx.reward_ctx)
        return SettleRewardRequest(
            job_id=result.job_id,
            job_type=result.job_type,
            worker=worker,
            token=ctx.reward_ctx.token,
            amount=str(ctx.reward_ctx.amount),
            recipient=result.reward_result.recipient,
        )

    past_24h_points = await total_mined_points_in_past_n_hour_per_worker(
        rclient, worker, 24
    )
    if past_24h_points < 2500:
        factor = 1
    elif past_24h_points < 5000:
        factor = (5000 - past_24h_points) / 5000
    else:
        factor = 0
    rewarded = 0.5 * factor
    await record_mined_points(rclient, worker, rewarded)
    return SettleRewardRequest(
        job_id=result.job_id,
        job_type=result.job_type,
        worker=worker,
        amount=str(rewarded),
    )
