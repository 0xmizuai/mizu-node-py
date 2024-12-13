import asyncio
from datetime import datetime
import logging
import os
import random
from typing import Tuple
from typing import Tuple

from prometheus_client import Gauge
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, update, delete, and_, func

from mizu_node.common import epoch
from mizu_node.db.orm.job_queue import JobQueue
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import (
    DataJobContext,
    JobStatus,
    JobType,
)
from mizu_node.types.node_service import RewardJobRecord

logging.basicConfig(level=logging.INFO)

JOB_TTL = int(os.environ.get("JOB_TTL", 3 * 24 * 60 * 60))  # 3 days in seconds


def get_random_offset():
    max_concurrent_lease = int(os.environ.get("MAX_CONCURRENT_LEASE", 10))
    return random.randint(0, max_concurrent_lease)


async def lease_job(
    session: AsyncSession,
    job_type: JobType,
    ttl_secs: int,
    worker: str,
) -> Tuple[int, int, DataJobContext] | None:
    current_time = epoch()
    stmt = """
        WITH candidate_jobs AS (
            SELECT id, retry, ctx,
                   random() as r  -- Add randomization
            FROM job_queue
            WHERE job_type = :job_type
            AND status = :status
            LIMIT 100  -- Get a batch of candidates
        ),
        selected_job AS (
            SELECT id, retry, ctx
            FROM candidate_jobs
            ORDER BY r  -- Random order
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE job_queue j
        SET status = :status,
            assigned_at = :current_time,
            lease_expired_at = :lease_expired_at,
            worker = :worker
        FROM selected_job s
        WHERE j.id = s.id
        RETURNING j.id, s.retry, s.ctx
    """

    result = await session.execute(
        text(stmt),
        {
            "worker": worker,
            "lease_expired_at": current_time + ttl_secs,
            "current_time": current_time,
            "job_type": job_type.value,
            "status": JobStatus.processing.value,
        },
    )

    row = result.first()
    if row is None:
        return None

    return (row.id, row.retry, DataJobContext.model_validate(row.ctx))


async def add_jobs(
    session: AsyncSession,
    job_type: JobType,
    contexts: list[DataJobContext],
    reference_id: int | None = None,
) -> list[int]:
    jobs = [
        JobQueue(
            job_type=job_type,
            ctx=ctx.model_dump(by_alias=True, exclude_none=True),
            reference_id=reference_id,
        )
        for ctx in contexts
    ]
    session.add_all(jobs)
    await session.flush()
    return [job.id for job in jobs]


async def update_worker(session: AsyncSession, item_id: int, worker: str):
    stmt = """
        UPDATE job_queue 
        SET worker = :worker 
        WHERE id = :item_id
    """
    await session.execute(
        text(stmt),
        {
            "worker": worker,
            "item_id": item_id,
        },
    )


async def complete_job(
    session: AsyncSession, item_id: int, status: JobStatus, result: BaseModel | None
) -> bool:
    stmt = """
        UPDATE job_queue 
        SET status = :status,
            result = :result,
            finished_at = :finished_at
        WHERE id = :item_id
    """
    result_dict = (
        result.model_dump(by_alias=True, exclude_none=True) if result else None
    )
    db_result = await session.execute(
        text(stmt),
        {
            "status": status.value,
            "result": result_dict,
            "finished_at": epoch(),
            "item_id": item_id,
        },
    )
    return db_result.rowcount > 0


async def light_clean(session: AsyncSession):
    # Reset expired processing jobs back to pending
    stmt = (
        update(JobQueue)
        .where(
            and_(
                JobQueue.status == JobStatus.processing,
                JobQueue.lease_expired_at < epoch(),
            )
        )
        .values(status=JobStatus.pending)
    )
    await session.execute(stmt)

    # Delete completed pow/reward/classify jobs
    stmt = (
        delete(JobQueue)
        .where(
            and_(
                JobQueue.status.in_([JobStatus.finished, JobStatus.error]),
                JobQueue.job_type.in_([JobType.pow, JobType.classify, JobType.reward]),
                JobQueue.finished_at < epoch() - JOB_TTL,
            )
        )
        .returning(JobQueue.id)
    )
    result1 = await session.execute(stmt)
    deleted_rows1 = len(result1.all()) if result1 else 0

    # Delete completed batch_classify jobs with no result
    stmt = (
        delete(JobQueue)
        .where(
            and_(
                JobQueue.status == JobStatus.finished,
                JobQueue.job_type == JobType.batch_classify,
                JobQueue.result.is_(None),
                JobQueue.finished_at < epoch() - JOB_TTL,
            )
        )
        .returning(JobQueue.id)
    )
    result2 = await session.execute(stmt)
    deleted_rows2 = len(result2.all()) if result2 else 0

    deleted_rows = deleted_rows1 + deleted_rows2
    if deleted_rows:
        logging.info(f"Deleted {deleted_rows} old completed jobs")


async def clear_jobs(session: AsyncSession, job_type: JobType):
    stmt = delete(JobQueue).where(JobQueue.job_type == job_type)
    await session.execute(stmt)


async def delete_one_job(session: AsyncSession, item_id: int) -> None:
    stmt = delete(JobQueue).where(JobQueue.id == item_id)
    await session.execute(stmt)


async def get_queue_len(session: AsyncSession, job_type: JobType) -> int:
    stmt = (
        select(func.count())
        .select_from(JobQueue)
        .where(
            and_(JobQueue.job_type == job_type, JobQueue.status == JobStatus.pending)
        )
    )
    result = await session.execute(stmt)
    return result.scalar_one()


async def get_job_lease(
    session: AsyncSession, item_id: int, job_type: JobType
) -> Tuple[DataJobContext | None, list[str]]:
    stmt = select(JobQueue.ctx, JobQueue.worker).where(
        and_(
            JobQueue.id == item_id,
            JobQueue.job_type == job_type,
            JobQueue.status == JobStatus.processing,
            JobQueue.lease_expired_at > epoch(),
        )
    )
    result = await session.execute(stmt)
    job = result.first()
    if job:
        if job.worker:
            return (DataJobContext.model_validate(job.ctx), job.worker.split(","))
        else:
            return (DataJobContext.model_validate(job.ctx), [])
    else:
        return None, []


async def get_reward_jobs_stats(
    session: AsyncSession, worker: str
) -> Tuple[int, int | None]:
    stmt = select(
        func.count().label("job_count"),
        func.max(JobQueue.assigned_at).label("last_assigned_at"),
    ).where(
        and_(
            JobQueue.job_type == JobType.reward,
            JobQueue.status == JobStatus.processing,
            JobQueue.worker == worker,
            JobQueue.lease_expired_at > epoch(),
        )
    )

    result = await session.execute(stmt)
    row = result.first()
    return row[0], row[1]


async def get_assigned_reward_jobs(
    session: AsyncSession, worker: str
) -> list[RewardJobRecord]:
    stmt = (
        select(
            JobQueue.id, JobQueue.assigned_at, JobQueue.lease_expired_at, JobQueue.ctx
        )
        .where(
            and_(
                JobQueue.job_type == JobType.reward,
                JobQueue.status == JobStatus.processing,
                JobQueue.worker == worker,
                JobQueue.lease_expired_at > epoch(),
            )
        )
        .order_by(JobQueue.lease_expired_at.asc())
    )

    result = await session.execute(stmt)
    rows = result.all()

    return [
        RewardJobRecord(
            job_id=row[0],
            assigned_at=row[1],
            lease_expired_at=row[2],
            reward_ctx=DataJobContext.model_validate(row[3]).reward_ctx,
        )
        for row in rows
    ]


async def get_job_info(session: AsyncSession, id: int) -> JobQueue:
    stmt = select(JobQueue).where(JobQueue.id == id)
    result = await session.execute(stmt)
    row = result.first()
    return row[0] if row else None


ALL_JOB_TYPES = [JobType.pow, JobType.classify, JobType.batch_classify, JobType.reward]

QUEUE_LEN = Gauge(
    "app_job_queue_len",
    "the queue length of each job_type",
    ["job_type"],
)


async def queue_clean(connections: Connections):
    while True:
        async with connections.get_job_db_session() as session:
            for job_type in ALL_JOB_TYPES:
                length = await get_queue_len(session, job_type)
                QUEUE_LEN.labels(job_type.name).set(length)
            try:
                logging.info("light clean start for queue")
                await light_clean(session)
                logging.info("light clean done for queue")
            except Exception as e:
                logging.error(f"failed to clean queue with error {e}")
                continue
            await asyncio.sleep(int(os.environ.get("QUEUE_CLEAN_INTERVAL", 300)))
