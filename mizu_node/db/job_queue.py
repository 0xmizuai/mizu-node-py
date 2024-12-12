import asyncio
import logging
import os
import random
from typing import Any, Tuple
from typing import Tuple

from prometheus_client import Gauge
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete, and_, func, Integer

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

JOB_TTL = int(os.environ.get("JOB_TTL", 7 * 24 * 60 * 60))  # 7 days in seconds


def get_random_offset():
    max_concurrent_lease = int(os.environ.get("MAX_CONCURRENT_LEASE", 10))
    return random.randint(0, max_concurrent_lease)


async def lease_job(
    session: AsyncSession,
    job_type: JobType,
    ttl_secs: int,
    worker: str,
) -> Tuple[int | None, int | None, DataJobContext | None]:
    candidate_jobs = (
        select(JobQueue.id, JobQueue.retry, JobQueue.ctx, func.random().label("r"))
        .where(
            and_(JobQueue.job_type == job_type, JobQueue.status == JobStatus.pending)
        )
        .limit(100)
        .cte("candidate_jobs")
    )

    selected_job = (
        select(candidate_jobs.c.id, candidate_jobs.c.retry, candidate_jobs.c.ctx)
        .order_by("r")
        .limit(1)
        .with_for_update(skip_locked=True)
        .cte("selected_job")
    )

    stmt = (
        update(JobQueue)
        .where(JobQueue.id == selected_job.c.id)
        .values(
            status=JobStatus.processing,
            assigned_at=func.extract("epoch", func.now()).cast(Integer),
            lease_expired_at=epoch() + ttl_secs,
            worker=worker,
        )
        .returning(JobQueue.id, selected_job.c.retry, selected_job.c.ctx)
    )

    try:
        result = await session.execute(stmt)
        row = result.first()
        if row:
            await session.commit()
            return (row[0], row[1], DataJobContext.model_validate(row[2]))
    except Exception:
        await session.rollback()

    return None


async def add_jobs(
    session: AsyncSession,
    job_type: JobType,
    publisher: str,
    contexts: list[DataJobContext],
) -> list[int]:
    jobs = [
        JobQueue(
            job_type=job_type,
            ctx=ctx.model_dump(by_alias=True, exclude_none=True),
            publisher=publisher,
        )
        for ctx in contexts
    ]
    session.add_all(jobs)
    await session.flush()
    return [job.id for job in jobs]


async def complete_job(
    session: AsyncSession, item_id: int, status: JobStatus, result: BaseModel
) -> bool:
    stmt = (
        update(JobQueue)
        .where(JobQueue.id == item_id)
        .values(
            status=status,
            result=result.model_dump(by_alias=True, exclude_none=True),
            finished_at=epoch(),
        )
    )
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount > 0


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

    # Delete completed jobs older than JOB_TTL
    stmt = (
        delete(JobQueue)
        .where(
            and_(
                JobQueue.status.in_([JobStatus.finished, JobStatus.error]),
                JobQueue.finished_at < epoch() - JOB_TTL,
            )
        )
        .returning(JobQueue.id)
    )

    result = await session.execute(stmt)
    deleted_rows = result.scalars().all()

    await session.commit()

    if deleted_rows:
        logging.info(f"Deleted {len(deleted_rows)} old completed jobs")


async def clear_jobs(session: AsyncSession, job_type: JobType) -> None:
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
) -> Tuple[DataJobContext | None, str | None]:
    stmt = select(JobQueue.ctx, JobQueue.worker).where(
        and_(
            JobQueue.id == item_id,
            JobQueue.job_type == job_type,
            JobQueue.status == JobStatus.processing,
            JobQueue.lease_expired_at > epoch(),
        )
    )
    result = await session.execute(stmt)
    row = result.first()
    return (DataJobContext.model_validate(row[0]), row[1]) if row else (None, None)


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


async def get_job_info_raw(session: AsyncSession, id: int) -> Any:
    stmt = select(JobQueue).where(JobQueue.id == id)
    result = await session.execute(stmt)
    return result.first()


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
