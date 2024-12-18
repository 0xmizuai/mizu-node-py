import logging
from typing import Tuple
from typing import Tuple

from psycopg2 import sql
from pydantic import BaseModel
from psycopg2.extensions import connection
from redis import Redis

from mizu_node.common import epoch
from mizu_node.config import get_lease_ttl
from mizu_node.db.common import with_transaction
from mizu_node.types.data_job import (
    DataJobContext,
    JobStatus,
    JobType,
)
from mizu_node.types.service import RewardJobRecord


logging.basicConfig(level=logging.INFO)  # Set the desired logging level


def job_queue_cache_key(job_type: JobType, reference_id: int | None) -> str:
    if reference_id is None or reference_id == 0:
        return f"job_cache_v2:{job_type.name}"  # backward compatibility
    else:
        return f"job_cache_v2:{job_type.name}:{reference_id}"


@with_transaction
def lease_job(
    db: connection,
    redis: Redis,
    job_type: JobType,
    reference_id: int,
    worker: str,
) -> Tuple[int, int, DataJobContext] | None:
    """Optimized job leasing with less contention."""
    id = redis.rpop(job_queue_cache_key(job_type, reference_id))
    if id is None:
        return None

    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """UPDATE job_queue
                SET
                    status = %s,
                    assigned_at = %s,
                    lease_expired_at = %s,
                    worker = %s
                WHERE id = %s
                RETURNING id, retry, ctx"""
            ),
            (
                JobStatus.processing,
                epoch(),
                epoch() + get_lease_ttl(job_type),
                worker,
                id,
            ),
        )

        row = cur.fetchone()
        if row is None:
            return None

        item_id, retry, ctx = row
        return (item_id, retry, DataJobContext.model_validate(ctx))


@with_transaction
def add_jobs(
    db: connection,
    job_type: JobType,
    contexts: list[BaseModel],
    reference_id: int = 0,
) -> list[int]:
    with db.cursor() as cur:
        inserted_ids = []
        for ctx in contexts:
            cur.execute(
                sql.SQL(
                    """
                        INSERT INTO job_queue (job_type, ctx, reference_id) 
                        VALUES (%s, %s::jsonb, %s)
                        RETURNING id
                        """
                ),
                (
                    job_type,
                    ctx.model_dump_json(by_alias=True, exclude_none=True),
                    reference_id,
                ),
            )
            inserted_ids.append(cur.fetchone()[0])
        return inserted_ids


@with_transaction
def complete_job(
    db: connection, item_id: int, status: JobStatus, result: BaseModel
) -> bool:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """UPDATE job_queue
                SET status = %s, result = %s::jsonb
                WHERE id = %s
                """
            ),
            (
                status,
                result.model_dump_json(by_alias=True, exclude_none=True),
                item_id,
            ),
        )
        return cur.rowcount > 0


@with_transaction
def clear_jobs(
    db: connection, job_type: JobType, reference_ids: list[int] = []
) -> None:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                "DELETE FROM job_queue WHERE job_type = %s"
                + (
                    f" AND reference_id IN ({', '.join(map(str, reference_ids))})"
                    if reference_ids
                    else ""
                )
            ),
            (job_type,),
        )


@with_transaction
def delete_one_job(db: connection, item_id: int) -> None:
    with db.cursor() as cur:
        cur.execute(sql.SQL("DELETE FROM job_queue WHERE id = %s"), (item_id,))


@with_transaction
def get_num_of_jobs(
    db: connection,
    job_type: JobType,
    status: JobStatus,
    reference_ids: list[int] = [],
) -> int:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT COUNT(*) FROM job_queue WHERE job_type = %s AND status = %s"
                + (
                    f" AND reference_id IN ({', '.join(map(str, reference_ids))})"
                    if reference_ids
                    else ""
                ),
            ),
            (job_type, status),
        )
        return cur.fetchone()[0]


def get_queue_len(
    db: connection, job_type: JobType, reference_ids: list[int] = []
) -> int:
    cached = get_num_of_jobs(db, job_type, JobStatus.cached, reference_ids)
    pending = get_num_of_jobs(db, job_type, JobStatus.pending, reference_ids)
    logging.info(
        f"{job_type.name}: cached: {cached}, db pending: {pending}, total: {cached + pending}"
    )
    return cached + pending


@with_transaction
def requeue_job(db: connection, item_id: int) -> None:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """UPDATE job_queue
                SET status = %s
                WHERE id = %s
            """
            ),
            (JobStatus.pending, item_id),
        )


@with_transaction
def get_job_lease(
    db: connection, item_id: int
) -> Tuple[DataJobContext | None, str | None]:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT ctx, worker
                FROM job_queue 
                WHERE id = %s 
                AND status = %s
                AND lease_expired_at > EXTRACT(EPOCH FROM NOW())::BIGINT
            """
            ),
            (
                item_id,
                JobStatus.processing,
            ),
        )
        row = cur.fetchone()
        return (DataJobContext.model_validate(row[0]), row[1]) if row else (None, [])


@with_transaction
def get_reward_jobs_stats(db: connection, worker: str) -> Tuple[int, int | None]:
    """Get count and last assigned time of rewarding jobs for a worker.

    Returns:
        Tuple of (count, last_assigned_at)
        last_assigned_at will be None if no jobs exist
    """
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT
                    COUNT(*) as job_count,
                    MAX(assigned_at) as last_assigned_at
                FROM job_queue
                WHERE job_type = %s
                AND status = %s
                AND worker = %s
                AND lease_expired_at > EXTRACT(EPOCH FROM NOW())::BIGINT
                """
            ),
            (JobType.reward, JobStatus.processing, worker),
        )
        count, last_assigned = cur.fetchone()
        return count, last_assigned


@with_transaction
def get_assigned_reward_jobs(db: connection, worker: str) -> list[RewardJobRecord]:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT id, assigned_at, lease_expired_at, ctx
                FROM job_queue
                WHERE job_type = %s
                AND status = %s
                AND worker = %s
                AND lease_expired_at > EXTRACT(EPOCH FROM NOW())::BIGINT
                ORDER BY lease_expired_at ASC
                """
            ),
            (JobType.reward, JobStatus.processing, worker),
        )
        return [
            RewardJobRecord(
                job_id=row[0],
                assigned_at=row[1],
                lease_expired_at=row[2],
                reward_ctx=DataJobContext.model_validate(row[3]).reward_ctx,
            )
            for row in cur.fetchall()
        ]


@with_transaction
def get_job_info(db: connection, id: int) -> dict:
    """Get job information as a dictionary.

    Returns a dictionary containing specific job fields from the job_queue table.
    Returns None if job is not found.
    """
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT
                    id,
                    job_type,
                    status,
                    ctx,
                    publisher,
                    published_at,
                    assigned_at,
                    lease_expired_at,
                    result,
                    finished_at,
                    reference_id,
                    worker,
                    retry
                FROM job_queue
                WHERE id = %s
            """
            ),
            (id,),
        )
        row = cur.fetchone()
        if row is None:
            return None

        return {
            "id": row[0],
            "job_type": row[1],
            "status": row[2],
            "ctx": row[3],
            "publisher": row[4],
            "published_at": row[5],
            "assigned_at": row[6],
            "lease_expired_at": row[7],
            "result": row[8],
            "finished_at": row[9],
            "reference_id": row[10],
            "worker": row[11],
            "retry": row[12],
        }
