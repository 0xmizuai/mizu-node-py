import logging
import os
import time
from typing import Tuple
from typing import Tuple

from prometheus_client import Gauge
from psycopg2 import sql
from pydantic import BaseModel
from psycopg2.extensions import connection
from redis import Redis

from mizu_node.common import epoch
from mizu_node.constants import REWARD_TTL
from mizu_node.db.common import with_transaction
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import (
    DataJobContext,
    JobStatus,
    JobType,
)
from mizu_node.types.service import RewardJobRecord


logging.basicConfig(level=logging.INFO)  # Set the desired logging level


def job_queue_cache_key(job_type: JobType) -> str:
    return f"job_cache_v2:{job_type.name}"


@with_transaction
def lease_job(
    db: connection,
    redis: Redis,
    job_type: JobType,
    worker: str,
) -> Tuple[int, int, DataJobContext] | None:
    """Optimized job leasing with less contention."""
    id = redis.rpop(job_queue_cache_key(job_type))
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
                    worker = CASE
                        WHEN status = 1 THEN worker || ',' || %s
                        ELSE %s
                    END
                WHERE id = %s
                RETURNING id, retry, ctx"""
            ),
            (
                JobStatus.processing,
                epoch(),
                epoch() + get_lease_ttl(job_type),
                worker,
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
    reference_id: str | None = None,
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
def light_clean(db: connection):
    with db.cursor() as cur:
        # Reset expired processing jobs back to pending
        cur.execute(
            sql.SQL(
                """
                UPDATE job_queue 
                SET status = %s
                WHERE status = %s
                AND lease_expired_at < EXTRACT(EPOCH FROM NOW())::BIGINT
            """
            ),
            (JobStatus.pending, JobStatus.processing),
        )

        # delete finished or error jobs
        cur.execute(
            sql.SQL(
                """DELETE FROM job_queue
                WHERE (status = %s OR status = %s)
                AND job_type IN (%s, %s)"""
            ),
            (JobStatus.finished, JobStatus.error, JobType.pow, JobType.reward),
        )


@with_transaction
def clear_jobs(db: connection, job_type: JobType) -> None:
    with db.cursor() as cur:
        cur.execute(sql.SQL("DELETE FROM job_queue WHERE job_type = %s"), (job_type,))


@with_transaction
def delete_one_job(db: connection, item_id: int) -> None:
    with db.cursor() as cur:
        cur.execute(sql.SQL("DELETE FROM job_queue WHERE id = %s"), (item_id,))


@with_transaction
def get_queue_len(db: connection, redis: Redis, job_type: JobType) -> int:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT COUNT(*) FROM job_queue WHERE job_type = %s AND status = %s"
            ),
            (job_type, JobStatus.pending),
        )
        cached = redis.llen(job_queue_cache_key(job_type))
        return cur.fetchone()[0] + cached


@with_transaction
def update_job_worker(db: connection, item_id: int, worker: str) -> None:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """UPDATE job_queue
                SET worker = %s
                WHERE id = %s
            """
            ),
            (worker, item_id),
        )


@with_transaction
def get_job_lease(
    db: connection, item_id: int, job_type: JobType
) -> Tuple[DataJobContext | None, str | None]:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT ctx, worker
                FROM job_queue 
                WHERE id = %s 
                AND job_type = %s 
                AND status = %s
                AND lease_expired_at > EXTRACT(EPOCH FROM NOW())::BIGINT
            """
            ),
            (item_id, job_type, JobStatus.processing),
        )
        row = cur.fetchone()
        return (
            (
                DataJobContext.model_validate(row[0]),
                row[1].split(",") if row[1] else [],
            )
            if row
            else (None, [])
        )


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

    Returns a dictionary containing all job fields from the job_queue table.
    Returns None if job is not found.
    """
    with db.cursor() as cur:
        cur.execute(sql.SQL("SELECT * FROM job_queue WHERE id = %s"), (id,))
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


ALL_JOB_TYPES = [JobType.pow, JobType.classify, JobType.batch_classify, JobType.reward]

QUEUE_LEN = Gauge(
    "app_job_queue_len",
    "the queue length of each job_type",
    ["job_type"],
)


def queue_clean(conn: Connections):
    while True:
        with conn.get_pg_connection() as db:
            for job_type in ALL_JOB_TYPES:
                QUEUE_LEN.labels(job_type.name).set(
                    get_queue_len(db, conn.redis, job_type)
                )
            try:
                logging.info(f"light clean start for queue {str(job_type)}")
                light_clean(db)
                logging.info(f"light clean done for queue {str(job_type)}")
            except Exception as e:
                logging.error(f"failed to clean queue {job_type} with error {e}")
                continue
            time.sleep(int(os.environ.get("QUEUE_CLEAN_INTERVAL", 300)))


min_queue_len = 50000


@with_transaction
def refill_job_cache(db: connection, redis: Redis):
    with db.cursor() as cur:
        for job_type in ALL_JOB_TYPES:
            logging.info(f"refill job cache start for queue {str(job_type)}")
            if redis.llen(job_queue_cache_key(job_type)) > min_queue_len:
                logging.info(f"job cache for queue {str(job_type)} is full, skipping")
                continue

            cur.execute(
                sql.SQL(
                    """UPDATE job_queue 
                    SET status = %s
                    WHERE id IN (
                        SELECT id FROM job_queue
                        WHERE job_type = %s AND status = %s
                        ORDER BY published_at ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id"""
                ),
                (
                    JobStatus.processing,
                    job_type,
                    JobStatus.pending,
                    min_queue_len,
                ),
            )
            job_ids = [str(row[0]) for row in cur.fetchall()]
            if job_ids:
                redis.lpush(job_queue_cache_key(job_type), *job_ids)


def refill_job_cache_loop(conn: Connections):
    while True:
        with conn.get_pg_connection() as db:
            refill_job_cache(db, conn.redis)
        time.sleep(int(os.environ.get("QUEUE_REFILL_INTERVAL", 60)))


def get_lease_ttl(job_type: JobType) -> int:
    if job_type == JobType.reward:
        return REWARD_TTL
    elif job_type == JobType.batch_classify:
        return 3600
    else:
        return 600
