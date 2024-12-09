import logging
import os
import random
import time
from typing import Any, Tuple
from typing import Tuple

from prometheus_client import Gauge
from psycopg2 import sql
from pydantic import BaseModel
from psycopg2.extensions import connection

from mizu_node.common import epoch
from mizu_node.db.common import with_transaction
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import (
    DataJobContext,
    DataJobResult,
    JobStatus,
    JobType,
)
from mizu_node.types.service import DataJobQueryResult, RewardJobRecord


JOB_TTL = int(os.environ.get("JOB_TTL", 7 * 24 * 60 * 60))  # 7 days in seconds

logging.basicConfig(level=logging.INFO)  # Set the desired logging level


def get_random_offset():
    max_concurrent_lease = int(os.environ.get("MAX_CONCURRENT_LEASE", 10))
    return random.randint(0, max_concurrent_lease)


@with_transaction
def lease_job(
    db: connection,
    job_type: JobType,
    ttl_secs: int,
    worker: str,
) -> Tuple[int, int, DataJobContext] | None:
    """Optimized job leasing with less contention."""
    with db.cursor() as cur:
        # Add randomization to reduce contention
        cur.execute(
            sql.SQL(
                """
                WITH candidate_jobs AS (
                    SELECT id, retry, ctx,
                           random() as r  -- Add randomization
                    FROM job_queue
                    WHERE job_type = %s
                    AND status = %s
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
                SET status = %s,
                    assigned_at = EXTRACT(EPOCH FROM NOW())::BIGINT,
                    lease_expired_at = %s,
                    worker = %s
                FROM selected_job s
                WHERE j.id = s.id
                RETURNING j.id, s.retry, s.ctx
                """
            ),
            (
                job_type,
                JobStatus.pending,
                JobStatus.processing,
                epoch() + ttl_secs,
                worker,
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
    publisher: str,
    contexts: list[BaseModel],
) -> list[int]:
    with db.cursor() as cur:
        inserted_ids = []
        for ctx in contexts:
            cur.execute(
                sql.SQL(
                    """
                        INSERT INTO job_queue (job_type, ctx, publisher) 
                        VALUES (%s, %s::jsonb, %s)
                        RETURNING id
                        """
                ),
                (
                    job_type,
                    ctx.model_dump_json(by_alias=True, exclude_none=True),
                    publisher,
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

        # Delete completed jobs older than 7 days
        cur.execute(
            sql.SQL(
                """
                DELETE FROM job_queue 
                WHERE status IN (%s, %s)
                AND finished_at < EXTRACT(EPOCH FROM NOW())::BIGINT - %s
                RETURNING id
                """
            ),
            (
                JobStatus.finished,
                JobStatus.error,
                JOB_TTL,
            ),
        )

        deleted_rows = cur.fetchall()
        if deleted_rows:
            logging.info(f"Deleted {len(deleted_rows)} old completed jobs")


@with_transaction
def clear_jobs(db: connection, job_type: JobType) -> None:
    with db.cursor() as cur:
        cur.execute(sql.SQL("DELETE FROM job_queue WHERE job_type = %s"), (job_type,))


@with_transaction
def delete_one_job(db: connection, item_id: int) -> None:
    with db.cursor() as cur:
        cur.execute(sql.SQL("DELETE FROM job_queue WHERE id = %s"), (item_id,))


@with_transaction
def queue_len(db: connection, job_type: JobType) -> int:
    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT COUNT(*) FROM job_queue WHERE job_type = %s AND status = %s"
            ),
            (job_type, JobStatus.pending),
        )
        return cur.fetchone()[0]


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
        return (DataJobContext.model_validate(row[0]), row[1]) if row else (None, None)


@with_transaction
def get_jobs_info(db: connection, item_ids: list[int]) -> list[DataJobQueryResult]:
    if not item_ids:
        return []

    with db.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT id, job_type, status, ctx, result, worker, finished_at
                FROM job_queue 
                WHERE id = ANY(%s)
                """
            ),
            (item_ids,),
        )
        rows = cur.fetchall()

        return [
            DataJobQueryResult(
                job_id=row[0],
                job_type=row[1],
                status=row[2],
                context=DataJobContext.model_validate(row[3]),
                result=(DataJobResult.model_validate(row[4]) if row[4] else None),
                worker=row[5],
                finished_at=row[6],
            )
            for row in rows
        ]


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
def get_job_info_raw(db: connection, id: int) -> list[Tuple[Any, ...]]:
    with db.cursor() as cur:
        cur.execute(sql.SQL("SELECT * FROM job_queue WHERE id = %s"), (id,))
        return cur.fetchone()


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
                QUEUE_LEN.labels(job_type.name).set(queue_len(db, job_type))
            try:
                logging.info(f"light clean start for queue {str(job_type)}")
                light_clean(db)
                logging.info(f"light clean done for queue {str(job_type)}")
            except Exception as e:
                logging.error(f"failed to clean queue {job_type} with error {e}")
                continue
            time.sleep(int(os.environ.get("QUEUE_CLEAN_INTERVAL", 300)))
