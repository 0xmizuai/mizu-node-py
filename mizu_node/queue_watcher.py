import asyncio
import logging
import time

from psycopg2 import sql
from psycopg2.extensions import connection, cursor
from redis import Redis

from mizu_node.config import get_min_queue_len
from mizu_node.db.common import with_transaction
from mizu_node.db.job_queue import job_queue_cache_key
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import JobStatus, JobType

logging.basicConfig(level=logging.INFO)  # Set the desired logging level


def reset_expired_processing_jobs(cur: cursor):
    cur.execute(
        sql.SQL(
            """
            UPDATE job_queue 
            SET status = %s
            WHERE status = %s
            AND lease_expired_at > EXTRACT(EPOCH FROM NOW())::BIGINT
        """
        ),
        (JobStatus.pending, JobStatus.processing),
    )
    logging.info(f"reset all expired jobs")


def cleanup_finished_jobs(cur: cursor):
    cur.execute(
        sql.SQL(
            """DELETE FROM job_queue
            WHERE (status = %s OR status = %s)
            AND job_type IN (%s, %s)
            AND finished_at < EXTRACT(EPOCH FROM NOW())::BIGINT - 86400
            """
        ),
        (JobStatus.finished, JobStatus.error, JobType.pow, JobType.reward),
    )
    logging.info(f"cleanup all finished jobs")


def get_pending_job_queues(cur: cursor):
    cur.execute(
        sql.SQL(
            """SELECT DISTINCT job_type, reference_id
            FROM job_queue
            WHERE status = %s
            """
        ),
        (JobStatus.pending,),
    )
    for job_type, reference_id in cur.fetchall():
        yield JobType(job_type), reference_id


def select_and_update_pending_jobs(
    cur: cursor, job_type: JobType, reference_id: int, min_queue_len: int
):
    cur.execute(
        sql.SQL(
            """WITH selected_jobs AS (
                SELECT id FROM job_queue
                WHERE job_type = %s AND reference_id = %s AND status = %s
                ORDER BY published_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE job_queue
            SET status = %s
            FROM selected_jobs
            WHERE job_queue.id = selected_jobs.id
            RETURNING job_queue.id"""
        ),
        (job_type, reference_id, JobStatus.pending, min_queue_len, JobStatus.cached),
    )
    return [str(row[0]) for row in cur.fetchall()]


def refill_job_queues(cur: cursor, redis: Redis):
    for job_type, reference_id in get_pending_job_queues(cur):
        min_queue_len = get_min_queue_len(job_type)
        queue_key = job_queue_cache_key(job_type, reference_id)
        logging.info(
            f"refill job cache start for queue {queue_key}: before {redis.llen(queue_key)}"
        )
        if redis.llen(queue_key) > min_queue_len:
            logging.info(f"job cache for queue {queue_key} is full, skipping")
            continue

        job_ids = select_and_update_pending_jobs(
            cur, job_type, reference_id, min_queue_len
        )
        if job_ids:
            logging.info(
                f"refill job cache for queue {queue_key} with {len(job_ids)} jobs"
            )
            redis.lpush(queue_key, *job_ids)
        logging.info(
            f"refill job cache done for queue {queue_key}: after {redis.llen(queue_key)}"
        )


@with_transaction
def process_queue(db: connection, redis: Redis, round: int):
    with db.cursor() as cur:
        logging.info(f"queue clean start")
        reset_expired_processing_jobs(cur)
        cleanup_finished_jobs(cur)
        logging.info(f"queue clean done")
        refill_job_queues(cur, redis)


async def watch(conn: Connections):
    while True:
        with conn.get_pg_connection() as db:
            try:
                logging.info(f"queue watcher start")
                process_queue(db, conn.redis, round)
                logging.info(f"queue watcher done")
            except Exception as e:
                logging.error(f"failed to clean queue with error {e}")
            finally:
                await asyncio.sleep(300)
