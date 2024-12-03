import logging
import os
import time
from typing import Tuple
from prometheus_client import Gauge
from psycopg2 import sql
from datetime import datetime, timedelta

from mizu_node.types.data_job import JobType

from psycopg2.extensions import connection

logging.basicConfig(level=logging.INFO)  # Set the desired logging level


class JobStatus:
    PENDING = 0
    PROCESSING = 1
    FINISHED = 2


class JobQueue:
    def __init__(self, job_type: JobType):
        self.job_type = int(job_type)

    def add_items(self, db: connection, item_ids: list[int], data: list[str]) -> None:
        with db.cursor() as cur:
            for item_id, data in zip(item_ids, data):
                cur.execute(
                    sql.SQL(
                        "INSERT INTO job_queue (id, job_type, data, status, created_at) VALUES (%s, %s, %s, %s, %s)"
                    ),
                    (item_id, self.job_type, data, JobStatus.PENDING, datetime.now()),
                )
            db.commit()

    def clear(self, db: connection) -> None:
        with db.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM job_queue WHERE job_type = %s"), (self.job_type,)
            )
            db.commit()

    def queue_len(self, db: connection) -> int:
        with db.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT COUNT(*) FROM job_queue WHERE job_type = %s AND status = %s"
                ),
                (self.job_type, JobStatus.PENDING),
            )
            return cur.fetchone()[0]

    def lease(
        self, db: connection, ttl_secs: int, worker: str
    ) -> Tuple[int, int, str] | None:
        with db.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT id, retry, data FROM job_queue WHERE job_type = %s AND status = %s ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED"
                ),
                (self.job_type, JobStatus.PENDING),
            )
            row = cur.fetchone()
            if row is None:
                return None

            item_id, retry, data = row
            cur.execute(
                sql.SQL(
                    "UPDATE job_queue SET status = %s, expired_at = %s, worker = %s WHERE id = %s"
                ),
                (
                    JobStatus.PROCESSING,
                    datetime.now() + timedelta(seconds=ttl_secs),
                    worker,
                    item_id,
                ),
            )
            db.commit()
            return (item_id, retry, data)

    def complete(self, db: connection, item_id: int) -> bool:
        with db.cursor() as cur:
            cur.execute(
                sql.SQL("UPDATE job_queue SET status = %s WHERE id = %s"),
                (JobStatus.FINISHED, item_id),
            )
            db.commit()
            return cur.rowcount > 0

    def light_clean(self, db: connection):
        with db.cursor() as cur:
            # Clean finished jobs
            cur.execute(
                sql.SQL("DELETE FROM job_queue WHERE status = %s"),
                (JobStatus.FINISHED,),
            )
            # Reset expired processing jobs back to pending
            cur.execute(
                sql.SQL(
                    """
                    UPDATE job_queue 
                    SET status = %s, 
                        expired_at = NULL, 
                        worker = NULL
                        retry = retry + 1
                    WHERE status = %s
                    AND expired_at < NOW()
                """
                ),
                (JobStatus.PENDING, JobStatus.PROCESSING),
            )
            db.commit()

    def get_lease(self, db: connection, item_id: int) -> str | None:
        with db.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT worker 
                    FROM job_queue 
                    WHERE id = %s 
                    AND job_type = %s 
                    AND status = %s
                    AND expired_at > NOW()
                """
                ),
                (item_id, self.job_type, JobStatus.PROCESSING),
            )
            row = cur.fetchone()
            return row[0] if row else None


ALL_JOB_TYPES = [JobType.classify, JobType.pow, JobType.batch_classify, JobType.reward]

job_queues = {job_type: JobQueue(job_type) for job_type in ALL_JOB_TYPES}


def job_queue(job_type: JobType):
    return job_queues[job_type]


def queue_clear(db: connection, job_type: JobType):
    job_queue(job_type).clear(db)


QUEUE_LEN = Gauge(
    "app_job_queue_len",
    "the queue length of each job_type",
    ["job_type"],
)


def queue_clean(db: connection):
    while True:
        for job_type in ALL_JOB_TYPES:
            QUEUE_LEN.labels(job_type.name).set(job_queue(job_type).queue_len(db))
            try:
                logging.info(f"light clean start for queue {str(job_type)}")
                job_queues[job_type].light_clean(db)
                logging.info(f"light clean done for queue {str(job_type)}")
            except Exception as e:
                logging.error(f"failed to clean queue {job_type} with error {e}")
                continue
        time.sleep(int(os.environ.get("QUEUE_CLEAN_INTERVAL", 300)))
