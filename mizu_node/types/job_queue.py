import logging
import os
import time
from typing import Tuple
from prometheus_client import Gauge
from pydantic import BaseModel, Field
from redis import Redis

from mizu_node.types.data_job import JobType
from mizu_node.types.key_prefix import KeyPrefix

logging.basicConfig(level=logging.INFO)  # Set the desired logging level


def delete_with_prefix(db: Redis, prefix: str) -> None:
    with db.pipeline() as pipe:
        for key in db.scan_iter(prefix):
            pipe.delete(key)
        pipe.execute()


class QueueItem(BaseModel):
    item_id: str
    retry: int = Field(default=0)


class JobQueue(object):
    """A work queue backed by a redis database"""

    def __init__(self, name: KeyPrefix):
        self._name = name
        self._main_queue_key = name.of(":queue")
        self._processing_key = name.of(":processing")
        self._lease_key = KeyPrefix.concat(name, ":lease:")
        self._item_data_key = KeyPrefix.concat(name, ":job:")

    def add_items(self, db: Redis, item_ids: list[str], data: list[str]) -> None:
        pipeline = db.pipeline()
        for item_id, data in zip(item_ids, data):
            pipeline.set(self._item_data_key.of(item_id), data)
            pipeline.lpush(
                self._main_queue_key, QueueItem(item_id=item_id).model_dump_json()
            )
        pipeline.execute()

    def clear(self, db: Redis) -> None:
        delete_with_prefix(db, self._name.of("*"))

    def queue_len(self, db: Redis) -> int:
        return db.llen(self._main_queue_key)

    def processing_len(self, db: Redis) -> int:
        # this is not accurate since we don't delete completed jobs
        # until light clean
        return db.llen(self._processing_key)

    def get_item_data(self, db: Redis, item_id: str) -> str | None:
        return db.get(self._item_data_key.of(item_id))

    def lease(
        self, db: Redis, ttl_secs: int, worker: str
    ) -> Tuple[QueueItem, str] | None:
        maybe_item_id: str | None = db.lmove(
            self._main_queue_key,
            self._processing_key,
            src="RIGHT",
            dest="LEFT",
        )
        if maybe_item_id is None:
            return None

        item = QueueItem.model_validate_json(maybe_item_id)
        values = (
            db.pipeline()
            .get(self._item_data_key.of(item.item_id))
            .setex(self._lease_key.of(item.item_id), ttl_secs, worker)
            .execute()
        )
        return (item, values[0])

    def get_lease(self, db: Redis, item_id: str | bytes) -> str | None:
        return db.get(self._lease_key.of(item_id))

    def complete(self, db: Redis, item_id: str) -> bool:
        job_del_result, _ = (
            db.pipeline()
            .delete(self._item_data_key.of(item_id))
            .delete(self._lease_key.of(item_id))
            .execute()
        )
        return job_del_result is not None and job_del_result != 0

    def light_clean(self, db: Redis):
        processing: list[bytes | str] = db.lrange(
            self._processing_key,
            0,
            -1,
        )
        total = len(processing)
        completed = 0
        expired = 0
        for item_str in processing:
            item = QueueItem.model_validate_json(item_str)
            has_lease_key = self.get_lease(db, item.item_id) is not None
            has_data_key = db.exists(self._item_data_key.of(item.item_id)) != 0

            # job completed
            if not has_data_key:
                logging.debug(
                    f"{item.item_id} has been completed, will be deleted from processing queue",
                )
                db.lrem(self._processing_key, 0, item_str)
                completed += 1
                continue

            # lease expired
            if not has_lease_key:
                logging.debug(f"{item.item_id} lease has expired, will reset")
                # move the job back to right of the queue
                item.retry += 1
                db.pipeline().lrem(self._processing_key, 0, item_str).rpush(
                    self._main_queue_key, item.model_dump_json()
                ).execute()
                expired += 1
        return total, completed, expired


ALL_JOB_TYPES = [JobType.classify, JobType.pow, JobType.batch_classify, JobType.reward]

job_queues = {
    job_type: JobQueue(KeyPrefix(f"mizu_node_py:job_queue_{job_type.name}"))
    for job_type in ALL_JOB_TYPES
}


def job_queue(job_type: JobType):
    return job_queues[job_type]


def queue_clear(rclient: Redis, job_type: JobType):
    job_queue(job_type).clear(rclient)


QUEUE_LEN = Gauge(
    "app_job_queue_len",
    "the queue length of each job_type",
    ["job_type"],
)


def queue_clean(rclient: Redis):
    while True:
        for job_type in ALL_JOB_TYPES:
            QUEUE_LEN.labels(job_type.name).set(job_queue(job_type).queue_len(rclient))
            try:
                logging.info(f"light clean start for queue {str(job_type)}")
                total, completed, expired = job_queues[job_type].light_clean(rclient)
                logging.info(
                    f"light clean done for queue {str(job_type)}: total={total}, completed={completed}, expired={expired}"
                )
            except Exception as e:
                logging.error(f"failed to clean queue {job_type} with error {e}")
                continue
        time.sleep(int(os.environ.get("QUEUE_CLEAN_INTERVAL", 300)))
