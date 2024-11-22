import time
from typing import Tuple
import uuid
from pydantic import BaseModel, Field
from redis import Redis

import uuid
from redis import Redis

from mizu_node.types.job import DataJob, JobConfig, JobType
from mizu_node.types.key_prefix import KeyPrefix


class QueueItem(BaseModel):
    item_id: str
    retry: int = Field(default=0)


class JobQueue(object):
    """A work queue backed by a redis database"""

    def __init__(self, name: KeyPrefix):
        self._session = uuid.uuid4().hex
        self._main_queue_key = name.of(":queue")
        self._processing_key = name.of(":processing")
        self._lease_key = KeyPrefix.concat(name, ":lease:")
        self._item_data_key = KeyPrefix.concat(name, ":job:")

    def add_items(self, db: Redis, item_ids: list[str], data: list[str]) -> None:
        pipeline = db.pipeline()
        for item_id, data in zip(item_ids, data):
            pipeline.set(
                self._item_data_key.of(item_id), data.model_dump_json(by_alias=True)
            )
            pipeline.lpush(
                self._main_queue_key, QueueItem(item_id=item_id).model_dump_json()
            )
        pipeline.execute()

    def queue_len(self, db: Redis) -> int:
        return db.llen(self._main_queue_key)

    def processing_len(self, db: Redis) -> int:
        # this is not accurate since we don't delete completed jobs
        # until light clean
        return db.llen(self._processing_key)

    def get_item_data(self, db: Redis, item_id: str) -> str | None:
        return db.get(self._item_data_key.of(item_id))

    def lease(self, db: Redis, ttl_secs: int) -> Tuple[str, int] | None:
        maybe_item_id: str | None = db.lmove(
            self._main_queue_key,
            self._processing_key,
            src="RGIHT",
            dest="LEFT",
        )
        if maybe_item_id is None:
            return None

        item = QueueItem.model_validate_json(maybe_item_id)
        (data,) = (
            db.pipeline()
            .get(self._item_data_key.of(item.item_id))
            .setex(self._lease_key.of(item.item_id), ttl_secs, self._session)
            .execute()
        )
        return (data, item.retry)

    def lease_exists(self, db: Redis, item_id: str | bytes) -> bool:
        return db.exists(self._lease_key.of(item_id)) != 0

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
        for item_str in processing:
            item = QueueItem.model_validate_json(item_str)
            has_lease_key = self.lease_exists(db, item.item_id)
            has_data_key = db.exists(self._item_data_key.of(item.item_id)) != 0

            # job completed
            if not has_data_key:
                print(
                    item.item_id,
                    " has been completed, will be deleted from processing queue",
                )
                db.lrem(self._processing_key, 0, item.item_id)
                continue

            # lease expired
            if not has_lease_key:
                print(item.item_id, " lease has expired, will reset")
                # move the job back to right of the queue
                item.retry += 1
                db.pipeline().lrem(self._processing_key, 0, item_str).rpush(
                    self._main_queue_key, item.model_dump_json()
                ).execute()


job_queues = {
    job_type: JobQueue(KeyPrefix(f"mizu_node_py:job_queue_{job_type.name}"))
    for job_type in [
        JobType.classify,
        JobType.pow,
        JobType.batch_classify,
        JobType.reward,
    ]
}


def job_queue(job_type: JobType):
    return job_queues[job_type]


def queue_clean(rclient: Redis):
    while True:
        for queue in job_queues.values():
            queue.light_clean(rclient)
        time.sleep(600)
