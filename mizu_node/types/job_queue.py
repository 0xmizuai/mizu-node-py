import uuid
from redis import Redis

from mizu_node.constants import ASSIGNED_JOB_EXPIRE_TTL_SECONDS
import uuid
from redis import Redis

from mizu_node.types.key_prefix import KeyPrefix


class QueueItem(object):
    def __init__(self, id: str, data: str):
        self.id = id
        self.data = data


class JobQueue(object):
    """A work queue backed by a redis database"""

    def __init__(self, name: KeyPrefix):
        self._session = uuid.uuid4().hex
        self._main_queue_key = name.of(":queue")
        self._processing_key = name.of(":processing")
        self._lease_key = KeyPrefix.concat(name, ":lease:")
        self._item_data_key = KeyPrefix.concat(name, ":job:")

    def add_items(self, db: Redis, items: list[QueueItem]) -> None:
        for item in items:
            db.set(self._item_data_key.of(item.id), item.data)
        db.lpush(self._main_queue_key, *[item.id for item in items])

    def queue_len(self, db: Redis) -> int:
        return db.llen(self._main_queue_key)

    def processing_len(self, db: Redis) -> int:
        # this is not accurate since we don't delete completed jobs
        # until light clean
        return db.llen(self._processing_key)

    def get_item_data(self, db: Redis, item_id: str) -> str | None:
        return db.get(self._item_data_key.of(item_id))

    def lease(self, db: Redis) -> str | None:
        maybe_item_id: bytes | str | None = db.lmove(
            self._main_queue_key,
            self._processing_key,
        )
        if maybe_item_id is None:
            return None

        data: str | None = db.get(self._item_data_key.of(maybe_item_id))
        if data is None:
            # the item will be cleaned up from processing queue in the next clean
            return None

        db.setex(
            self._lease_key.of(maybe_item_id),
            ASSIGNED_JOB_EXPIRE_TTL_SECONDS,
            self._session,
        )
        return data

    def lease_exists(self, db: Redis, item_id: str) -> bool:
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
        for item_id in processing:
            has_lease_key = self.lease_exists(db, item_id)
            has_data_key = db.exists(self._item_data_key.of(item_id)) != 0

            # job completed
            if not has_data_key:
                print(
                    item_id,
                    " has been completed, will be deleted from processing queue",
                )
                db.lrem(self._processing_key, 0, item_id)
                continue

            # lease expired
            if not has_lease_key:
                print(item_id, " lease has expired, will reset")
                db.pipeline().lrem(self._processing_key, 0, item_id).lpush(
                    self._main_queue_key, item_id
                ).execute()
