import uuid
from redis import Redis

from mizu_node.constants import ASSIGNED_JOB_EXPIRE_TTL_SECONDS, REDIS_JOB_QUEUE_NAME
from mizu_node.types import DataJob, JobType, KeyPrefix
import uuid
from redis import Redis


class JobQueue(object):
    """A work queue backed by a redis database"""

    def __init__(self, name: KeyPrefix):
        self._session = uuid.uuid4().hex
        self._main_queue_key = name.of(":queue")
        self._processing_key = name.of(":processing")
        self._lease_key = KeyPrefix.concat(name, ":lease:")
        self._job_data_key = KeyPrefix.concat(name, ":job:")

    def add_jobs(self, db: Redis, jobs: list[DataJob]) -> None:
        job_ids = [job.job_id for job in jobs]
        for job in jobs:
            db.set(self._job_data_key.of(job.job_id), job.model_dump_json())
        db.lpush(self._main_queue_key, *job_ids)

    def queue_len(self, db: Redis) -> int:
        return db.llen(self._main_queue_key)

    def processing_len(self, db: Redis) -> int:
        # this is not accurate since we don't delete completed jobs
        # until light clean
        return db.llen(self._processing_key)

    def get_job_data(self, db: Redis, job_id: str) -> DataJob | None:
        job_json = db.get(self._job_data_key.of(job_id))
        if job_json is None:
            return None
        return DataJob.model_validate_json(job_json)

    def lease(self, db: Redis) -> DataJob | None:
        maybe_job_id: bytes | str | None = db.lmove(
            self._main_queue_key,
            self._processing_key,
        )
        if maybe_job_id is None:
            return None

        data: bytes | None = db.get(self._job_data_key.of(maybe_job_id))
        if data is None:
            # the item will be cleaned up from processing queue in the next clean
            return None

        db.setex(
            self._lease_key.of(maybe_job_id),
            ASSIGNED_JOB_EXPIRE_TTL_SECONDS,
            self._session,
        )
        return DataJob.model_validate_json(data)

    def lease_exists(self, db: Redis, job_id: str) -> bool:
        return db.exists(self._lease_key.of(job_id)) != 0

    def complete(self, db: Redis, job_id: str) -> bool:
        job_del_result, _ = (
            db.pipeline()
            .delete(self._job_data_key.of(job_id))
            .delete(self._lease_key.of(job_id))
            .execute()
        )
        return job_del_result is not None and job_del_result != 0

    def light_clean(self, db: Redis):
        processing: list[bytes | str] = db.lrange(
            self._processing_key,
            0,
            -1,
        )
        for job_id in processing:
            has_lease_key = self.lease_exists(db, job_id)
            has_data_key = db.exists(self._job_data_key.of(job_id)) != 0

            # job completed
            if not has_data_key:
                print(
                    job_id, " has been completed, will be deleted from processing queue"
                )
                db.lrem(self._processing_key, 0, job_id)
                continue

            # lease expired
            if not has_lease_key:
                print(job_id, " lease has expired, will reset")
                db.pipeline().lrem(self._processing_key, 0, job_id).lpush(
                    self._main_queue_key, job_id
                ).execute()


VALID_JOB_TYPES = [JobType.classification, JobType.pow]
job_queues = {
    job_type: JobQueue(KeyPrefix(REDIS_JOB_QUEUE_NAME + ":" + job_type + ":"))
    for job_type in VALID_JOB_TYPES
}
