import queue

from redis import Redis

from mizu_node.types.job import WorkerJob
from mizu_node.types.job_queue import TTL


class JobQueueMock:
    def __init__(self, name):
        self.name = name
        self.q = queue.Queue()
        self.processing = {}

    def add_item(self, job: WorkerJob):
        self.q.put_nowait(job.model_dump_json(by_alias=True))

    def get(self, rclient: Redis):
        if self.q.empty():
            return None
        job = WorkerJob.model_validate_json(self.q.get())
        self.processing[job.job_id] = job
        return job

    def ack(self, rclient: Redis, job_id: str):
        self.processing.pop(job_id, None)

    def queue_len(self):
        return self.q.qsize() + len(self.processing)
