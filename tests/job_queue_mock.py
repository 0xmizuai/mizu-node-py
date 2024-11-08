import queue
from uuid import uuid4

from fastapi.encoders import jsonable_encoder
from redis import Redis

from mizu_node.types.job import WorkerJob
from mizu_node.types.job_queue import TTL


class JobQueueMock:
    def __init__(self, name):
        self.name = name
        self.q = queue.Queue()
        self.processing = {}

    def add_item(self, job: WorkerJob):
        self.q.put_nowait(jsonable_encoder(job))

    def get(self, rclient: Redis):
        if self.q.empty():
            return None
        job = WorkerJob(**self.q.get())
        self.processing[job.job_id] = job
        return job

    def ack(self, rclient: Redis, job_id: str):
        self.processing.pop(job_id, None)

    def queue_len(self):
        return self.q.qsize() + len(self.processing)
