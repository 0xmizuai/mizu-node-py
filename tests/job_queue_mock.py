import queue

from redis import Redis

from mizu_node.types.job import WorkerJob


class JobQueueMock:
    def __init__(self, name, connection_type: str):
        self.name = name
        self.q = queue.Queue()
        self.processing = {}
        self.connection_type = connection_type

    def add_item(self, job: WorkerJob):
        self.q.put_nowait(job.model_dump_json(by_alias=True))

    def get(self, rclient: Redis):
        if self.q.empty():
            return None
        job = WorkerJob.model_validate_json(self.q.get())
        self.processing[job.job_id] = job
        return job

    def expire_job(self, job_id: str):
        self.processing.pop(job_id, None)
        self.q.put_nowait(job_id)

    def ack(self, rclient: Redis, job_id: str):
        result = self.processing.pop(job_id, None)
        return result is not None

    def queue_len(self):
        return self.q.qsize() + len(self.processing)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
