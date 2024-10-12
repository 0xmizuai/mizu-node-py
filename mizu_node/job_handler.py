import queue
import time

from bson import ObjectId

from mizu_node.db.job import (
    ClassificationJob,
    ClassificationJobResult,
    get_pending_job_ids,
    take_job,
    new_job,
    finish_job,
)

JOB_EXPIRE_TTL_SECONDS = 3600


class ProcessingJobQueueItem:
    worker: str
    assigned_at: int


# reconstruct pending jobs from mongodb
pending_jobs = queue.Queue(get_pending_job_ids())
processing_jobs: dict[ObjectId, ProcessingJobQueueItem] = {}


def handle_pending_jobs_len():
    return pending_jobs.qsize()


def handle_assigned_jobs_len():
    return len(processing_jobs)


def handle_take_job(worker: str) -> ClassificationJob | None:
    if len(pending_jobs) == 0:
        return None
    job_id = pending_jobs.get()
    processing_jobs[job_id] = ProcessingJobQueueItem(
        worker, assigned_at=int(time.time())
    )
    return take_job(job_id)


def handle_new_job(job: ClassificationJob):
    job_id = new_job(job)
    pending_jobs.put(job_id)
    return {"status": "ok"}


def handle_finish_job(worker: str, result: ClassificationJobResult):
    processing_job = processing_jobs.get(result.job_id)
    if not processing_job:
        return {"status": "error", "message": "Job not found"}
    if processing_job.worker != worker:
        return {"status": "error", "message": "worker not match"}
    if processing_job.assigned_at < int(time.time()) - JOB_EXPIRE_TTL_SECONDS:
        return {"status": "error", "message": "job expired"}
    finish_job(result)
    processing_jobs.pop(result.job_id, None)
    return {"status": "ok"}
