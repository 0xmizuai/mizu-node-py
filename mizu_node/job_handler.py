import time
import os

from bson import ObjectId

from mizu_node.db.job import (
    ClassificationJobFromPublisher,
    ClassificationJobForWorker,
    ClassificationJobResultFromWorker,
    take_job,
    new_job,
    finish_job,
    get_pending_jobs_num,
    get_processing_jobs_num,
)


def handle_pending_jobs_len():
    return get_pending_jobs_num()


def handle_assigned_jobs_len():
    return get_processing_jobs_num()


def handle_take_job(worker: str) -> ClassificationJobForWorker | None:
    return take_job(worker)


def handle_new_job(job: ClassificationJobFromPublisher):
    new_job(job)
    return {"status": "ok"}


def handle_finish_job(result: ClassificationJobResultFromWorker):
    finish_job(result)
    return {"status": "ok"}
