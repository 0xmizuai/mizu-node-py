from fastapi.encoders import jsonable_encoder
import requests

from mizu_node.types import JobType, PendingJobPayload, WorkerJobResult
from mizu_node.utils import epoch

SERVICE_URL = "http://localhost:8000"


def build_pending_job(input: str):
    return PendingJobPayload(
        publisher="p1",
        published_at=epoch(),
        job_type=JobType.classification,
        input=input,
    )


def get_pending_job_num():
    return requests.get(SERVICE_URL + "/stats/pending_jobs_num")


def get_assigned_job_num():
    return requests.get(SERVICE_URL + "/stats/assigned_jobs_num")


def publish_job(jobs: list[PendingJobPayload]):
    requests.post(SERVICE_URL + "/publish_jobs", json=jsonable_encoder.encode(jobs))


def take_job():
    requests.post(SERVICE_URL + "/take_job")


def finish_job():
    requests.post(SERVICE_URL + "/finish_job")


def happy_path():
    pending_job1 = build_pending_job("input1")
    pending_job2 = build_pending_job("input2")
    pending_job3 = build_pending_job("input3")
    publish_job([pending_job1, pending_job2, pending_job3])
    worker_job = take_job()
    result = WorkerJobResult(job_id=worker_job.job_id, output=["tag1", "tag2"])
    finish_job(result)


def inhonest_worker():
    pending_job = build_pending_job("input1")
    publish_job([pending_job])
    worker_job = take_job()
    result = WorkerJobResult(job_id=worker_job.job_id, output=["tag1", "tag2"])
    finish_job(result)
