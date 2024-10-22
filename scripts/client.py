import random
from uuid import uuid4
from fastapi.encoders import jsonable_encoder
import requests

from mizu_node.types.common import JobType
from mizu_node.types.data_job import (
    ClassifyContext,
    DataJob,
    DataJobPayload,
    PowContext,
    PublishJobRequest,
    WorkerJob,
    WorkerJobResult,
    build_worker_job,
)
from mizu_node.utils import epoch

SERVICE_URL = "http://localhost:8000"


def _build_classify_job_payload():
    return DataJobPayload(
        job_type=JobType.classify,
        classify_ctx=ClassifyContext(r2_url=str(uuid4()), byte_size=1, checksum="0x"),
    )


def _build_pow_job_payload():
    return DataJobPayload(
        job_type=JobType.classify,
        pow_ctx=PowContext(difficulty=1, seed=str(uuid4())),
    )


def _build_job():
    if bool(random.getrandbits(1)):
        return _build_classify_job_payload()
    else:
        return _build_pow_job_payload()


def _build_publish_job_request(num_jobs: int):
    return PublishJobRequest(data=[_build_job() for _ in range(num_jobs)])


def publish_job(num_jobs: int) -> list[(str, int)]:
    req = _build_publish_job_request(num_jobs)
    result = requests.post(
        SERVICE_URL + "/publish_jobs", json=jsonable_encoder.encode(req)
    )
    jobs_id = result.json()["data"]["job_ids"]
    return [(job_id, req.job_type) for job_id, req in zip(jobs_id, req.data)]


def take_job() -> DataJob:
    res = requests.post(SERVICE_URL + "/take_job")
    return WorkerJob.model_validate(res["data"]["job"])


def _build_job_result(job: WorkerJob):
    if job.job_type == JobType.classify:
        return WorkerJobResult(
            job_id=job.job_id, job_type=job.job_type, classify_result=["tag1", "tag2"]
        )
    else:
        return WorkerJobResult(
            job_id=job.job_id, job_type=job.job_type, pow_result="0x"
        )


def finish_job(job: WorkerJob):
    result = _build_job_result(job)
    requests.post(SERVICE_URL + "/finish_job", json=jsonable_encoder(result))


def happy_path():
    publish_job(3)
    worker_job = take_job()
    finish_job(worker_job)


def inhonest_worker():
    publish_job(1)
    worker_job = take_job()
    finish_job(worker_job)
