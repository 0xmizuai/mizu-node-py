import argparse
import random
from uuid import uuid4
from fastapi.encoders import jsonable_encoder
import requests

from mizu_node.security import block_worker
from mizu_node.types.common import JobType
from mizu_node.types.data_job import (
    ClassifyContext,
    DataJob,
    DataJobPayload,
    PowContext,
    PublishJobRequest,
    WorkerJob,
    WorkerJobResult,
)
from mizu_node.utils import epoch
from scripts.auth import get_api_keys, issue_api_key

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
    result = requests.post(SERVICE_URL + "/publish_jobs", json=jsonable_encoder(req))
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
    block_worker()
    worker_job = take_job()
    finish_job(worker_job)


parser = argparse.ArgumentParser()
parser.add_argument(
    "--new_api_key", action="store", type=str, help="User to issue new API key"
)
parser.add_argument(
    "--get_api_keys", action="store", type=str, help="User to query API keys"
)
parser.add_argument("--happy", action="store_true")
parser.add_argument("--unhappy", action="store_true")

args = parser.parse_args()


def main():
    if args.happy:
        happy_path()
    elif args.unhappy:
        inhonest_worker()
    elif args.new_api_key:
        key = issue_api_key(args.new_api_key)
        print("API key: " + key)
    elif args.get_api_keys:
        keys = get_api_keys(args.get_api_keys)
        for key in keys:
            print("API key: " + key)
    else:
        raise ValueError("Invalid arguments")
