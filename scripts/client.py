import argparse
import os
import random
from uuid import uuid4
from fastapi.encoders import jsonable_encoder
import requests

from mizu_node.constants import VERIFY_KEY
from mizu_node.security import verify_jwt
from mizu_node.types.common import JobType
from mizu_node.types.data_job import (
    ClassifyContext,
    DataJobPayload,
    PowContext,
    PublishJobRequest,
    WorkerJob,
    WorkerJobResult,
)
from scripts.auth import get_api_keys, issue_api_key, sign_jwt

SERVICE_URL = "http://localhost:8000"
SECRET_KEY = os.environ["SECRET_KEY"]


def _build_classify_job_payload():
    return DataJobPayload(
        job_type=JobType.classify,
        classify_ctx=ClassifyContext(r2_key=str(uuid4()), byte_size=1, checksum="0x"),
    )


def _build_pow_job_payload():
    return DataJobPayload(
        job_type=JobType.pow,
        pow_ctx=PowContext(difficulty=1, seed=str(uuid4())),
    )


def _build_job():
    if bool(random.getrandbits(1)):
        return _build_classify_job_payload()
    else:
        return _build_pow_job_payload()


def _build_publish_job_request(num_jobs: int):
    return PublishJobRequest(data=[_build_job() for _ in range(num_jobs)])


def _build_job_result(job: WorkerJob):
    if job.job_type == JobType.classify:
        return WorkerJobResult(
            job_id=job.job_id, job_type=job.job_type, classify_result=["tag1", "tag2"]
        )
    else:
        return WorkerJobResult(
            job_id=job.job_id, job_type=job.job_type, pow_result="0x"
        )


def publish_jobs(num_jobs: int, api_key: str) -> list[(str, int)]:
    req = _build_publish_job_request(num_jobs)
    result = requests.post(
        SERVICE_URL + "/publish_jobs",
        json=jsonable_encoder(req),
        headers={"Authorization": "Bearer " + api_key},
    )
    jobs_id = result.json()["data"]["job_ids"]
    return [(job_id, req.job_type) for job_id, req in zip(jobs_id, req.data)]


def process_job(job_type: JobType, token: str) -> str:
    res = requests.post(
        SERVICE_URL + "/take_job?job_type=" + job_type,
        headers={"Authorization": "Bearer " + token},
    )
    job = WorkerJob.model_validate(res["data"]["job"])
    requests.post(
        SERVICE_URL + "/finish_job",
        json=jsonable_encoder(_build_job_result(job)),
        headers={"Authorization": "Bearer " + token},
    )
    return job.job_id


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="command", required=True)

job_parser_publish = subparsers.add_parser(
    "publish", add_help=False, description="publish jobs"
)
job_parser_publish.add_argument(
    "--num", action="store", type=int, help="Number of jobs to publish"
)
job_parser_publish.add_argument(
    "--api_key",
    action="store",
    type=str,
    help="the api key",
)

job_parser_process = subparsers.add_parser(
    "process",
    add_help=False,
    description="take and finish jobs",
)
job_parser_process.add_argument(
    "--token",
    action="store",
    type=str,
    help="the jwt token for auth",
)
job_parser_process.add_argument(
    "--job_type",
    action="store",
    type=str,
    help="the job type to process",
)

new_api_key_parser = subparsers.add_parser(
    "new_api_key",
    add_help=False,
)
new_api_key_parser.add_argument(
    "--user", action="store", type=str, help="User to issue new API key"
)

get_api_keys_parser = subparsers.add_parser(
    "get_api_keys",
    add_help=False,
)
get_api_keys_parser.add_argument(
    "--user", action="store", type=str, help="User to query API keys"
)

new_jwt_parser = subparsers.add_parser(
    "new_jwt",
    add_help=False,
)
new_jwt_parser.add_argument("--user", action="store", type=str, help="user id to sign")

verify_jwt_parser = subparsers.add_parser(
    "verify_jwt",
    add_help=False,
)
verify_jwt_parser.add_argument(
    "--token", action="store", type=str, help="the token to verify"
)

args = parser.parse_args()


def main():
    if args.command == "publish":
        job_ids = publish_jobs(args.num, args.api_key)
        print("Published jobs: " + str(job_ids))
    elif args.command == "process":
        job_id = process_job(args.job_type, args.token)
        print(f"Processed {args.job_type} job: {job_id}")
    elif args.command == "new_api_key":
        key = issue_api_key(args.user)
        print("API key: " + key)
    elif args.command == "get_api_keys":
        keys = get_api_keys(args.user)
        for key in keys:
            print("API key: " + key)
    elif args.command == "new_jwt":
        token = sign_jwt(args.user, SECRET_KEY)
        print("Token: " + token)
    elif args.command == "verify_jwt":
        user = verify_jwt(args.token, VERIFY_KEY)
        print("User: " + user)
    else:
        raise ValueError("Invalid arguments")
