from fastapi.encoders import jsonable_encoder
import requests

from mizu_node.types.common import JobType
from mizu_node.types.data_job import (
    WorkerJob,
    WorkerJobResult,
)

SERVICE_URL = "http://localhost:8000"


def _build_job_result(job: WorkerJob):
    if job.job_type == JobType.pow:
        return WorkerJobResult(
            job_id=job.job_id, job_type=job.job_type, pow_result="0x"
        )


def process_job(job_type: JobType, jwt: str) -> str:
    res = requests.get(
        SERVICE_URL + "/take_job?job_type=" + str(job_type),
        headers={"Authorization": "Bearer " + jwt},
    )
    job_raw = res.json()["data"]["job"]
    if job_raw:
        job = WorkerJob.model_validate(job_raw)
        print("processing job: " + job.model_dump_json())
        requests.post(
            SERVICE_URL + "/finish_job",
            json=jsonable_encoder(_build_job_result(job)),
            headers={"Authorization": "Bearer " + jwt},
        )
        return job.job_id
    else:
        print("no job available")
