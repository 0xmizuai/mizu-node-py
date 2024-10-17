from enum import Enum
import requests

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

from mizu_node.constants import VERIFY_JOB_CALLBACK_URL
from mizu_node.worker.classifier import classify
from mizu_node.worker.embeddings.domain_embeddings import V1_EMBEDDING


class JobType(str, Enum):
    pow = "pow"
    classification = "classification"


class WorkerJob(BaseModel):
    job_id: str
    job_type: JobType
    input: str
    callback_url: str = None


class WorkerJobResult(BaseModel):
    job_id: str
    output: str | list[str]


def job_worker(job: WorkerJob):
    if job.job_type == JobType.classification:
        text = requests.get(job.input).json()
        tags = classify(text, V1_EMBEDDING)
        requests.post(
            VERIFY_JOB_CALLBACK_URL,
            json=jsonable_encoder(WorkerJobResult(job_id=job.job_id, output=tags)),
        )
    else:
        raise NotImplementedError(f"Job type {job.job_type} not implemented")
