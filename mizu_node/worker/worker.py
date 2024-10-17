from enum import Enum
import requests

from mizu_node.constants import VERIFY_JOB_CALLBACK_URL
from mizu_node.types import WorkerJob, WorkerJobResult
from mizu_node.worker.classifier import classify
from mizu_node.worker.embeddings.domain_embeddings import V1_EMBEDDING


class JobType(str, Enum):
    pow = "pow"
    classification = "classification"


def job_worker(job: WorkerJob):
    if job.job_type == JobType.classification:
        text = requests.get(job.input).json()
        tags = classify(text, V1_EMBEDDING)
        requests.post(
            VERIFY_JOB_CALLBACK_URL,
            json=WorkerJobResult(job_id=job.job_id, output=tags).model_dump_json(),
        )
    else:
        raise NotImplementedError(f"Job type {job.job_type} not implemented")
