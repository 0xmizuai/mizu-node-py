from typing import Union
from pydantic import BaseModel
from enum import Enum
import uuid

from mizu_node.utils import epoch


class VerificationMode(str, Enum):
    none = "none"
    always = "always"
    random = "random"


class JobType(str, Enum):
    pow = "pow"
    classification = "classification"


class PendingJobPayload(BaseModel):
    publisher: str
    published_at: int
    input: str  # could be serialized json


class PendingJobRequest(BaseModel):
    job_type: JobType
    jobs: list[PendingJobPayload]


class PendingJob(PendingJobPayload):
    job_id: str
    job_type: JobType

    def from_payload(job: PendingJobPayload, job_type: JobType):
        return PendingJob(
            job_id=str(uuid.uuid4()),
            publisher=job.publisher,
            published_at=job.published_at,
            job_type=job_type,
            input=job.input,
        )


class AssignedJob(PendingJob):
    worker: str
    assigned_at: int

    def from_pending_job(job: PendingJob, worker: str):
        return AssignedJob(
            job_id=job.job_id,
            publisher=job.publisher,
            published_at=job.published_at,
            job_type=job.job_type,
            input=job.input,
            worker=worker,
            assigned_at=epoch(),
        )


class FinishedJob(AssignedJob):
    finished_at: int
    output: Union[str, list[str]]  # could be serialized json

    def from_assigned_job(job: AssignedJob, output: Union[str, list[str]]):
        return FinishedJob(
            job_id=job.job_id,
            publisher=job.publisher,
            published_at=job.published_at,
            job_type=job.job_type,
            input=job.input,
            worker=job.worker,
            assigned_at=job.assigned_at,
            finished_at=epoch(),
            output=output,
        )


# worker related


class WorkerJob(BaseModel):
    job_id: str
    job_type: JobType
    input: str

    def from_pending_job(job: PendingJob):
        return WorkerJob(
            job_id=job.job_id,
            job_type=job.job_type,
            input=job.input,
        )


class WorkerJobResult(BaseModel):
    job_id: str
    output: Union[str, list[str]]  # serialized json
    worker: str = None
