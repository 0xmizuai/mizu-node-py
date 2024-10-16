import hashlib
from pydantic import BaseModel
from enum import Enum

from mizu_node.utils import epoch


class JobType(str, Enum):
    pow = "pow"
    classification = "classification"


class PendingJobPayload(BaseModel):
    publisher: str
    published_at: int
    job_type: JobType
    input: str  # could be serialized json


class PendingJob(PendingJobPayload):
    job_id: str

    def _gen_job_id(job: PendingJobPayload) -> str:
        sha = hashlib.sha256()
        sha.update(job.input + job.publisher + str(job.published_at).encode())
        return sha.hexdigest()

    def from_payload(job: PendingJobPayload):
        return PendingJob(
            job_id=PendingJob._gen_job_id(job),
            publisher=job.publisher,
            published_at=job.published_at,
            job_type=job.job_type,
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
    output: str | list[str]  # could be serialized json

    def from_assigned_job(job: AssignedJob, output: str | list[str]):
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


class WorkerJob(PendingJob):
    callback_url: str = None
    debug: bool = False

    def from_pending_job(job: PendingJob, callback_url: str):
        return WorkerJob(
            job_id=job.job_id,
            publisher=job.publisher,
            published_at=job.published_at,
            job_type=job.job_type,
            input=job.input,
            callback_url=callback_url,
        )


class WorkerJobResult(BaseModel):
    job_id: str
    output: str | list[str]  # serialized json
    worker: str
