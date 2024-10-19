from pydantic import BaseModel
from enum import Enum
import uuid

from mizu_node.utils import epoch


class QueueItem(object):
    def __init__(self, id: str, data: str):
        self.id = id
        self.data = data


class VerificationMode(str, Enum):
    none = "none"
    always = "always"
    random = "random"


class JobType(str, Enum):
    pow = "pow"
    classification = "classification"


class DataJobPayload(BaseModel):
    publisher: str
    published_at: int
    input: str  # could be serialized json


class PublishJobRequest(BaseModel):
    job_type: JobType
    jobs: list[DataJobPayload]  # could be serialized json


class DataJob(DataJobPayload):
    job_type: JobType
    job_id: str

    def from_payload(job: DataJobPayload, job_type: JobType):
        return DataJob(
            job_id=str(uuid.uuid4()),
            publisher=job.publisher,
            published_at=job.published_at,
            job_type=job_type,
            input=job.input,
        )


class WorkerJob(BaseModel):
    job_id: str
    job_type: JobType
    input: str
    callback_url: str | None = None

    def from_data_job(job: DataJob, callback_url: str | None = None):
        return WorkerJob(
            job_id=job.job_id,
            job_type=job.job_type,
            input=job.input,
            callback_url=callback_url,
        )


class WorkerJobResult(BaseModel):
    job_id: str
    job_type: JobType
    output: str | list[str]  # serialized json
    worker: str = None


class FinishedJob(DataJob):
    finished_at: int
    worker: str
    output: str | list[str]  # could be serialized json

    def from_job_result(job: DataJob, result: WorkerJobResult):
        return FinishedJob(
            job_id=job.job_id,
            publisher=job.publisher,
            published_at=job.published_at,
            job_type=job.job_type,
            input=job.input,
            worker=result.worker,
            finished_at=epoch(),
            output=result.output,
        )


class KeyPrefix:
    def __init__(self, prefix: str):
        self.prefix = prefix

    def of(self, name: str) -> str:
        return self.prefix + name

    @classmethod
    def concat(cls, prefix, name: str):
        return cls(prefix.of(name))
