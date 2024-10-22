import time
from pydantic import BaseModel
import uuid

from mizu_node.types.common import JobType


class ClassificationContext(BaseModel):
    r2_url: str
    byte_size: str
    checksum: str


class PowContext(BaseModel):
    difficulty: int
    seed: str


class DataJobPayload(BaseModel):
    job_type: JobType
    classification_ctx: ClassificationContext | None = None
    pow_ctx: PowContext | None = None


class PublishJobRequest(BaseModel):
    data: list[DataJobPayload]


class DataJob(DataJobPayload):
    job_id: str
    job_type: JobType
    publisher: str
    published_at: int


class WorkerJob(DataJobPayload):
    job_id: str


class WorkerJobResult(BaseModel):
    job_id: str
    job_type: JobType
    classify_result: list[str]
    pow_result: str


class FinishedJob(object):
    def __init__(self, worker: str, job: DataJob, result: WorkerJobResult):
        self._id = job.job_id
        self.job_type = job.job_type
        self.classification_ctx = job.classification_ctx
        self.pow_ctx = job.pow_ctx
        self.published_at = job.published_at
        self.publisher = job.publisher
        self.finished_at = int(time.time())
        self.worker = worker
        self.classification_result = result.classification_result
        self.pow_result = result.pow_result


def build_data_job(publisher: str, job: DataJobPayload) -> DataJob:
    return DataJob(
        job_id=str(uuid.uuid4()),
        job_type=job.job_type,
        publisher=publisher,
        published_at=int(time.time()),
        classification_ctx=job.classification_ctx,
        pow_ctx=job.pow_ctx,
    )


def build_worker_job(job: DataJob) -> WorkerJob:
    return WorkerJob(
        job_id=job.job_id,
        job_type=job.job_type,
        classification_ctx=job.classification_ctx,
        pow_ctx=job.pow_ctx,
    )
