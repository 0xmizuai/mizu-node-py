import time
from pydantic import BaseModel
import uuid

from mizu_node.types.common import JobType


class ClassifyContext(BaseModel):
    r2_url: str
    byte_size: int
    checksum: str


class PowContext(BaseModel):
    difficulty: int
    seed: str


class DataJobPayload(BaseModel):
    job_type: JobType
    classify_ctx: ClassifyContext | None = None
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
    classify_result: list[str] | None = None
    pow_result: str | None = None


class FinishedJob(object):
    def __init__(self, worker: str, job: DataJob, result: WorkerJobResult):
        self._id = job.job_id
        self.job_type = job.job_type
        self.classify_ctx = job.classify_ctx
        self.pow_ctx = job.pow_ctx
        self.published_at = job.published_at
        self.publisher = job.publisher
        self.finished_at = int(time.time())
        self.worker = worker
        self.classify_result = result.classify_result
        self.pow_result = result.pow_result


def build_data_job(publisher: str, job: DataJobPayload) -> DataJob:
    return DataJob(
        job_id=str(uuid.uuid4()),
        job_type=job.job_type,
        publisher=publisher,
        published_at=int(time.time()),
        classify_ctx=job.classify_ctx,
        pow_ctx=job.pow_ctx,
    )


def build_worker_job(job: DataJob) -> WorkerJob:
    return WorkerJob(
        job_id=job.job_id,
        job_type=job.job_type,
        classify_ctx=job.classify_ctx,
        pow_ctx=job.pow_ctx,
    )
