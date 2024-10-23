import time
from pydantic import BaseModel
import uuid

from mizu_node.types.common import JobType


class ClassifyContext(BaseModel):
    r2Key: str
    byteSize: int
    checksum: str


class PowContext(BaseModel):
    difficulty: int
    seed: str


class DataJobPayload(BaseModel):
    jobType: JobType
    classifyCtx: ClassifyContext | None = None
    powCtx: PowContext | None = None


class PublishJobRequest(BaseModel):
    data: list[DataJobPayload]


class DataJob(DataJobPayload):
    jobId: str
    jobType: JobType
    publisher: str
    publishedAt: int


class WorkerJob(DataJobPayload):
    jobId: str


class WorkerJobResult(BaseModel):
    jobId: str
    jobType: JobType
    classifyResult: list[str] | None = None
    powResult: str | None = None


class FinishedJob(object):
    def __init__(self, worker: str, job: DataJob, result: WorkerJobResult):
        self._id = job.jobId
        self.jobType = job.jobType
        self.classifyCtx = job.classifyCtx
        self.powCtx = job.powCtx
        self.publishedAt = job.publishedAt
        self.publisher = job.publisher
        self.finishedAt = int(time.time())
        self.worker = worker
        self.classifyResult = result.classifyResult
        self.powResult = result.powResult


def build_data_job(publisher: str, job: DataJobPayload) -> DataJob:
    return DataJob(
        jobId=str(uuid.uuid4()),
        jobType=job.jobType,
        publisher=publisher,
        publishedAt=int(time.time()),
        classifyCtx=job.classifyCtx,
        powCtx=job.powCtx,
    )


def build_worker_job(job: DataJob) -> WorkerJob:
    return WorkerJob(
        jobId=job.jobId,
        jobType=job.jobType,
        classifyCtx=job.classifyCtx,
        powCtx=job.powCtx,
    )
