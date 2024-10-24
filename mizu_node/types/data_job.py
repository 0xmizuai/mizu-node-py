import time
from pydantic import BaseModel, ConfigDict, Field
import uuid

from mizu_node.types.common import JobType


class ClassifyContext(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    r2_key: str = Field(alias="r2Key")
    byte_size: int = Field(alias="byteSize")
    checksum: str


class PowContext(BaseModel):
    difficulty: int
    seed: str


class DataJobPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_type: JobType = Field(alias="jobType")
    classify_ctx: ClassifyContext | None = Field(alias="classifyCtx", default=None)
    pow_ctx: PowContext | None = Field(alias="powCtx", default=None)


class PublishJobRequest(BaseModel):
    data: list[DataJobPayload]


class DataJob(DataJobPayload):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="jobId")
    job_type: JobType = Field(alias="jobType")
    publisher: str
    published_at: int = Field(alias="publishedAt")


class WorkerJob(DataJobPayload):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="jobId")


class WorkerJobResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="jobId")
    job_type: JobType = Field(alias="jobType")
    classify_result: list[str] | None = Field(alias="classifyResult", default=None)
    pow_result: str | None = Field(alias="powResult", default=None)


class FinishedJob(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    db_id: str = Field(alias="_id")
    job_type: JobType = Field(alias="jobType")
    classify_ctx: ClassifyContext | None = Field(alias="classifyCtx")
    pow_ctx: PowContext | None = Field(alias="powCtx")
    published_at: int = Field(alias="publishedAt")
    publisher: str
    finished_at: int = Field(alias="finishedAt")
    worker: str
    classify_result: list[str] | None = Field(alias="classifyResult")
    pow_result: str | None = Field(alias="powResult")

    @classmethod
    def from_models(cls, worker: str, job: DataJob, result: WorkerJobResult):
        return cls(
            db_id=job.job_id,
            job_type=job.job_type,
            classify_ctx=job.classify_ctx,
            pow_ctx=job.pow_ctx,
            published_at=job.published_at,
            publisher=job.publisher,
            finished_at=int(time.time()),
            worker=worker,
            classify_result=result.classify_result,
            pow_result=result.pow_result,
        )


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
