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


class BatchClassifyContext(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    data_url: str = Field(alias="dataUrl")
    batch_size: int = Field(alias="batchSize")
    bytesize: int = Field(alias="bytesize")
    decompressed_bytesize: int = Field(alias="decompressedBytesize")
    checksum_md5: str = Field(alias="checksumMd5")
    classifer_id: str = Field(alias="classiferId")


class DataJobPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_type: JobType = Field(alias="jobType")
    classify_ctx: ClassifyContext | None = Field(alias="classifyCtx", default=None)
    pow_ctx: PowContext | None = Field(alias="powCtx", default=None)
    batch_classify_ctx: BatchClassifyContext | None = Field(
        alias="batchClassifyCtx", default=None
    )


class PublishJobRequest(BaseModel):
    data: list[DataJobPayload]


class QueryJobRequest(BaseModel):
    job_ids: list[str] = Field(alias="jobIds")


class DataLabelResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    label: str
    score: float


class WetContext(BaseModel):
    warc_id: str = Field(alias="warcId")
    uri: str
    languages: list[str]
    crawled_at: int = Field(alias="crawledAt")


class ClassifyResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    wet_context: WetContext = Field(alias="wetContext")
    labels: list[DataLabelResult]


class WorkerJobResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="jobId")
    job_type: JobType = Field(alias="jobType")
    classify_result: list[str] | None = Field(alias="classifyResult", default=None)
    pow_result: str | None = Field(alias="powResult", default=None)
    batch_classify_result: list[ClassifyResult] | None = Field(
        alias="batchClassifyResult"
    )


class DataJob(DataJobPayload):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="_id")
    job_type: JobType = Field(alias="jobType")
    classify_ctx: ClassifyContext | None = Field(alias="classifyCtx")
    classify_result: list[str] | None = Field(alias="classifyResult")
    pow_ctx: PowContext | None = Field(alias="powCtx")
    pow_result: str | None = Field(alias="powResult")
    batch_classify_ctx: list[ClassifyResult] | None = Field(alias="batchClassifyCtx")
    batch_classify_result: list[str] | None = Field(alias="batchClassifyResult")
    published_at: int = Field(alias="publishedAt")
    publisher: str
    finished_at: int = Field(alias="finishedAt")
    worker: str

    @classmethod
    def from_job_payload(
        cls, publisher: str, payload: DataJobPayload, job_id: str | None = None
    ):
        return cls(
            job_id=job_id | str(uuid.uuid4()),
            job_type=payload.job_type,
            publisher=publisher,
            published_at=int(time.time()),
            classify_ctx=payload.classify_ctx,
            pow_ctx=payload.pow_ctx,
            batch_classify_ctx=payload.batch_classify_ctx,
        )


class WorkerJob(DataJobPayload):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="jobId")


def build_worker_job(job: DataJob) -> WorkerJob:
    return WorkerJob(
        job_id=job.job_id,
        job_type=job.job_type,
        classify_ctx=job.classify_ctx,
        pow_ctx=job.pow_ctx,
        batch_classify_ctx=job.batch_classify_ctx,
    )
