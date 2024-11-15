from enum import Enum
from pydantic import BaseModel, ConfigDict, Field

from mizu_node.types.classifier import ClassifyResult


class VerificationMode(str, Enum):
    none = "none"
    always = "always"
    random = "random"


class JobType(int, Enum):
    pow = 0
    classify = 1
    batch_classify = 2


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
    byte_size: int = Field(alias="byteSize")
    decompressed_byte_size: int = Field(alias="decompressedByteSize")
    checksum_md5: str = Field(alias="checksumMd5")
    classifier_id: str = Field(alias="classifierId")


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


class DataJob(DataJobPayload):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="_id")
    classify_result: list[str] | None = Field(alias="classifyResult", default=None)
    pow_result: str | None = Field(alias="powResult", default=None)
    batch_classify_result: list[ClassifyResult] | None = Field(
        alias="batchClassifyResult", default=None
    )
    finished_at: int | None = Field(alias="finishedAt", default=None)
    worker: str | None = Field(default=None)
    published_at: int | None = Field(alias="publishedAt", default=None)
    publisher: str | None = Field(default=None)


class WorkerJob(DataJobPayload):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="_id")


class WorkerJobResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="_id")
    job_type: JobType = Field(alias="jobType")
    classify_result: list[str] | None = Field(alias="classifyResult", default=None)
    pow_result: str | None = Field(alias="powResult", default=None)
    batch_classify_result: list[ClassifyResult] | None = Field(
        alias="batchClassifyResult", default=None
    )
    finished_at: int | None = Field(alias="finishedAt", default=None)
