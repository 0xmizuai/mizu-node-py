from enum import Enum
from typing import Literal
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
    reward = 3


class JobStatus(int, Enum):
    pending = 0
    finished = 1
    error = 2


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


class Token(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    chain: str
    address: str
    protocol: Literal["ERC20", "ERC721", "ERC1155"]


class RewardContext(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    # None if the reward is mizu points
    token: Token | None = Field(default=None)
    amount: int
    # signature of worker id + token + amount
    signature: str | None = Field(default=None)


class JobConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    max_retry: int = Field(alias="maxRetry", default=3)


class DataJobPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_type: JobType = Field(alias="jobType")
    job_config: JobConfig = Field(alias="jobConfig", default=JobConfig())
    classify_ctx: ClassifyContext | None = Field(alias="classifyCtx", default=None)
    pow_ctx: PowContext | None = Field(alias="powCtx", default=None)
    batch_classify_ctx: BatchClassifyContext | None = Field(
        alias="batchClassifyCtx", default=None
    )
    reward_ctx: RewardContext | None = Field(alias="rewardCtx", default=None)


class PublishJobRequest(BaseModel):
    data: list[DataJobPayload]


class QueryJobRequest(BaseModel):
    job_ids: list[str] = Field(alias="jobIds")


class RewardResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    signature: str
    address: str | None = Field(default=None)


class ErrorCode(int, Enum):
    reserved = 0
    max_retry_exceeded = 1


class ErrorResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    code: ErrorCode
    message: str | None = Field(default=None)


class WorkerJobResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="_id")
    job_type: JobType = Field(alias="jobType")
    classify_result: list[str] | None = Field(alias="classifyResult", default=None)
    pow_result: str | None = Field(alias="powResult", default=None)
    batch_classify_result: list[ClassifyResult] | None = Field(
        alias="batchClassifyResult", default=None
    )
    reward_result: RewardResult | None = Field(alias="rewardResult", default=None)
    error_result: ErrorResult | None = Field(alias="errorResult", default=None)


class DataJob(DataJobPayload, WorkerJobResult):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="_id")
    status: JobStatus
    finished_at: int | None = Field(alias="finishedAt", default=None)
    worker: str | None = Field(default=None)
    published_at: int | None = Field(alias="publishedAt", default=None)
    publisher: str | None = Field(default=None)


class WorkerJob(DataJobPayload):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="_id")
