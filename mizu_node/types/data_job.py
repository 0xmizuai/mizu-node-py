from enum import Enum
from typing import Literal, Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator

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


class ErrorCode(int, Enum):
    reserved = 0
    max_retry_exceeded = 1


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
    decompressed_byte_size: int = Field(alias="decompressedByteSize")
    checksum_md5: str = Field(alias="checksumMd5")
    classifier_id: str = Field(alias="classifierId")


class Token(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    chain: str
    address: str
    decimals: int
    protocol: Literal["ERC20", "ERC721", "ERC1155"]


class RewardContext(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    # None if the reward is mizu points
    token: Token | None = Field(default=None)
    amount: str


class DataJobPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_type: JobType = Field(alias="jobType")
    classify_ctx: Optional[ClassifyContext] = Field(alias="classifyCtx", default=None)
    pow_ctx: Optional[PowContext] = Field(alias="powCtx", default=None)
    batch_classify_ctx: Optional[BatchClassifyContext] = Field(
        alias="batchClassifyCtx", default=None
    )
    reward_ctx: Optional[RewardContext] = Field(alias="rewardCtx", default=None)

    @model_validator(mode="after")
    def _validate_job_type(self):
        if self.job_type == JobType.reward and self.reward_ctx is None:
            raise ValueError("reward_ctx is required for reward job")
        if self.job_type == JobType.classify and self.classify_ctx is None:
            raise ValueError("classify_ctx is required for classify job")
        if self.job_type == JobType.pow and self.pow_ctx is None:
            raise ValueError("pow_ctx is required for pow job")
        if self.job_type == JobType.batch_classify and self.batch_classify_ctx is None:
            raise ValueError("batch_classify_ctx is required for batch_classify job")
        return self


class RewardResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    recipient: Optional[str] = Field(default=None)


class ErrorResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    code: ErrorCode
    message: Optional[str] = Field(default=None)


class JobResultBase(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_type: JobType = Field(alias="jobType")
    classify_result: Optional[list[str]] = Field(alias="classifyResult", default=None)
    pow_result: Optional[str] = Field(alias="powResult", default=None)
    batch_classify_result: Optional[list[ClassifyResult]] = Field(
        alias="batchClassifyResult", default=None
    )
    reward_result: Optional[RewardResult] = Field(alias="rewardResult", default=None)
    error_result: Optional[ErrorResult] = Field(alias="errorResult", default=None)


class JobResultBaseWithValidator(JobResultBase):
    @model_validator(mode="after")
    def _validate_job_type(self):
        if self.error_result is not None:
            return self

        if self.job_type == JobType.reward and self.reward_result is None:
            raise ValueError("reward_result is required for reward job")
        if self.job_type == JobType.classify and self.classify_result is None:
            raise ValueError("classify_result is required for classify job")
        if self.job_type == JobType.pow and self.pow_result is None:
            raise ValueError("pow_result is required for pow job")
        if (
            self.job_type == JobType.batch_classify
            and self.batch_classify_result is None
        ):
            raise ValueError("batch_classify_result is required for batch_classify job")
        return self


##################################### Client Data Type Start ###########################################


class WorkerJob(DataJobPayload):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="_id")


class WorkerJobResult(JobResultBaseWithValidator):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="_id")


##################################### Client Data Type End ##############################################


##################################### MONGODB Data Type Start ###########################################


class DataJobInputNoId(DataJobPayload):
    model_config = ConfigDict(populate_by_name=True)

    status: JobStatus = Field(default=JobStatus.pending)
    published_at: int = Field(alias="publishedAt")
    publisher: str


class DataJobResultNoId(JobResultBaseWithValidator):
    model_config = ConfigDict(populate_by_name=True)

    status: JobStatus
    finished_at: int = Field(alias="finishedAt")
    worker: str


class DataJobQueryResult(JobResultBase):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="_id")
    status: JobStatus = Field(default=JobStatus.pending)
    finished_at: Optional[int] = Field(alias="finishedAt", default=None)


##################################### MONGODB Data Type End ############################################


class RewardJobRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="_id")
    assigned_at: int = Field(alias="assignedAt")
    reward_ctx: RewardContext = Field(alias="rewardCtx")


class RewardJobRecords(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    jobs: list[RewardJobRecord] = Field(default=[])
