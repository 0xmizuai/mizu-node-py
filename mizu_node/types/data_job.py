from enum import Enum
from typing import Literal, Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator


class VerificationMode(str, Enum):
    none = "none"
    always = "always"
    random = "random"


class JobType(int, Enum):
    pow = 0
    classify = 1  # deprecated
    batch_classify = 2
    reward = 3


class JobStatus(int, Enum):
    pending = 0
    processing = 1
    finished = 2
    error = 3
    cached = 4


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
    classifier_id: int = Field(alias="classifierId")


class Token(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    chain: str
    address: str
    decimals: int = Field(default=18)
    protocol: Literal["ERC20", "ERC721", "ERC1155"]


class RewardContext(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    # None if the reward is mizu points
    token: Token | None = Field(default=None)
    amount: str | float | int


class DataJobContext(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    classify_ctx: Optional[ClassifyContext] = Field(alias="classifyCtx", default=None)
    pow_ctx: Optional[PowContext] = Field(alias="powCtx", default=None)
    batch_classify_ctx: Optional[BatchClassifyContext] = Field(
        alias="batchClassifyCtx", default=None
    )
    reward_ctx: Optional[RewardContext] = Field(alias="rewardCtx", default=None)


class DataJobContextWithValidator(DataJobContext):
    model_config = ConfigDict(populate_by_name=True)

    job_type: JobType = Field(alias="jobType")

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


class ClassifyResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    uri: str
    text: str


class DataJobResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    classify_result: Optional[list[str]] = Field(alias="classifyResult", default=None)
    pow_result: Optional[str] = Field(alias="powResult", default=None)
    batch_classify_result: Optional[list[ClassifyResult]] = Field(
        alias="batchClassifyResult", default=None
    )
    reward_result: Optional[RewardResult] = Field(alias="rewardResult", default=None)
    error_result: Optional[ErrorResult] = Field(alias="errorResult", default=None)


class DataJobResultWithValidator(DataJobResult):
    model_config = ConfigDict(populate_by_name=True)

    job_type: JobType = Field(alias="jobType")

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


class WorkerJob(DataJobContextWithValidator):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str | int = Field(alias="_id")


class WorkerJobResult(DataJobResultWithValidator):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str | int = Field(alias="_id")
