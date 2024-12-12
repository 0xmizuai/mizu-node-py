from enum import Enum
from typing import Literal, Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator


# Enums
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


class ErrorCode(int, Enum):
    reserved = 0
    max_retry_exceeded = 1


# Base Context Models
class Token(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    chain: str
    address: str
    decimals: int = Field(default=18)
    protocol: Literal["ERC20", "ERC721", "ERC1155"]


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


class RewardContext(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    token: Optional[Token] = Field(default=None)  # None if the reward is mizu points
    amount: str | float | int


# Result Models
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


# Main Job Models
class DataJobContext(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    classify_ctx: Optional[ClassifyContext] = Field(alias="classifyCtx", default=None)
    pow_ctx: Optional[PowContext] = Field(alias="powCtx", default=None)
    batch_classify_ctx: Optional[BatchClassifyContext] = Field(
        alias="batchClassifyCtx", default=None
    )
    reward_ctx: Optional[RewardContext] = Field(alias="rewardCtx", default=None)


class DataJobResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    classify_result: Optional[list[str]] = Field(alias="classifyResult", default=None)
    pow_result: Optional[str] = Field(alias="powResult", default=None)
    batch_classify_result: Optional[list[ClassifyResult]] = Field(
        alias="batchClassifyResult", default=None
    )
    reward_result: Optional[RewardResult] = Field(alias="rewardResult", default=None)
    error_result: Optional[ErrorResult] = Field(alias="errorResult", default=None)


# Worker Models
class WorkerJob(DataJobContext):
    model_config = ConfigDict(populate_by_name=True)
    job_id: str | int = Field(alias="_id")
    job_type: JobType = Field(alias="jobType")

    @model_validator(mode="after")
    def validate_context(self):
        context_map = {
            JobType.reward: (self.reward_ctx, "reward_ctx"),
            JobType.classify: (self.classify_ctx, "classify_ctx"),
            JobType.pow: (self.pow_ctx, "pow_ctx"),
            JobType.batch_classify: (self.batch_classify_ctx, "batch_classify_ctx"),
        }
        if self.job_type in context_map:
            ctx, name = context_map[self.job_type]
            if ctx is None:
                raise ValueError(f"{name} is required for {self.job_type.name} job")
        return self


class WorkerJobResult(DataJobResult):
    model_config = ConfigDict(populate_by_name=True)
    job_id: str | int = Field(alias="_id")
    job_type: JobType = Field(alias="jobType")

    @model_validator(mode="after")
    def validate_result(self):
        if self.error_result is not None:
            return self

        result_map = {
            JobType.reward: (self.reward_result, "reward_result"),
            JobType.classify: (self.classify_result, "classify_result"),
            JobType.pow: (self.pow_result, "pow_result"),
        }
        if self.job_type in result_map:
            result, name = result_map[self.job_type]
            if result is None:
                raise ValueError(f"{name} is required for {self.job_type.name} job")
        return self
