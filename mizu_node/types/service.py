from typing import Optional
from pydantic import BaseModel, ConfigDict, Field

from mizu_node.types.classifier import ClassifierConfig
from mizu_node.types.data_job import (
    BatchClassifyContext,
    DataJobQueryResult,
    JobType,
    PowContext,
    RewardContext,
    RewardResult,
    WorkerJob,
    WorkerJobResult,
)


class RegisterClassifierRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    config: ClassifierConfig


class RegisterClassifierResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str


class QueryClassifierResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    classifier: ClassifierConfig


class PublishPowJobRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    data: list[PowContext]


class PublishRewardJobRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    data: list[RewardContext]


class PublishBatchClassifyJobRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    data: list[BatchClassifyContext]


class PublishJobResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_ids: list[str] = Field(alias="jobIds")


class QueryJobResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    jobs: list[DataJobQueryResult]


class TakeJobResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job: Optional[WorkerJob]


class QueryQueueLenResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    length: int


class FinishJobRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_result: WorkerJobResult


class FinishJobResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    rewarded_points: int = Field(alias="rewardedPoints")


class SettleRewardRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="jobId")
    job_type: JobType = Field(alias="jobType")
    worker: str
    # only for reward job
    ctx: Optional[RewardContext]
    output: Optional[RewardResult]