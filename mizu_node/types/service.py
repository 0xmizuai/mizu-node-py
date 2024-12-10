from typing import Optional
from pydantic import BaseModel, ConfigDict, Field

from mizu_node.types.data_job import (
    BatchClassifyContext,
    DataJobContext,
    DataJobResult,
    JobStatus,
    JobType,
    PowContext,
    RewardContext,
    Token,
    WorkerJob,
    WorkerJobResult,
)


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

    job_ids: list[int] = Field(alias="jobIds")


class DataJobQueryResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str | int = Field(alias="_id")
    job_type: JobType = Field(alias="jobType")
    context: DataJobContext
    result: DataJobResult | None = Field(default=None)
    status: JobStatus = Field(default=JobStatus.pending)
    worker: Optional[str] = Field(default=None)
    finished_at: Optional[int] = Field(alias="finishedAt", default=None)


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

    job_result: WorkerJobResult = Field(alias="jobResult")
    user: Optional[str] = Field(default=None)


class FinishJobResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    rewarded_points: float = Field(alias="rewardedPoints")


class QueryMinedPointsResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    points: float


class SettleRewardRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str | int = Field(alias="jobId")
    job_type: JobType = Field(alias="jobType")
    worker: str
    # only for reward job
    token: Optional[Token] = Field(default=None)
    amount: Optional[str] = Field(default=None)
    recipient: Optional[str] = Field(default=None)


class FinishJobV2Response(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    settle_reward: Optional[SettleRewardRequest] = Field(alias="settleReward")


class CooldownConfig:
    def __init__(self, interval: int, limit: int):
        self.interval = interval
        self.limit = limit


class RewardJobRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str | int = Field(alias="_id")
    assigned_at: int = Field(alias="assignedAt")
    lease_expired_at: int = Field(alias="leaseExpiredAt")
    reward_ctx: RewardContext = Field(alias="rewardCtx")


class QueryRewardJobsResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    jobs: list[RewardJobRecord] = Field(default=[])
