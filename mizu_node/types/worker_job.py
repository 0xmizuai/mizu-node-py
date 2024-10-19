import time
from pydantic import BaseModel

from mizu_node.types.data_job import DataJob, JobType


class WorkerJob(BaseModel):
    job_id: str
    job_type: JobType
    input: str
    callback_url: str | None = None

    def from_data_job(job: DataJob, callback_url: str | None = None):
        return WorkerJob(
            job_id=job.job_id,
            job_type=job.job_type,
            input=job.input,
            callback_url=callback_url,
        )


class WorkerJobResult(BaseModel):
    job_id: str
    job_type: JobType
    output: str | list[str]  # serialized json
    worker: str = None


class FinishedJob(DataJob):
    finished_at: int
    worker: str
    output: str | list[str]  # could be serialized json

    def from_job_result(job: DataJob, result: WorkerJobResult):
        return FinishedJob(
            job_id=job.job_id,
            publisher=job.publisher,
            published_at=job.published_at,
            job_type=job.job_type,
            input=job.input,
            worker=result.worker,
            finished_at=int(time.time()),
            output=result.output,
        )
