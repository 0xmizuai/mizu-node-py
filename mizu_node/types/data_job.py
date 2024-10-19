from pydantic import BaseModel
import uuid

from mizu_node.types.common import JobType
from mizu_node.utils import epoch


class DataJobPayload(BaseModel):
    publisher: str
    published_at: int
    input: str  # could be serialized json


class PublishJobRequest(BaseModel):
    job_type: JobType
    jobs: list[DataJobPayload]  # could be serialized json


class DataJob(DataJobPayload):
    job_type: JobType
    job_id: str

    def from_payload(job: DataJobPayload, job_type: JobType):
        return DataJob(
            job_id=str(uuid.uuid4()),
            publisher=job.publisher,
            published_at=job.published_at,
            job_type=job_type,
            input=job.input,
        )
