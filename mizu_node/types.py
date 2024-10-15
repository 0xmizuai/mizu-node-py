from pydantic import BaseModel

# All following _id should be the hash of the data to classify


class ClassificationJobForWorker(BaseModel):
    key: str
    callback_url: str = None
    debug: bool = False


class ClassificationJobResult(BaseModel):
    key: str
    worker: str
    tags: list[str]


class ClassificationJobFromPublisher(BaseModel):
    key: str
    publisher: str
    created_at: int


class ProcessingJob(ClassificationJobFromPublisher):
    worker: str
    assigned_at: int


class ClassificationJobDBResult(ProcessingJob):
    finished_at: int
    tags: list[str]
