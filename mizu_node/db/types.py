import json
from pydantic import BaseModel


class AIRuntimeConfig(BaseModel):
    debug: bool = False
    callback_url: str = None


# All following _id should be the hash of the data to classify


class ClassificationJobForWorker(BaseModel):
    _id: str
    config: AIRuntimeConfig


class ClassificationJobResultFromWorker(BaseModel):
    _id: str
    worker: str
    tags: list[str]


class ClassificationJobFromPublisher(BaseModel):
    _id: str
    publisher: str
    created_at: int


class ProcessingJob(ClassificationJobFromPublisher):
    worker: str
    assigned_at: int


class ClassificationJobResult(ProcessingJob):
    finished_at: int
    tags: list[str]
