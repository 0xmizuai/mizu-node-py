from typing import Union
from pydantic import BaseModel


class AIRuntimeConfig(BaseModel):
    debug: bool = False
    callback_url: str = None


class ClassificationJob(BaseModel):
    job_id: str
    text: str
    config: Union[AIRuntimeConfig, None] = None


class ClassificationJobResult(BaseModel):
    job_id: str
    tags: list[str]


def handle_take_job():
    return {"status": "ok"}


def handle_add_job(job: ClassificationJob):
    return {"status": "ok"}


def handle_finish_job(result: ClassificationJobResult):
    return {"status": "ok"}
