import os

from bson import ObjectId
from pymongo import MongoClient

from mizu_node.db.r2 import read_raw_data
from mizu_node.job_handler import ClassificationJob

from typing import Union
from pydantic import BaseModel


class AIRuntimeConfig(BaseModel):
    debug: bool = False
    callback_url: str = None


class ClassificationJob(BaseModel):
    job_id: ObjectId
    data: str = None
    config: Union[AIRuntimeConfig, None] = None


class ClassificationJobResult(BaseModel):
    job_id: ObjectId
    tags: list[str]


FINISH_JOB_CALLBACK_URL = os.environ["FINISH_JOB_CALLBACK_URL"]
MONGO_URL = os.environ["MONGO_URL"]
MONGO_DB_NAME = os.environ["MONGO_DB_NAME"]

client = MongoClient(MONGO_URL)
db = client[MONGO_DB_NAME]


def get_pending_job_ids():
    ids = db.jobs.find({"status": "pending"}, {"_id": 1})
    return [i["_id"] for i in ids]


def take_job(job_id: str) -> ClassificationJob | None:
    metadata = db.jobs.find_one({"_id": job_id})
    text = read_raw_data(metadata["raw_data_key"])
    return ClassificationJob(
        job_id=job_id,
        data=text,
        config=AIRuntimeConfig(callback_url=FINISH_JOB_CALLBACK_URL),
    )


def new_job(publisher: str, data_key: str) -> str:
    return db.jobs.insert_one(
        {
            "raw_data_key": data_key,
            "publisher": publisher,
            "status": "pending",
            "tags": [],
        }
    ).inserted_id


def finish_job(result: ClassificationJobResult):
    db.jobs.update_one(
        {
            "_id": result.job_id,
            "status": "finished",
            "tags": result.tags,
        }
    )
