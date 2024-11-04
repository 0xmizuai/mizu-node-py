import argparse
from datetime import datetime
import os
import queue
import threading

from fastapi.encoders import jsonable_encoder
from pymongo import MongoClient
import requests

from mizu_node.types.common import JobType
from mizu_node.types.data_job import (
    BatchClassifyContext,
    ClassifyContext,
    DataJobPayload,
    PublishJobRequest,
)


NUM_OF_THREADS = int(os.environ.get("NUM_OF_THREADS", 32))
MONGO_URL = os.environ["MONGO_URL"]
MONGO_DB_NAME = "commoncrawl"

SERVICE_URL = os.environ["SERVICE_URL"]


def _build_job(r2_key: str, chunk_size: int):
    return DataJobPayload(
        job_type=JobType.batch_classify,
        batch_classify_ctx=BatchClassifyContext(
            r2_key=r2_key,
            chunk_size=chunk_size,
            labels=["label1", "label2"],
            embedding_mode="average",
        ),
    )


def _build_publish_job_request(num_jobs: int):
    return PublishJobRequest(data=[_build_job() for _ in range(num_jobs)])


def publish_jobs(api_key: str, num_jobs: int):
    req = _build_publish_job_request(num_jobs)
    result = requests.post(
        SERVICE_URL + "/publish_jobs",
        json=jsonable_encoder(req),
        headers={"Authorization": "Bearer " + api_key},
    )
    jobs_id = result.json()["data"]["job_ids"]
    return [(job_id, req.job_type) for job_id, req in zip(jobs_id, req.data)]


class CommonCrawlWetPublisher(threading.Thread):
    def __init__(self, wid: int, api_key: str):
        super().__init__()
        self.api_key = api_key
        self.wid = wid
        self.mclient = MongoClient(MONGO_URL)
        self.r2_metadata = self.mclient[MONGO_DB_NAME]["metadata"]

    def process(self):
        records = self.r2_metadata.find(
            {"type": "wet", "published_at": {"$exists": True}}
        ).limit(1000)
        publish_jobs(self.api_key, records)
        for record in records:
            self.r2_metadata.update_one(
                {"_id": record["_id"]},
                {"$set": {"published_at": datetime.now()}},
            )

    def run(self):
        while True:
            try:
                doc = self.q.get_nowait()
            except queue.Empty:
                break
            self.process(doc)
            self.q.task_done()


parser = argparse.ArgumentParser()
parser.add_argument("--total", type=int, action="store")
args = parser.parse_args()


def main():
    q = queue.Queue()
    for i in range(args.total):
        q.put_nowait(i)
    for wid in range(NUM_OF_THREADS):
        CommonCrawlWetPublisher(wid).start()
