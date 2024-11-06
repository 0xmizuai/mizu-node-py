import argparse
from datetime import datetime
import gzip
import os
import queue
import threading

from fastapi.encoders import jsonable_encoder
from pymongo import MongoClient
import requests

from mizu_node.types.common import JobType
from mizu_node.types.data_job import (
    BatchClassifyContext,
    DataJobPayload,
    PublishJobRequest,
)
from mizu_node.types.job_queue import JobQueue
from mizu_node.types.key_prefix import KeyPrefix


NUM_OF_THREADS = int(os.environ.get("NUM_OF_THREADS", 32))
MONGO_URL = os.environ["MONGO_URL"]
MONGO_DB_NAME = "commoncrawl"

SERVICE_URL = os.environ["SERVICE_URL"]

queue = JobQueue(KeyPrefix("commoncrawl:mizu"))


def _build_batch_classify_job(r2_key: str, chunk_size: int):
    return DataJobPayload(
        job_type=JobType.batch_classify,
        batch_classify_ctx=BatchClassifyContext(
            r2_key=r2_key,
            chunk_size=chunk_size,
            labels=["label1", "label2"],
            embedding_mode="average",
        ),
    )


def publish_batch_classify_jobs(api_key: str, batch_size: int = 1000):
    req = PublishJobRequest(
        data=[_build_batch_classify_job() for _ in range(batch_size)]
    )
    result = requests.post(
        SERVICE_URL + "/publish_jobs",
        json=jsonable_encoder(req),
        headers={"Authorization": "Bearer " + api_key},
    )
    jobs_id = result.json()["data"]["job_ids"]
    for job_id, req in zip(jobs_id, req.data):
        yield job_id, req.job_type


class CommonCrawlWetPublisher(threading.Thread):
    def __init__(self, mode: str, api_key: str, batch: str):
        super().__init__()
        self.mode = mode
        self.batch = batch
        self.api_key = api_key
        self.mclient = MongoClient(MONGO_URL)
        self.r2_metadata = self.mclient[MONGO_DB_NAME]["metadata"]
        self.jobs = self.mclient[MONGO_DB_NAME]["jobs"]

    def produce(self, doc):
        for doc in self.r2_metadata.find({"type": "wet", "batch": self.batch}):
            if self.jobs.find_one({"_id": doc["_id"]}):
                continue
            self.q.put_nowait(doc)

    def consume(self):
        publish_batch_classify_jobs(self.api_key, records)
        for record in records:
            self.r2_metadata.update_one(
                {"_id": record["_id"]},
                {"$set": {"published_at": datetime.now()}},
            )

    def consume(self):
        while True:
            try:
                doc = self.q.get_nowait()
            except queue.Empty:
                break
            self.process(doc)
            self.q.task_done()

    def run(self):
        if self.mode == "produce":
            self.produce()
        elif self.mode == "consume":
            self.consume()
        else:
            raise ValueError("Invalid mode")


parser = argparse.ArgumentParser()
parser.add_argument("--metadata_url", type=str, action="store")
parser.add_argument("--api_key", type=str, action="store")
parser.add_argument("--batch", type=str, action="store")
args = parser.parse_args()


def start():
    CommonCrawlWetPublisher("produce", args.api_key, args.batch, q).start()
    CommonCrawlWetPublisher("consume", args.api_key, args.batch, q).start()
