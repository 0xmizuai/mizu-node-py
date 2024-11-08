import json
import math
import os
import queue
import secrets
import threading
import time
from typing import Iterator
import zlib

from fastapi.encoders import jsonable_encoder
from pymongo import MongoClient
import requests

from mizu_node.constants import R2_DATA_PREFIX
from mizu_node.types.common import JobType
from mizu_node.types.data_job import (
    BatchClassifyContext,
    DataJob,
    DataJobPayload,
    PowContext,
    PublishJobRequest,
)

MONGO_URL = os.environ["MONGO_URL"]
MONGO_DB_NAME = "commoncrawl"
DATA_LINK_PREFIX = "https://rawdata.mizu.technology/"


NODE_SERVICE_URL = os.environ["NODE_SERVICE_URL"]


class DataJobPublisher(threading.Thread):
    def __init__(self, api_key: str):
        super().__init__()
        self.api_key = api_key

    def publish(self, jobs: list[DataJobPayload]) -> Iterator[str]:
        result = requests.post(
            NODE_SERVICE_URL + "/publish_jobs",
            json=jsonable_encoder(PublishJobRequest(data=jobs)),
            headers={"Authorization": "Bearer " + self.api_key},
        )
        for job_id in result.json()["data"]["job_ids"]:
            yield job_id

    def run(self):
        raise NotImplementedError


class PowDataJobPublisher(DataJobPublisher):
    def __init__(
        self,
        api_key: str,
        num_of_threads: int,
        batch_size: int = 100,
        cool_down: int = 600,  # 600 seconds
        threshold: int = 100000,  # auto-publish when queue length is below 100000
    ):
        super().__init__(api_key)
        self.batch_size = batch_size
        self.threshold = threshold
        self.num_of_threads = num_of_threads
        self.cool_down = cool_down

    def _build_pow_job_payload(self):
        return DataJobPayload(
            job_type=JobType.pow,
            pow_ctx=PowContext(difficulty=5, seed=secrets.token_hex(32)),
        )

    def check_queue_stats(self):
        result = requests.post(
            NODE_SERVICE_URL + "/stats/queue_len?job_type=0",
        )
        length = result.json()["data"]["length"]
        if length > self.threshold:
            return
        return math.ceil((self.threshold - length) / self.num_of_threads)

    def publish_in_batches(self, jobs: list[DataJobPayload]) -> Iterator[str]:
        total = 0
        while total < len(jobs):
            if total < self.batch_size:
                self.publish(jobs[total:-1])
                return
            else:
                self.publish(jobs[total : total + self.batch_size])
                total += self.batch_size

    def run(self):
        while True:
            num_of_jobs = self.check_queue_stats()
            jobs = [self._build_pow_job_payload() for _ in range(num_of_jobs)]
            self.publish_in_batches(jobs)
            time.sleep(self.cool_down)


class CommonCrawlDataJobPublisher(DataJobPublisher):

    def __init__(
        self,
        api_key: str,
        q: queue.Queue,
        cc_batch: str,
        classifier_id: str,
        batch_size: int = 100,
        cool_down: int = 600,
    ):
        super().__init__(api_key)
        self.cc_batch = cc_batch
        self.q = q
        self.batch_size = batch_size
        self.classifier_id = classifier_id
        self.cool_down = cool_down
        self.mclient = MongoClient(MONGO_URL)
        self.jobs_coll = self.mclient[MONGO_DB_NAME]["published_jobs"]

    def _build_batch_classify_job(self, doc: any, classifier_id: str):
        r2_key = f"{doc['batch']}/{doc['type']}/{doc['filename']}/{doc['chunk']}.zz"
        return DataJobPayload(
            job_type=JobType.batch_classify,
            batch_classify_ctx=BatchClassifyContext(
                data_url=f"{DATA_LINK_PREFIX}/{r2_key}",
                batch_size=doc["chunk_size"],
                bytesize=doc["bytesize"],
                checksum_md5=doc["md5"],
                decompressed_bytesize=doc["decompressed_bytesize"],
                classifer_id=classifier_id,
            ),
        )

    def query_status(self):
        for doc in self.dest.find({"finished_at": {"$exists": False}}):
            result = requests.post(
                NODE_SERVICE_URL + "/query_jobs",
                json=jsonable_encoder({"job_ids": [doc["_id"]]}),
                headers={"Authorization": "Bearer " + self.api_key},
            )
            if result.json()["data"][0]["finished_at"]:
                self.dest.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"finished_at": result.json()["data"][0]["finished_at"]}},
                )

    def publish_and_record(self, jobs: list[DataJobPayload]):
        job_ids = list(self.publish(jobs))
        self.jobs_coll.insert_many(
            [
                jsonable_encoder(DataJob.from_job_payload("", payload, job_id))
                for job_id, payload in zip(job_ids, jobs)
            ]
        )

    def publish_all(self, r2_key: str):
        with requests.get(f"{R2_DATA_PREFIX}/{r2_key}") as res:
            batch = []
            decompressed = zlib.decompress(res.content)
            for record_str in decompressed.decode("utf-8").split("\n"):
                record = json.loads(record_str)
                if self.jobs_coll.count_documents({"_id": record["_id"]}) > 0:
                    continue
                batch.append(self._build_batch_classify_job(record, self.classifier_id))
                if len(batch) == self.batch_size:
                    self.publish_and_record(batch)
            if len(batch) > 0:
                self.publish_and_record(batch)

    def run(self):
        while True:
            try:
                r2_key = self.q.get(timeout=3)  # 3s timeout
                self.publish_all(r2_key)
                time.sleep(self.cool_down)
            except queue.Empty:
                return
            self.q.task_done()


def publish_pow_jobs(api_key: str, num_of_threads: int):
    threads = []
    for _ in range(num_of_threads):
        threads.append(PowDataJobPublisher(api_key))
        threads[-1].start()
    for t in threads:
        t.join()


def publish_batch_classify_jobs(
    api_key: str, cc_batch: str, classifier_id: str, num_of_threads: int
):
    q = queue.Queue()
    with requests.get(f"{R2_DATA_PREFIX}/{cc_batch}/metadata.path.zz") as res:
        extracted = zlib.decompress(res.content)
        for line in [line.decode() for line in extracted.split(b"\n")]:
            q.put(line)

    threads = []
    for _ in range(num_of_threads):
        threads.append(CommonCrawlDataJobPublisher(api_key, q, cc_batch, classifier_id))
        threads[-1].start()
    for t in threads:
        t.join()
