import json
import math
import os
import queue
import secrets
import threading
import time
from typing import Iterator
import zlib

import boto3
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
from scripts.importer import (
    R2_ACCESS_KEY,
    R2_ACCOUNT_ID,
    R2_BACKCUP_BUCKET_NAME,
    R2_SECRET_KEY,
)

MONGO_URL = os.environ["CC_MONGO_URL"]
MONGO_DB_NAME = "commoncrawl"
DATA_LINK_PREFIX = "https://rawdata.mizu.technology/"


class DataJobPublisher(threading.Thread):
    def __init__(self, api_key: str, service_url: str | None = None):
        super().__init__()
        self.api_key = api_key
        self.service_url = service_url or os.environ.get(
            "NODE_SERVICE_URL", "http://localhost:8000"
        )

    def publish(self, jobs: list[DataJobPayload]) -> Iterator[str]:
        result = requests.post(
            self.service_url + "/publish_jobs",
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
        service_url: str | None = None,
    ):
        super().__init__(api_key, service_url)
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
            self.service_url + "/stats/queue_len?job_type=0",
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
        service_url: str | None = None,
    ):
        super().__init__(api_key, service_url)
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
                self.service_url + "/query_jobs",
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
                print("processing metadata file: ", r2_key)
                self.publish_all(r2_key)
                time.sleep(self.cool_down)
            except queue.Empty:
                return
            self.q.task_done()


class CommonCrawlMetadataLoader(threading.Thread):
    def __init__(self, cc_batch: str, q: queue.Queue):
        super().__init__()
        self.cc_batch = cc_batch
        self.q = q
        self.s3 = boto3.resource(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
        )

    def run(self):
        bucket = self.s3.Bucket(R2_BACKCUP_BUCKET_NAME)
        for obj in bucket.objects.filter(Prefix=f"{self.cc_batch}/"):
            self.q.put(obj.key)


def publish_pow_jobs(api_key: str, num_of_threads: int = 32):
    threads = []
    for _ in range(num_of_threads):
        threads.append(PowDataJobPublisher(api_key))
        threads[-1].start()
    for t in threads:
        t.join()


def publish_batch_classify_jobs(
    api_key: str, cc_batch: str, classifier_id: str, num_of_threads: int = 32
):
    q = queue.Queue()
    loader = CommonCrawlMetadataLoader(cc_batch, q)
    loader.start()

    publishers = []
    for _ in range(num_of_threads):
        publishers.append(
            CommonCrawlDataJobPublisher(api_key, q, cc_batch, classifier_id)
        )
        publishers[-1].start()

    loader.join()
    for t in publishers:
        t.join()
