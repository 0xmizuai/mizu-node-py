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
from mizu_node.types.job import (
    BatchClassifyContext,
    DataJobPayload,
    JobType,
    PowContext,
    PublishJobRequest,
)
from scripts.importer import (
    R2_ACCESS_KEY,
    R2_ACCOUNT_ID,
    R2_BACKCUP_BUCKET_NAME,
    R2_SECRET_KEY,
)
from scripts.models import ClientJobRecord, WetMetadata

MONGO_URL = os.environ["CC_MONGO_URL"]
MONGO_DB_NAME = "commoncrawl"
PUBLISHED_JOBS_COLLECTION = "published_jobs"
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
        self.jobs_coll = self.mclient[MONGO_DB_NAME][PUBLISHED_JOBS_COLLECTION]

    def _build_batch_classify_job(self, doc: WetMetadata, classifier_id: str):
        r2_key = f"{doc.batch}/{doc.type}/{doc.filename}/{doc.chunk}.zz"
        return DataJobPayload(
            job_type=JobType.batch_classify,
            batch_classify_ctx=BatchClassifyContext(
                data_url=f"{DATA_LINK_PREFIX}/{r2_key}",
                batch_size=doc.chunk_size,
                bytesize=doc.bytesize,
                checksum_md5=doc.md5,
                decompressed_bytesize=doc.decompressed_bytesize,
                classifer_id=classifier_id,
            ),
        )

    def publish_and_record(self, jobs: list[(WetMetadata, DataJobPayload)]):
        job_ids = list(self.publish([payload for _, payload in jobs]))
        job_records = [
            ClientJobRecord(
                id=metadata.id,
                batch=self.cc_batch,
                classifier_id=self.classifier_id,
                metadata_type=metadata.type,
                job_id=job_id,
                job_type=payload.job_type,
                created_at=int(time.time()),
            )
            for job_id, (metadata, payload) in zip(job_ids, jobs)
        ]
        self.jobs_coll.insert_many([record.model_dump_json() for record in job_records])

    def publish_all(self, metadatas: list[WetMetadata]):
        batch = []
        for metadata in metadatas:
            if self.jobs_coll.count_documents({"_id": metadata.id}) > 0:
                continue
            job = self._build_batch_classify_job(metadata, self.classifier_id)
            batch.append((metadata.id, job))
            if len(batch) == self.batch_size:
                self.publish_and_record(batch)
                batch = []
        if len(batch) > 0:
            self.publish_and_record(batch)

    def get_one(self, retry=10) -> WetMetadata | None:
        if retry == 0:
            raise ValueError("No more metadata")
        try:
            return self.q.get(timeout=3)
        except queue.Empty:
            time.sleep(3)
            return self.get_one(retry - 1)

    def get_batch(self) -> Iterator[WetMetadata]:
        for _ in range(self.batch_size):
            metadata = self.get_one()
            if metadata is None:
                return
            else:
                yield metadata

    def run(self):
        while True:
            metadatas = list(self.get_batch())
            self.publish_all(metadatas)
            if len(metadatas) < self.batch_size:
                return
            time.sleep(self.cool_down)


class CommonCrawlDataJobManager(threading.Thread):
    def __init__(
        self,
        q: queue.Queue,
        cc_batch: str,
        metadata_type: str,
        classifier_id: str,
        num_of_publishers: int,
    ):
        self.q = q
        self.cc_batch = cc_batch
        self.metadata_type = metadata_type
        self.classifier_id = classifier_id
        self.num_of_publishers = num_of_publishers
        self.total_processed = 0
        self.mclient = MongoClient(MONGO_URL)
        self.jobs_coll = self.mclient[MONGO_DB_NAME][PUBLISHED_JOBS_COLLECTION]

    def query_status(self):
        pending_jobs = list(
            self.jobs_coll.find(
                {
                    "batch": self.cc_batch,
                    "metadata_type": self.metadata_type,
                    "classifier_id": self.classifier_id,
                    "created_at": {"$gt": int(time.time()) - 1800},  # wait for 30mins
                    "finished_at": {"$exists": False},
                }
            )
            .sort("created_at", 1)
            .limit(1000)
        )
        if len(pending_jobs) == 0:
            return

        result = requests.post(
            self.service_url + "/job_status",
            json=jsonable_encoder({"job_ids": [doc["_id"] for doc in pending_jobs]}),
            headers={"Authorization": "Bearer " + self.api_key},
        )
        for job_result in result.json()["data"]:
            self.dest.update_one(
                {"_id": job_result["job_id"]},
                {
                    "$set": {
                        "finished_at": job_result["finished_at"],
                        "batch_classify_result": job_result["batch_classify_result"],
                    }
                },
            )

        # if all jobs are finished, load more metadatas
        if len(result.json()["data"]) == len(pending_jobs):
            self.query_status()

    def load_one_file(self, filepath: str):
        with requests.get(f"{R2_DATA_PREFIX}/{filepath}") as res:
            decompressed = zlib.decompress(res.content)
            lines = decompressed.decode("utf-8").split("\n")
            for record_str in lines:
                self.q.put_nowait(WetMetadata.model_validate_json(record_str))
            self.total_processed += len(lines)

    def load_metadatas(self):
        s3 = boto3.resource(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
        )
        bucket = s3.Bucket(R2_BACKCUP_BUCKET_NAME)
        objs = bucket.objects.filter(Prefix=f"{self.cc_batch}")
        for obj in objs:
            self.load_one_file(obj.key)
            print(f"Loader: enqueued {self.total_jobs} jobs")

        for _ in range(self.num_of_publishers):
            self.q.put_nowait(None)  # for publisher to exit

    def wait_until_queue_empty(self):
        while not self.q.empty():
            time.sleep(30)

    def count_finished_jobs(self):
        return self.jobs_coll.count_documents(
            {
                "batch": self.cc_batch,
                "metadata_type": self.metadata_type,
                "classifier_id": self.classifier_id,
                "finished_at": {"$exists": True},
            }
        )

    def run(self):
        self.load_metadatas()
        self.wait_until_queue_empty()
        while True:
            self.query_status()
            if self.count_finished_jobs() >= self.total_processed:
                return
            time.sleep(60)


def publish_pow_jobs(api_key: str, num_of_threads: int = 32):
    threads = []
    for _ in range(num_of_threads):
        threads.append(PowDataJobPublisher(api_key))
        threads[-1].start()
    for t in threads:
        t.join()


def publish_batch_classify_jobs(
    api_key: str,
    cc_batch: str,
    classifier_id: str,
    metadata_type: str = "wet",
    num_of_threads: int = 32,
):
    q = queue.Queue()
    CommonCrawlDataJobManager(q, cc_batch, metadata_type, num_of_threads).start()
    for _ in range(num_of_threads):
        CommonCrawlDataJobPublisher(
            api_key, q, cc_batch, classifier_id, metadata_type
        ).start()
