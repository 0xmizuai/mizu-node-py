from functools import wraps
import logging
import math
import os
import queue
import secrets
import threading
import time
from typing import Iterator
import zlib

import boto3
from pymongo import MongoClient
import requests

from mizu_node.constants import (
    API_KEY_COLLECTION,
    CLASSIFIER_COLLECTION,
    R2_DATA_PREFIX,
)
from mizu_node.types.classifier import ClassifierConfig, DataLabel
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

CC_MONGO_URL = os.environ["CC_MONGO_URL"]
CC_MONGO_DB_NAME = "commoncrawl"

MIZU_NODE_MONGO_URL = os.environ["MIZU_NODE_MONGO_URL"]
MIZU_NODE_MONGO_DB_NAME = "mizu_node"

PUBLISHED_JOBS_COLLECTION = "published_jobs"


def retry_with_backoff(max_retries=3, initial_delay=1, max_delay=30):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for retry in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, ValueError) as e:
                    if retry == max_retries - 1:  # Last retry
                        raise
                    wait = min(delay * (2**retry), max_delay)
                    logging.warning(
                        f"Attempt {retry + 1} failed: {str(e)}. Retrying in {wait} seconds..."
                    )
                    time.sleep(wait)
            return func(*args, **kwargs)  # Final attempt

        return wrapper

    return decorator


class DataJobPublisher(threading.Thread):
    def __init__(self, api_key: str, service_url: str | None = None):
        super().__init__()
        self.api_key = api_key
        self.service_url = service_url or os.environ.get(
            "NODE_SERVICE_URL", "http://127.0.0.1:8000"
        )

    @retry_with_backoff(max_retries=3, initial_delay=2, max_delay=30)
    def publish(self, jobs: list[DataJobPayload]) -> Iterator[str]:
        try:
            response = requests.post(
                self.service_url + "/publish_jobs",
                json=PublishJobRequest(data=jobs).model_dump(by_alias=True),
                headers={"Authorization": "Bearer " + self.api_key},
                timeout=30,  # Added timeout
            )
            response.raise_for_status()  # Raises HTTPError for bad status codes

            data = response.json()
            if "data" not in data or "jobIds" not in data["data"]:
                raise ValueError("Invalid response format")

            for job_id in data["data"]["jobIds"]:
                yield job_id

        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error occurred: {str(e)}, Response: {response.text}")
            raise ValueError(f"Failed to publish jobs: {str(e)}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error occurred: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            raise ValueError(f"Failed to publish jobs: {str(e)}")

    def run(self):
        raise NotImplementedError


class PowDataJobPublisher(DataJobPublisher):
    def __init__(
        self,
        api_key: str,
        num_of_threads: int,
        batch_size: int = 100,
        cool_down: int = 10,  # 10 seconds
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
        max_processed_jobs: int,
        batch_size: int = 100,
        cool_down: int = 3,
        service_url: str | None = None,
    ):
        super().__init__(api_key, service_url)
        self.cc_batch = cc_batch
        self.q = q
        self.batch_size = batch_size
        self.max_processed_jobs = max_processed_jobs
        self.classifier_id = classifier_id
        self.cool_down = cool_down
        self.mclient = MongoClient(MIZU_NODE_MONGO_URL)
        self.jobs_coll = self.mclient[MIZU_NODE_MONGO_DB_NAME][
            PUBLISHED_JOBS_COLLECTION
        ]

    def _build_batch_classify_job(self, doc: WetMetadata, classifier_id: str):
        r2_key = f"{doc.batch}/{doc.type}/{doc.filename}/{doc.chunk}.zz"
        return DataJobPayload(
            job_type=JobType.batch_classify,
            batch_classify_ctx=BatchClassifyContext(
                data_url=f"{R2_DATA_PREFIX}/{r2_key}",
                batch_size=doc.chunk_size,
                byte_size=doc.byte_size,
                checksum_md5=doc.md5,
                decompressed_byte_size=doc.decompressed_byte_size,
                classifier_id=classifier_id,
            ),
        )

    def publish_and_record(self, metadatas: list[WetMetadata]):
        jobs = [
            self._build_batch_classify_job(metadata, self.classifier_id)
            for metadata in metadatas
        ]
        job_ids = list(self.publish(jobs))
        if job_ids:
            job_records = [
                ClientJobRecord(
                    id=metadata.id,
                    batch=self.cc_batch,
                    classifier_id=self.classifier_id,
                    metadata_type=metadata.type,
                    job_id=job_id,
                    job_type=JobType.batch_classify,
                    created_at=int(time.time()),
                )
                for job_id, metadata in zip(job_ids, metadatas)
            ]
            self.jobs_coll.insert_many(
                [record.model_dump(by_alias=True) for record in job_records]
            )

    def publish_all(self, metadatas: list[WetMetadata]):
        batch = []
        print("processing metadatas")
        ids = [metadata.id for metadata in metadatas]
        docs = list(self.jobs_coll.find({"_id": {"$in": ids}}, {"_id": 1}))
        processed_ids = set([doc["_id"] for doc in docs])
        for metadata in metadatas:
            if metadata.id in processed_ids:
                continue
            batch.append(metadata)
            if len(batch) == self.batch_size:
                print(f"will publish {len(metadatas)} jobs")
                self.publish_and_record(batch)
                self.max_processed_jobs -= len(batch)
                batch = []
        if len(batch) > 0:
            print(f"will publish {len(metadatas)} jobs")
            self.publish_and_record(batch)
            self.max_processed_jobs -= len(batch)
        return False

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
        print("publisher running")
        while True:
            print("collecting metadatas")
            metadatas = list(self.get_batch())
            published = self.publish_all(metadatas)
            if self.max_processed_jobs <= 0:
                print("max_processed_jobs exceeded, exiting")
                return
            if len(metadatas) < self.batch_size:
                return
            if published:
                time.sleep(self.cool_down)


class CommonCrawlDataJobManager(threading.Thread):
    def __init__(
        self,
        q: queue.Queue,
        cc_batch: str,
        metadata_type: str,
        classifier_id: str,
        num_of_publishers: int,
        max_processed_jobs: int,
    ):
        super().__init__()
        self.q = q
        self.cc_batch = cc_batch
        self.metadata_type = metadata_type
        self.classifier_id = classifier_id
        self.num_of_publishers = num_of_publishers
        self.max_processed_jobs = max_processed_jobs
        self.total_processed = 0
        self.mclient = MongoClient(CC_MONGO_URL)
        self.jobs_coll = self.mclient[CC_MONGO_DB_NAME][PUBLISHED_JOBS_COLLECTION]
        self.s3 = boto3.resource(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
        )

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
            json={"job_ids": [str(doc["_id"]) for doc in pending_jobs]},
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
        obj = self.s3.meta.client.get_object(
            Bucket=R2_BACKCUP_BUCKET_NAME, Key=filepath
        )
        decompressed = zlib.decompress(obj["Body"].read())
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
            print(f"Loader: enqueued {self.total_processed} jobs")
            if self.total_processed >= self.max_processed_jobs:
                print("max_processed_jobs exceeded, exiting")
                break

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
        print("manager running")
        self.load_metadatas()
        self.wait_until_queue_empty()
        while True:
            self.query_status()
            if self.count_finished_jobs() >= self.total_processed:
                return
            time.sleep(60)


def get_api_key(user: str):
    mclient = MongoClient(MIZU_NODE_MONGO_URL)
    api_keys = mclient[MIZU_NODE_MONGO_DB_NAME][API_KEY_COLLECTION]
    doc = api_keys.find_one({"user": user})
    if doc is None:
        raise ValueError(f"User {user} not found")
    return doc["api_key"]


def publish_pow_jobs(user: str, num_of_threads: int = 32):
    api_key = get_api_key(user)
    threads = []
    for _ in range(num_of_threads):
        threads.append(PowDataJobPublisher(api_key))
        threads[-1].start()
    for t in threads:
        t.join()


def publish_batch_classify_jobs(
    user: str,
    cc_batch: str,
    classifier_id: str,
    metadata_type: str = "wet",
    num_of_threads: int = 1,
    max_processed_jobs: int = 1000,
):
    api_key = get_api_key(user)
    q = queue.Queue()
    manager = CommonCrawlDataJobManager(
        q, cc_batch, metadata_type, classifier_id, num_of_threads, max_processed_jobs
    )
    manager.start()

    publishers = []
    for _ in range(num_of_threads):
        publisher = CommonCrawlDataJobPublisher(
            api_key=api_key,
            q=q,
            cc_batch=cc_batch,
            classifier_id=classifier_id,
            max_processed_jobs=max_processed_jobs,
            batch_size=100,
        )
        publisher.start()
        publishers.append(publisher)

    # Wait for all threads to complete
    for publisher in publishers:
        publisher.join()
    manager.join()


def register_classifier(user: str):
    api_key = get_api_key(user)
    config = ClassifierConfig(
        name="default",
        embedding_model="Xenova/all-MiniLM-L6-v2",
        labels=[
            DataLabel(
                label="web3_legal",
                description="laws, compliance, and policies for digital assets and blockchain.",
            ),
            DataLabel(
                label="javascript",
                description="a programming language commonly used to create interactive effects within web browsers.",
            ),
            DataLabel(
                label="resume",
                description="structured summary of a person's work experience, education, and skills.",
            ),
            DataLabel(
                label="anti-ai",
                description="criticism or arguments against AI technology and its societal impacts.",
            ),
            DataLabel(
                label="adult video",
                description="explicit digital content created for adult entertainment purposes.",
            ),
        ],
    )
    response = requests.post(
        f"{os.environ['NODE_SERVICE_URL']}/register_classifier",
        json=config.model_dump(by_alias=True),
        headers={"Authorization": "Bearer " + api_key},
    )
    response.raise_for_status()
    return response.json()["data"]["id"]


def list_classifiers(user: str):
    mclient = MongoClient(MIZU_NODE_MONGO_URL)
    classifiers = mclient[MIZU_NODE_MONGO_DB_NAME][CLASSIFIER_COLLECTION]
    docs = list(classifiers.find({"publisher": user}))
    for doc in docs:
        config = ClassifierConfig(**doc)
        print(f"\nClassifier ID: {doc['_id']}")
        print(f"Name: {config.name}")
        print(f"Embedding Model: {config.embedding_model}")
        print("\nLabels:")
        for label in config.labels:
            print(f"  • {label.label}: {label.description}")
        print("-" * 80)
