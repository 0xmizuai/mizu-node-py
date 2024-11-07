import argparse
import os
import threading

from fastapi.encoders import jsonable_encoder
from pymongo import MongoClient
import requests

from mizu_node.types.common import JobType
from mizu_node.types.data_job import (
    BatchClassifyContext,
    DataJob,
    DataJobPayload,
    PublishJobRequest,
)

MONGO_URL = os.environ["MONGO_URL"]
MONGO_DB_NAME = "commoncrawl"
SERVICE_URL = os.environ["SERVICE_URL"]
DATA_LINK_PREFIX = "https://rawdata.mizu.technology/"


def _build_batch_classify_job(doc: any, classifier_id: str):
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


class CommonCrawlWetPublisher(threading.Thread):
    def __init__(
        self, api_key: str, batch: str, classifier_id: str, batch_size: int = 1000
    ):
        super().__init__()
        self.batch = batch
        self.batch_size = batch_size
        self.api_key = api_key
        self.classifier_id = classifier_id
        self.mclient = MongoClient(MONGO_URL)
        self.source = self.mclient[MONGO_DB_NAME]["metadata"]
        self.dest = self.mclient[MONGO_DB_NAME]["published_jobs"]

    def _publish(self, batch: list[DataJobPayload]):
        req = PublishJobRequest(data=batch)
        result = requests.post(
            SERVICE_URL + "/publish_jobs",
            json=jsonable_encoder(req),
            headers={"Authorization": "Bearer " + self.api_key},
        )
        job_ids = result.json()["data"]["job_ids"]
        self.dest.insert_many(
            [
                jsonable_encoder(DataJob.from_job_payload("self", payload, job_id))
                for job_id, payload in zip(job_ids, batch)
            ]
        )

    def run(self):
        batch = []
        for doc in self.source.find({"batch": self.batch}):
            if self.dest.count_documents({"_id": doc["_id"]}) > 0:
                continue
            batch.append(_build_batch_classify_job(doc, self.classifier_id))
            if len(batch) == self.batch_size:
                self._publish(batch)

        if len(batch) > 0:
            self._publish(batch)


parser = argparse.ArgumentParser()
parser.add_argument("--api_key", type=str, action="store")
parser.add_argument("--batch", type=str, action="store")
parser.add_argument("--classifier", type=str, action="store")
args = parser.parse_args()


def start():
    CommonCrawlWetPublisher(args.api_key, args.batch, args.classifier).start()
