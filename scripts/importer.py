import gzip
import hashlib
import json
import os
from datetime import datetime
import queue
import threading
import zlib

import boto3
from pymongo import MongoClient
import requests
from warcio.archiveiterator import ArchiveIterator
import requests

from scripts.models import WetMetadata

R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
R2_DATA_BUCKET_NAME = "mizu-cmc-compressed"
R2_BACKCUP_BUCKET_NAME = "mongo-backup"

COMMON_CRAWL_URL_PREFIX = "https://data.commoncrawl.org"
CLOUDFRONT_URL_PREFIX = "https://d3k3zv3epk7z9d.cloudfront.net"

NUM_OF_THREADS = int(os.environ.get("NUM_OF_THREADS", 32))
CC_MONGO_URL = os.environ["CC_MONGO_URL"]
CC_MONGO_DB_NAME = "commoncrawl"


def get_cc_s3_file(filepath: str):
    client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    return client.get_object(Bucket="commoncrawl", Key=filepath)


class WetRecord(object):
    def __init__(self, record: any):
        self.languages = (
            record.rec_headers.get_header("WARC-Identified-Content-Language") or ""
        ).split(",")
        self.content_length = record.rec_headers.get_header("Content-Length")
        self.uri = record.rec_headers.get_header("WARC-Target-URI")
        self.warc_id = record.rec_headers.get_header("WARC-Record-ID")
        record_date = record.rec_headers.get_header("WARC-Date")
        self.crawled_at = int(
            round(datetime.strptime(record_date, "%Y-%m-%dT%H:%M:%SZ").timestamp())
        )
        self.text = record.content_stream().read().decode("utf-8")


class Progress(object):
    def __init__(
        self,
        filepath: str,
        warc_id: str = None,
        next_chunk: int = 0,
        total_processed: int = 0,
        finished: bool = False,
    ):
        self.filepath = filepath
        self.last_warc_id = warc_id
        self.next_chunk = next_chunk
        self.total_processed = total_processed
        self.finished = finished

    def from_doc(doc):
        return Progress(
            doc["_id"],
            doc["last_warc_id"],
            doc["next_chunk"],
            doc["total_processed"],
            finished=doc["finished"],
        )


class RecordBatch(object):
    def __init__(self, filename: str):
        self.filename = filename
        self.records = []
        self.bytesize = 0


class CommonCrawlWetImporter(threading.Thread):
    def __init__(self, wid: int, source: str, batch: str, q: queue.Queue):
        super().__init__()
        self.wid = wid
        self.batch = batch
        self.source = source
        self.q = q
        self.s3 = boto3.resource(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
        )
        self.mclient = MongoClient(CC_MONGO_URL)
        self.progress_coll = self.mclient[CC_MONGO_DB_NAME]["progress"]
        self.r2_metadata = self.mclient[CC_MONGO_DB_NAME]["metadata"]

    def _get_progress(self, filepath: str) -> Progress | None:
        doc = self.progress_coll.find_one({"_id": filepath})
        return Progress.from_doc(doc) if doc else Progress(filepath)

    def _save_chunk(
        self,
        cached: RecordBatch,
        progress: Progress,
    ):
        r2_key = self._gen_r2_key(cached.filename, str(progress.next_chunk))
        print(
            f"Thread {self.wid}: writing chunk {progress.next_chunk} at {cached.filename}"
        )
        compressed = zlib.compress("\n".join(cached.records).encode("utf-8"))
        self.s3.meta.client.put_object(
            Bucket=R2_DATA_BUCKET_NAME,
            Key=r2_key,
            Body=compressed,
            ContentLength=len(compressed),
            Metadata={
                "chunk_size": str(len(cached.records)),
                "decompressed_bytesize": str(cached.bytesize),
            },
        )
        metadata = WetMetadata(
            id=hashlib.sha256(r2_key.encode("utf-8")).hexdigest(),
            batch=self.batch,
            filename=cached.filename,
            chunk=progress.next_chunk,
            chunk_size=len(cached.records),
            bytesize=len(compressed),
            decompressed_bytesize=cached.bytesize,
            md5=hashlib.md5(compressed).hexdigest(),
        )
        self.r2_metadata.update_one(
            {
                "_id": metadata.id,
            },
            {"$set": metadata.model_dump()},
            upsert=True,
        )
        progress.next_chunk += 1
        self.progress_coll.update_one(
            {"_id": progress.filepath},
            {
                "$set": {
                    "last_warc_id": progress.last_warc_id,
                    "next_chunk": progress.next_chunk,
                    "total_processed": progress.total_processed,
                    "finished": progress.finished,
                }
            },
            upsert=True,
        )

    def _gen_r2_key(self, filename: str, chunk: int):
        return os.path.join(self.batch, "wet", filename, str(chunk) + ".zz")

    def fetch_http_warc_file(self, prefix: str, filepath: str):
        resp = requests.get(f"{prefix}/{filepath}", stream=True)
        resp.raise_for_status()
        for record in ArchiveIterator(resp.raw):
            yield record

    def fetch_s3_warc_file(self, filepath: str):
        obj = get_cc_s3_file(filepath)
        for record in ArchiveIterator(obj["Body"]):
            yield record

    def iterate_warc_file(self, filepath: str):
        progress = self._get_progress(filepath)
        if progress.finished:
            print(f"Thread {self.wid}: {filepath} already processed, skipping...")
            return

        print(f"Thread {self.wid}: processing {filepath}")
        cached = RecordBatch(filepath.rsplit("/", 1)[-1])
        resuming = progress.next_chunk > 0

        if self.source == "s3":
            cursor = self.fetch_s3_warc_file(filepath)
        elif self.source == "cc":
            cursor = self.fetch_http_warc_file(COMMON_CRAWL_URL_PREFIX, filepath)
        else:
            cursor = self.fetch_http_warc_file(CLOUDFRONT_URL_PREFIX, filepath)

        for record in cursor:
            warc_id = record.rec_headers.get_header("WARC-Record-ID")
            if resuming and warc_id != progress.last_warc_id:
                continue
            elif resuming and warc_id == progress.last_warc_id:
                resuming = False
                continue

            progress.last_warc_id = warc_id
            progress.total_processed += 1
            if record.rec_type == "conversion":
                wet_record = WetRecord(record)
                r = json.dumps(wet_record.__dict__)
                cached.records.append(r)
                cached.bytesize += len(r)
            else:
                print(
                    f"Thread {self.wid}: skip non-conversion type {record.rec_type} with id {warc_id}"
                )

            # with raw data > 20MB, we got ~5MB after compression
            if cached.bytesize > 20 * 1024 * 1024:
                self._save_chunk(cached, progress)
                cached = RecordBatch(cached.filename)

        if len(cached.records) > 0:
            self._save_chunk(cached, progress)

        self.progress_coll.update_one(
            {"_id": progress.filepath},
            {
                "$set": {
                    "finished": True,
                }
            },
        )

    def run(self):
        while True:
            try:
                filepath = self.q.get(timeout=3)  # 3s timeout
                print(
                    f"Thread {self.wid}: one job taken, current queue size {self.q.qsize()}"
                )
                self.iterate_warc_file(filepath)
                self.q.task_done()
            except queue.Empty:
                return


def import_to_r2(source: str, pathfile: str, start: int, end: int):
    q = queue.Queue()
    if source == "s3":
        obj = get_cc_s3_file(pathfile)
        extracted = gzip.decompress(obj["Body"].read())
        lines = [line.decode() for line in extracted.split(b"\n")]
    else:
        url = (
            f"{COMMON_CRAWL_URL_PREFIX}/{pathfile}"
            if source == "cc"
            else f"{CLOUDFRONT_URL_PREFIX}/{pathfile}"
        )
        res = requests.get(url, stream=True)
        extracted = gzip.decompress(res.content)
        lines = [line.decode() for line in extracted.split(b"\n")]

    for i in range(start, end):
        q.put_nowait(lines[i].strip())

    for wid in range(NUM_OF_THREADS):
        CommonCrawlWetImporter(wid, source, "CC-MAIN-2024-42", q).start()


class CommonCrawlWetMigrator(threading.Thread):
    def __init__(self, q: queue.Queue):
        super().__init__()
        self.q = q
        self.r2_url_prefix = "https://rawdata.mizu.technology"
        self.mclient = MongoClient(CC_MONGO_URL)
        self.r2_metadata = self.mclient[CC_MONGO_DB_NAME]["metadata"]
        self.s3 = boto3.resource(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
        )

    def produce(self):
        for doc in self.r2_metadata.find({"decompressed_bytesize": {"$exists": False}}):
            print("id: ", doc["_id"])
            self.q.put_nowait(doc["_id"])

    def update_metadata(self, doc_id: str):
        print(f"processing record {doc_id}, current queue size is {self.q.qsize()}")
        doc = self.r2_metadata.find_one({"_id": doc_id})
        r2_key = f"{doc['batch']}/{doc['type']}/{doc['filename']}/{doc['chunk']}.zz"
        with requests.get(f"{self.r2_url_prefix}/{r2_key}") as res:
            decompressed = zlib.decompress(res.content)
            self.r2_metadata.update_one(
                {
                    "_id": hashlib.sha256(r2_key.encode("utf-8")).hexdigest(),
                },
                {
                    "$set": {
                        "decompressed_bytesize": len(decompressed),
                    }
                },
                upsert=True,
            )
            self.r2_metadata.update_one({"_id": r2_key}, {"$set": {"migrated": True}})

    def run(self):
        while True:
            try:
                r2_key = self.q.get(timeout=3)  # 3s timeout
            except queue.Empty:
                return
            self.update_metadata(r2_key)
            self.q.task_done()


def migrate():
    q = queue.Queue()
    CommonCrawlWetMigrator(q).produce()
    threads = []
    for _ in range(NUM_OF_THREADS):
        threads.append(CommonCrawlWetMigrator(q))
        threads[-1].start()

    for t in threads:
        t.join()


class CommonCrawlWetMetadataUploader(object):
    def __init__(self, cc_batch: str, batch_size: int = 50000):
        super().__init__()
        self.cc_batch = cc_batch
        self.batch_size = batch_size
        self.s3 = boto3.resource(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
        )
        self.mclient = MongoClient(CC_MONGO_URL)
        self.r2_metadata = self.mclient[CC_MONGO_DB_NAME]["metadata"]
        self.filter = {"batch": self.cc_batch, "type": "wet"}

    def upload_files(self, batch: list[str], batch_num: int):
        compressed = zlib.compress("\n".join(batch).encode("utf-8"))
        self.s3.meta.client.put_object(
            Bucket=R2_BACKCUP_BUCKET_NAME,
            Key=f"{self.cc_batch}/wet/metadata.{batch_num}.zz",
            Body=compressed,
            ContentLength=len(compressed),
        )

    def datetime_handler(self, obj):
        if isinstance(obj, datetime):
            return (
                obj.isoformat()
            )  # Convert to ISO format string like "2024-03-14T15:30:00"
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    def iterate_and_upload(self, processed=0, batch_num=0):
        print(f"Processed {processed} records")
        docs = list(
            self.r2_metadata.find(self.filter).skip(processed).limit(self.batch_size)
        )
        batches = [json.dumps(doc, default=self.datetime_handler) for doc in docs]
        if len(batches) > 0:
            self.upload_files(batches, batch_num)

        if len(batches) < self.batch_size:
            return
        else:
            self.iterate_and_upload(processed + len(batches), batch_num + 1)
