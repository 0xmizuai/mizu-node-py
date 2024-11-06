import argparse
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


R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
R2_BUCKET_NAME = "mizu-cmc-compressed"

COMMON_CRAWL_URL_PREFIX = "https://ds5q9oxwqwsfj.cloudfront.net"

NUM_OF_THREADS = int(os.environ.get("NUM_OF_THREADS", 32))
MONGO_URL = os.environ["MONGO_URL"]
MONGO_DB_NAME = "commoncrawl"


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
    def __init__(self, wid: int, batch: str, q: queue.Queue):
        super().__init__()
        self.wid = wid
        self.batch = batch
        self.q = q
        self.s3 = boto3.resource(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
        )
        self.mclient = MongoClient(MONGO_URL)
        self.progress_coll = self.mclient[MONGO_DB_NAME]["progress"]
        self.r2_metadata = self.mclient[MONGO_DB_NAME]["metadata"]

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
            Bucket=R2_BUCKET_NAME,
            Key=r2_key,
            Body=compressed,
            ContentLength=len(compressed),
            Metadata={
                "chunk_size": str(len(cached.records)),
                "decompressed_bytesize": str(cached.bytesize),
            },
        )
        self.r2_metadata.update_one(
            {
                "_id": hashlib.sha256(r2_key.encode("utf-8")).hexdigest(),
            },
            {
                "$set": {
                    "type": "wet",
                    "batch": self.batch,
                    "filename": cached.filename,
                    "chunk": progress.next_chunk,
                    "chunk_size": len(cached.records),
                    "bytesize": len(compressed),
                    "decompressed_bytesize": cached.bytesize,
                    "created_at": datetime.now(),
                    "md5": hashlib.md5(compressed).hexdigest(),
                }
            },
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

    def iterate_warc_file(self, filepath: str):
        progress = self._get_progress(filepath)
        if progress.finished:
            print(f"Thread {self.wid}: {filepath} already processed, skipping...")
            return

        print(f"Thread {self.wid}: processing {filepath}")
        resp = requests.get(f"{COMMON_CRAWL_URL_PREFIX}/{filepath}", stream=True)
        resp.raise_for_status()

        cached = RecordBatch(filepath.rsplit("/", 1)[-1])
        resuming = progress.next_chunk > 0
        for record in ArchiveIterator(resp.raw):
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
                self.iterate_warc_file(filepath)
                self.q.task_done()
            except queue.Empty:
                return


class CommonCrawlWetMigrator(threading.Thread):
    def __init__(self, mode: str, q: queue.Queue):
        super().__init__()
        self.mode = mode
        self.q = q
        self.r2_url_prefix = "https://rawdata.mizu.technology"
        self.mclient = MongoClient(MONGO_URL)
        self.r2_metadata = self.mclient[MONGO_DB_NAME]["metadata"]

    def produce(self):
        for doc in self.r2_metadata.find(
            {"filename": {"$exists": False}, "migrated": {"$ne": True}}
        ):
            if doc["_id"].startswith("CC-MAIN-2024-42"):
                self.q.put_nowait(doc["_id"])

    def update_metadata(self, r2_key: str):
        print(f"processing {r2_key}")
        [batch, wtype, filename, chunk] = r2_key.split("/")
        doc = self.r2_metadata.find_one({"_id": r2_key})
        with requests.get(f"{self.r2_url_prefix}/{r2_key}") as res:
            decompressed = zlib.decompress(res.content)
            self.r2_metadata.update_one(
                {
                    "_id": hashlib.sha256(r2_key.encode("utf-8")).hexdigest(),
                },
                {
                    "$set": {
                        "type": wtype,
                        "batch": batch,
                        "filename": filename,
                        "chunk": int(chunk.split(".")[0]),
                        "chunk_size": doc["chunk_size"],
                        "bytesize": len(res.content),
                        "decompressed_bytesize": len(decompressed),
                        "created_at": doc["created_at"],
                        "md5": hashlib.md5(res.content).hexdigest(),
                    }
                },
                upsert=True,
            )
            self.r2_metadata.update_one({"_id": r2_key}, {"$set": {"migrated": True}})

    def consume(self):
        while True:
            try:
                r2_key = self.q.get(timeout=3)  # 3s timeout
            except queue.Empty:
                return
            self.update_metadata(r2_key)
            self.q.task_done()

    def run(self):
        if self.mode == "producer":
            self.produce()
        elif self.mode == "consumer":
            self.consume()
        else:
            raise ValueError("Invalid mode")


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="command", required=True)

upload_parser = subparsers.add_parser(
    "upload", add_help=False, description="import data to r2"
)
upload_parser.add_argument("--range", type=str, action="store", help="e.g 10,20")
upload_parser.add_argument("--url", type=str, action="store", help="URL to download")

migrate_parser = subparsers.add_parser(
    "migrate", add_help=False, description="migrate metadata"
)
args = parser.parse_args()


def migrate():
    q = queue.Queue()
    CommonCrawlWetMigrator("producer", q).start()

    for _ in range(NUM_OF_THREADS):
        CommonCrawlWetMigrator("consumer", q).start()


def run_upload(url: str, start: int, end: int):
    q = queue.Queue()
    with requests.get(url, stream=True) as res:
        extracted = gzip.decompress(res.content)
        lines = [line.decode() for line in extracted.split(b"\n")]
    for i in range(start, end):
        q.put_nowait(lines[i].strip())

    for wid in range(NUM_OF_THREADS):
        CommonCrawlWetImporter(wid, "CC-MAIN-2024-42", q).start()


def main():
    if args.command == "upload":
        [start, end] = [int(i) for i in args.range.split(",")]
        run_upload(args.url, start, end)
    elif args.command == "migrate":
        migrate()
