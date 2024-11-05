import argparse
import gzip
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

COMMON_CRAWL_URL_PREFIX = "https://data.commoncrawl.org"

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
        cached: list[str],
        r2_key: str,
        progress: Progress,
    ):
        print(f"Thread {self.wid}: writing chunk {progress.next_chunk}")
        compressed = zlib.compress("\n".join(cached).encode("utf-8"))
        self.s3.meta.client.put_object(
            Bucket=R2_BUCKET_NAME, Key=r2_key, Body=compressed
        )
        self.r2_metadata.update_one(
            {
                "_id": r2_key,
            },
            {
                "$set": {
                    "type": "wet",
                    "chunk_size": len(cached),
                    "created_at": datetime.now(),
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

        filename = filepath.rsplit("/", 1)[-1]
        cached_size = 0
        cached: list[WetRecord] = []
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
                r = json.dumps(WetRecord(record).__dict__)
                cached.append(r)
                cached_size += len(r)
            else:
                print(
                    f"Thread {self.wid}: skip non-conversion type {record.rec_type} with id {warc_id}"
                )

            # with raw data > 20MB, we got ~5MB after compression
            if cached_size > 20 * 1024 * 1024:
                r2_key = self._gen_r2_key(filename, str(progress.next_chunk))
                self._save_chunk(cached, r2_key, progress)
                cached_size = 0
                cached = []

        if len(cached) > 0:
            r2_key = self._gen_r2_key(filename, str(progress.next_chunk))
            self._save_chunk(cached, r2_key, progress)

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
            except queue.Empty:
                return
            self.iterate_warc_file(filepath)
            self.q.task_done()


def download_large_file(url, destination):
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("File downloaded successfully!")
    except requests.exceptions.RequestException as e:
        print("Error downloading the file: ", e)


parser = argparse.ArgumentParser()
parser.add_argument("--range", type=str, action="store", help="e.g. 10,20")
parser.add_argument("--url", type=str, action="store", help="URL to download")
args = parser.parse_args()


def main():
    q = queue.Queue()
    with requests.get(args.url, stream=True) as res:
        extracted = gzip.decompress(res.content)
        lines = [line.decode() for line in extracted.split(b"\n")]
    [start, end] = [int(i) for i in args.range.split(",")]
    for i in range(start, end):
        q.put_nowait(lines[i].strip())
    for wid in range(NUM_OF_THREADS):
        CommonCrawlWetImporter(wid, "CC-MAIN-2024-42", q).start()
