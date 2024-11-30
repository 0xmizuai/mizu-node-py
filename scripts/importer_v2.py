import argparse
import asyncio
import hashlib
import json
import os
from pymongo import MongoClient

import aiohttp
import random
from typing import Optional

CC_MONGO_URL = os.environ["CC_MONGO_URL"]
CC_MONGO_DB_NAME = "commoncrawl"

LIMIT_PER_BATCH = 100

R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
CF_KV_NAMESPACE_ID = os.environ["CF_KV_NAMESPACE_ID"]
CF_KV_API_TOKEN = os.environ["CF_KV_API_TOKEN"]

MAX_CONCURRENT_REQUESTS = int(os.environ.get("MAX_CONCURRENT_REQUESTS", "50"))


def r2_kv_list_url(cursor: Optional[str] = None, limit: int = 1000):
    if cursor:
        return f"https://api.cloudflare.com/client/v4/accounts/{R2_ACCOUNT_ID}/storage/kv/namespaces/{CF_KV_NAMESPACE_ID}/keys?cursor={cursor}&limit={limit}"
    else:
        return f"https://api.cloudflare.com/client/v4/accounts/{R2_ACCOUNT_ID}/storage/kv/namespaces/{CF_KV_NAMESPACE_ID}/keys?limit={limit}"


def r2_kv_get_url(key_name: str):
    return f"https://api.cloudflare.com/client/v4/accounts/{R2_ACCOUNT_ID}/storage/kv/namespaces/{CF_KV_NAMESPACE_ID}/values/{key_name}"


async def call_http(
    url: str, token: str | None = None, max_retries: int = 3, base_delay: float = 1.0
) -> Optional[dict]:
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                print(f"Retrying {url}")
            connector = aiohttp.TCPConnector(limit=50)
            async with aiohttp.ClientSession(connector=connector) as session:
                if token:
                    session.headers["Authorization"] = f"Bearer {token}"
                async with session.get(url) as response:
                    response.raise_for_status()  # Raise exception for bad status codes
                    if response.content_type == "application/json":
                        return await response.json()
                    else:
                        return await response.read()
        except Exception as e:
            if attempt == max_retries - 1:  # Last attempt
                print(f"Failed to process {url} after {max_retries} attempts: {str(e)}")
                raise e

            # Calculate exponential backoff with jitter
            delay = base_delay * (2**attempt) + random.uniform(0, 0.1)
            print(
                f"Attempt {attempt + 1} failed for {url} with error code {str(e)}. Retrying in {delay:.2f} seconds..."
            )
            await asyncio.sleep(delay)


async def enqueue_all(mclient: MongoClient, offset: int):
    metadata_coll = mclient[CC_MONGO_DB_NAME]["metadata"]
    total = metadata_coll.count_documents({})
    while offset < total:
        records = list(
            metadata_coll.find({}).sort({"_id": 1}).skip(offset).limit(LIMIT_PER_BATCH)
        )
        if not records:
            break

        r2_keys = ",".join(
            [
                f"{r['batch']}/{r['type']}/{r['filename']}/{r['chunk']}.zz"
                for r in records
            ]
        )
        r2_worker_url = f"https://mizuai-queue-worker.shu-ecf.workers.dev"
        await call_http(f"{r2_worker_url}?r2_keys={r2_keys}")

        offset += len(records)
        print(f"enqueued {offset} of {total}")


async def get_all_files():
    with open("./names_diffs_v1.txt", "r") as f:
        lines = [line.strip() for line in f.readlines()]
        for i in range(0, len(lines), 100):
            r2_keys = ",".join(lines[i : i + 100])
            url = f"https://mizuai-queue-worker.shu-ecf.workers.dev?r2_keys={r2_keys}"
            print(f"fetching {url}")
            await call_http(url)


async def check_kv():
    with open("./names_diffs_v1.txt", "r") as f:
        lines = [line.strip() for line in f.readlines()]
        for line in lines:
            url = f"{r2_kv_get_url(line)}"
            try:
                await call_http(url, CF_KV_API_TOKEN, 1)
            except Exception as e:
                print(f"failed to get {line} with error {e}")


async def query_all_keys_from_r2():
    names = []
    cursor = None
    while True:
        url = r2_kv_list_url(cursor)
        print(f"fetching {url}")
        result = await call_http(url, CF_KV_API_TOKEN)
        if not result["success"]:
            print(result["errors"])
            print(result["messsages"])
            print("failed to query kv")
            return

        with open("names.txt", "a") as f:
            for name in result["result"]:
                f.write(name["name"] + "\n")

        if result["result_info"]["cursor"]:
            cursor = result["result_info"]["cursor"]

        if result["result_info"]["count"] < 1000:
            break


async def query_all_keys_from_db(mclient: MongoClient):
    metadata_coll = mclient[CC_MONGO_DB_NAME]["metadata"]
    with open("names_db.txt", "a") as f:
        for r in metadata_coll.find({}).sort({"_id": 1}):
            name = f"{r['batch']}/{r['type']}/{r['filename']}/{r['chunk']}.zz"
            f.write(name + "\n")


def _gen_id(batch: dict):
    return hashlib.md5(
        f"{batch['batch']}/{batch['type']}/{batch['filename']}/{batch['chunk']}.zz/{batch['subchunk']}.zz".encode(
            "utf-8"
        )
    ).hexdigest()


async def store_metadata(mclient: MongoClient, offset: int):
    metadata_coll = mclient[CC_MONGO_DB_NAME]["metadata"]
    metadata_v2_coll = mclient[CC_MONGO_DB_NAME]["metadata_v2"]
    total = metadata_coll.count_documents({})
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def process_record(record, f):
        async with semaphore:
            name = f"{record['batch']}/{record['type']}/{record['filename']}/{record['chunk']}.zz"
            url = f"{r2_kv_get_url(name)}"
            try:
                content = await call_http(url, CF_KV_API_TOKEN)
                batches = json.loads(content.decode("utf-8"))
                batches = [{"_id": _gen_id(batch), **batch} for batch in batches]
                metadata_v2_coll.insert_many(batches)
            except Exception as e:
                print(f"failed to get {name} with error {e}")
                f.write(f"{name}\n")

    with open("failed_names.txt", "a") as f:
        while True:
            records = list(
                metadata_coll.find({}).sort({"_id": 1}).skip(offset).limit(1000)
            )

            await asyncio.gather(*(process_record(record, f) for record in records))

            offset += len(records)
            print(f"stored {offset} of {total}")
            if len(records) < 1000:
                break


parser = argparse.ArgumentParser()

parser.add_argument("--offset", action="store", type=int, default=0)

args = parser.parse_args()


def main():
    mclient = MongoClient(CC_MONGO_URL)
    asyncio.run(store_metadata(mclient, args.offset))
