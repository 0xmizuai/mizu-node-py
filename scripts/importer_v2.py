import argparse
import asyncio
from datetime import datetime
import os
from pymongo import MongoClient

import aiohttp
import random
from typing import Optional

CC_MONGO_URL = os.environ["CC_MONGO_URL"]
CC_MONGO_DB_NAME = "commoncrawl"

LIMIT_PER_BATCH = 20
R2_WORKER_URL = os.environ["R2_WORKER_URL"]


async def call_http(
    url: str, max_retries: int = 3, base_delay: float = 1.0
) -> Optional[dict]:
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                print(f"Retrying {url}")
            connector = aiohttp.TCPConnector(limit=50)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url) as response:
                    response.raise_for_status()  # Raise exception for bad status codes
                    return await response.json()
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


async def process_one_batch(records: list[dict]):
    urls = [
        f"{R2_WORKER_URL}?r2_key={r['batch']}/{r['type']}/{r['filename']}/{r['chunk']}.zz"
        for r in records
    ]
    tasks = [asyncio.create_task(call_http(url)) for url in urls]
    return await asyncio.gather(*tasks)


async def process_all(mclient: MongoClient, offset: int):
    metadata_coll = mclient[CC_MONGO_DB_NAME]["metadata"]
    metadata_v2_coll = mclient[CC_MONGO_DB_NAME]["metadata_v2"]
    total = metadata_coll.count_documents({})
    while offset < total:
        records = list(
            metadata_coll.find({}).sort({"_id": 1}).skip(offset).limit(LIMIT_PER_BATCH)
        )
        result = await process_one_batch(records)
        metadatas = [m for r in result for m in r["metadata"]]
        flattened = [
            {
                **m,
                "created_at": datetime.fromisoformat(m["created_at"]),
            }
            for m in metadatas
        ]
        metadata_v2_coll.insert_many(flattened)
        offset += len(records)
        print(f"Processed {offset} of {total}")
        await asyncio.sleep(random.uniform(0, 3))


async def enqueue_all(mclient: MongoClient, offset: int):
    metadata_coll = mclient[CC_MONGO_DB_NAME]["metadata"]
    total = metadata_coll.count_documents({})
    while offset < total:
        records = list(
            metadata_coll.find({}).sort({"_id": 1}).skip(offset).limit(LIMIT_PER_BATCH)
        )
        r2_keys = [
            f"{r['batch']}/{r['type']}/{r['filename']}/{r['chunk']}.zz" for r in records
        ]
        print(f"{R2_WORKER_URL}?r2_keys={','.join(r2_keys)}")
        await call_http(f"{R2_WORKER_URL}?r2_keys={','.join(r2_keys)}")
        print(f"Enqueued {offset} of {total}")
        offset += len(records)
        await asyncio.sleep(random.uniform(0, 3))
        break


parser = argparse.ArgumentParser()

parser.add_argument("--offset", action="store", type=int, default=0)

args = parser.parse_args()


def main():
    mclient = MongoClient(CC_MONGO_URL)
    asyncio.run(enqueue_all(mclient, args.offset))
