import asyncio
import os
from pymongo import MongoClient
from pymongo.collection import Collection

import aiohttp

CC_MONGO_URL = os.environ["CC_MONGO_URL"]
CC_MONGO_DB_NAME = "commoncrawl"

LIMIT_PER_BATCH = 2
R2_WORKER_URL = os.environ["R2_WORKER_URL"]


async def process_one_file(url):
    print(f"Processing {url}")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return (await response.json())["metadata"]


async def process_one_batch(metadata_coll: Collection, offset: int, limit: int):
    records = list(metadata_coll.find({}).sort({"_id": 1}).skip(offset).limit(limit))
    urls = [
        f"{R2_WORKER_URL}?r2_key={r['batch']}/{r['type']}/{r['filename']}/{r['chunk']}.zz"
        for r in records
    ]
    tasks = [asyncio.create_task(process_one_file(url)) for url in urls]
    return await asyncio.gather(*tasks)


async def process_all(mclient: MongoClient):
    metadata_coll = mclient[CC_MONGO_DB_NAME]["metadata"]
    metadata_v2_coll = mclient[CC_MONGO_DB_NAME]["metadata_v2"]
    offset = 0
    total = metadata_coll.count_documents({})
    while offset < total:
        result = await process_one_batch(metadata_coll, offset, LIMIT_PER_BATCH)
        flattened = [x for xs in result for x in xs]
        metadata_v2_coll.insert_many(flattened)
        break
        # offset += len(responses)


def main():
    mclient = MongoClient(CC_MONGO_URL)

    asyncio.run(process_all(mclient))
