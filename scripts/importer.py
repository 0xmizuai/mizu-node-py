import argparse
import asyncio
import os
import gzip
import io

import aiohttp
import random
from typing import Optional

R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
CF_KV_NAMESPACE_ID = os.environ["CF_KV_NAMESPACE_ID"]
CF_KV_API_TOKEN = os.environ["CF_KV_API_TOKEN"]

MAX_CONCURRENT_REQUESTS = int(os.environ.get("MAX_CONCURRENT_REQUESTS", "50"))
LIMIT_PER_BATCH = 100

WORKER_URL = "https://mizu-importer-0.shu-ecf.workers.dev"


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
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=1800)
                ) as response:
                    response.raise_for_status()  # Raise exception for bad status codes
                    if response.content_type == "application/json":
                        return await response.json()
                    else:
                        return await response.read()
        except aiohttp.ClientError as e:
            if attempt == max_retries - 1:  # Last attempt
                print(f"Failed to process {url} after {max_retries} attempts:")
                print(f"Error type: {type(e).__name__}")
                print(f"Error details: {str(e)}")
                if isinstance(e, aiohttp.ClientResponseError):
                    print(f"Status: {e.status}")
                    print(f"Message: {e.message}")
                raise e
            # Calculate exponential backoff with jitter
            delay = base_delay * (2**attempt) + random.uniform(0, 0.1)
            print(
                f"Attempt {attempt + 1} failed for {url} with error code {str(e)}. Retrying in {delay:.2f} seconds..."
            )
            await asyncio.sleep(delay)


async def load_filepaths(url: str) -> list[dict]:
    """
    Fetches a gzipped file from URL, decompresses it, and returns array of JSON objects
    """
    try:
        # Fetch the gzipped content
        compressed_content = await call_http(url)
        if not compressed_content:
            return []

        # Create a bytes IO object and decompress
        with gzip.GzipFile(fileobj=io.BytesIO(compressed_content), mode="rb") as gz:
            # Read all lines and parse JSONx
            results = []
            for line in gz:
                line = line.decode("utf-8").strip()  # Convert bytes to string
                if line:  # Skip empty lines
                    results.append(line)
            return results

    except Exception as e:
        print(f"Error processing gzipped file from {url}: {str(e)}")
        return []


async def worker(queue: asyncio.Queue, worker_id: int):
    while True:
        try:
            result = await queue.get()
            if result is None:  # Poison pill to stop worker
                break

            print(f"Worker {worker_id} processing {result}")
            while True:
                response = await call_http(
                    f"{WORKER_URL}?filepath={result}&dataset=CC-MAIN-2024-46"
                )
                if not response:
                    print(f"Worker {worker_id}: No response received for {result}")
                    break

                status = response.get("status")
                if status == "processed":
                    print(f"Worker {worker_id}: Already processed {result}")
                    break
                elif status == "processing":
                    print(
                        f"Worker {worker_id}: File {result} is already being processed, skipping"
                    )
                    break
                elif status == "pending":
                    print(
                        f"Worker {worker_id}: Status is pending for {result}, waiting 5 minutes before retry"
                    )
                    await asyncio.sleep(300)  # Wait 5 minutes
                elif status == "ok":
                    print(f"Worker {worker_id}: Successfully processed {result}")
                    break
                elif status == "error":
                    print(f"Worker {worker_id}: Error processing {result}")
                    break
                else:
                    print(f"Worker {worker_id}: Unknown status {status} for {result}")
                    break

            queue.task_done()
            print(f"Worker {worker_id} completed task. Queue size: {queue.qsize()}")
        except Exception as e:
            print(f"Worker {worker_id} error processing {result}: {str(e)}")
            queue.task_done()


async def run():
    url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-46/wet.paths.gz"
    results = await load_filepaths(url)
    print(f"Loaded {len(results)} records")

    # Create 10 queues and workers
    num_workers = 10
    queue = asyncio.Queue()

    # Start workers
    workers = [asyncio.create_task(worker(queue, i)) for i in range(num_workers)]

    # Add tasks to queue
    for result in results[0:11]:
        await asyncio.sleep(random.uniform(0, 1))  # Random delay between 0-5 seconds
        await queue.put(result)

    # Add poison pills to stop workers
    for _ in range(num_workers):
        await queue.put(None)

    # Wait for all tasks to complete
    await asyncio.gather(*workers)
    print("All tasks completed")


parser = argparse.ArgumentParser()
args = parser.parse_args()


def main():
    asyncio.run(run())
