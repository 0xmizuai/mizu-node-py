import argparse
import asyncio
import datetime
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

WORKER_URL = "https://cf-worker.mizu.global"


async def call_http(
    url: str, max_retries: int = 3, base_delay: float = 1.0
) -> Optional[dict | bytes]:
    connector = aiohttp.TCPConnector(
        limit=500, ttl_dns_cache=300, keepalive_timeout=1000
    )
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            for attempt in range(max_retries):
                try:
                    if attempt > 0:
                        print(f"Retrying {url}")
                    async with session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(
                            total=1000, connect=120, sock_connect=60, sock_read=1800
                        ),
                    ) as response:
                        response.raise_for_status()
                        if response.content_type == "application/json":
                            return await response.json()
                        else:
                            return await response.read()
                except aiohttp.ClientConnectorError as e:
                    if attempt == max_retries - 1:
                        print(
                            f"Failed to resolve DNS for {url} after {max_retries} attempts:"
                        )
                        print(f"Error details: {str(e)}")
                        raise
                    delay = base_delay * (2**attempt) + random.uniform(
                        1, 3
                    )  # Longer delay for DNS issues
                    print(
                        f"DNS resolution failed for {url}. Retrying in {delay:.2f} seconds..."
                    )
                    await asyncio.sleep(delay)
                    continue
    finally:
        await connector.close()


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


async def importer(queues: dict[str, asyncio.Queue], worker_id: int):
    while True:
        try:
            result = await queues["unprocessed"].get()
            if result is None:  # Poison pill to stop worker
                break

            print(
                f"Importer {worker_id} processing {result} at {datetime.datetime.now()}"
            )
            try:
                response = await asyncio.wait_for(
                    call_http(
                        f"{WORKER_URL}?filepath={result}&dataset=CC-MAIN-2024-46",
                    ),
                    timeout=1000,
                )

                if not response:
                    print(f"Importer {worker_id}: No response received for {result}")
                    continue

                status = response.get("status")
                if status == "processed":
                    print(f"Importer {worker_id}: Already processed {result}")
                    await queues["processed"].put(result)
                    continue
                elif status == "processing":
                    print(
                        f"Importer {worker_id}: File {result} is already being processed"
                    )
                    await queues["processing"].put(result)
                    continue
                elif status == "ok":
                    print(f"Importer {worker_id}: Successfully processed {result}")
                    await queues["processed"].put(result)
                    continue
                elif status == "error":
                    print(f"Importer {worker_id}: Error processing {result}")
                    await queues["error"].put(result)
                    continue
                else:
                    print(f"Importer {worker_id}: Unknown status {status} for {result}")
                    await queues["error"].put(result)
                    continue

            except asyncio.TimeoutError:
                await queues["error"].put(result)
                print(f"Importer {worker_id}: Request timed out for {result}")
            except Exception as e:
                await queues["error"].put(result)
                print(f"Importer {worker_id}: Error processing {result}: {str(e)}")
            finally:
                queues["unprocessed"].task_done()
                print(
                    f"Importer {worker_id} completed task at {datetime.datetime.now()}. Queue size: {queues['unprocessed'].qsize()}"
                )

        except Exception as e:
            await queues["error"].put(result)
            print(f"Importer {worker_id} error processing {result}: {str(e)}")
            queues["unprocessed"].task_done()


async def run(offset: int = 0):
    url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-46/wet.paths.gz"
    results = await load_filepaths(url)
    print(f"Loaded {len(results)} records")

    # Create 10 queues and workers
    num_workers = 1000
    queues = {
        "unprocessed": asyncio.Queue(),
        "processing": asyncio.Queue(),
        "processed": asyncio.Queue(),
        "error": asyncio.Queue(),
    }

    # Start workers
    workers = [asyncio.create_task(importer(queues, i)) for i in range(num_workers)]

    # Add tasks to queue
    for result in results[offset:]:
        await queues["unprocessed"].put(result)

    # Add poison pills to stop workers
    for _ in range(num_workers):
        await queues["unprocessed"].put(None)

    # Wait for all tasks to complete
    await asyncio.gather(*workers)

    print(f"Unprocessed {queues['unprocessed'].qsize()} records")
    print(f"Processed {queues['processed'].qsize()} records")
    print(f"Error {queues['error'].qsize()} records")
    print(f"Processing {queues['processing'].qsize()} records")

    print("All tasks completed")


parser = argparse.ArgumentParser()
parser.add_argument("--offset", type=int, default=0)
args = parser.parse_args()


def main():
    asyncio.run(run(args.offset))
