import asyncio
import logging
import os
import random
import signal

import redis.asyncio as redis
from psycopg_pool import AsyncConnectionPool

from mizu_node.db.query import get_unpublished_queries, update_query_status_async
from mizu_node.types.query import QueryStatus


class QueryProcessor:

    def __init__(
        self,
        query_db_pool: AsyncConnectionPool,
        jobs_db_pool: AsyncConnectionPool,
        redis_url: str,
        max_concurrent_tasks: int = 10,
    ):
        self.interval_seconds = 10
        self.max_concurrent_tasks = max_concurrent_tasks
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self.query_db_pool = query_db_pool
        self.jobs_db_pool = jobs_db_pool
        self.redis_url = redis_url
        self.active_tasks = set()

        self.redis = redis.from_url(redis_url)

        self.shutdown_event = asyncio.Event()

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    async def fetch_entries(self, limit: int):
        if self.shutdown_event.is_set():
            return []

        async with self.query_db_pool.connection() as conn:
            return await get_unpublished_queries(conn, limit)

    async def process_entry(self, entry):
        try:
            async with self.semaphore:
                try:
                    if self.shutdown_event.is_set():
                        self.logger.info(f"Skipping {entry.id} dues to shutdown event")
                        return

                    self.logger.info(f"Query {entry.id}")
                    seconds = random.randint(20, 50)
                    self.logger.info(
                        f"Simulating some processing: this takes {seconds}s"
                    )
                    await asyncio.sleep(seconds)
                    self.logger.info(f"Done sleeping {seconds}s")
                except Exception as e:
                    self.logger.error(f"Error while processing {entry.id}: {str(e)}")

        finally:
            current_task = asyncio.current_task()
            if current_task in self.active_tasks:
                self.active_tasks.remove(current_task)

    def _handle_shutdown(self, _signum, _frame):
        self.logger.info("Received shutdown signal, stopping..")
        asyncio.get_event_loop().call_soon_threadsafe(self.shutdown_event.set)

    async def shutdown(self):
        self.logger.info("Shutting down. Waiting for tasks to complete")

        if self.active_tasks:
            try:
                await asyncio.wait(self.active_tasks, timeout=30)
            except asyncio.TimeoutError:
                self.logger.warning("Some tasks did not complete in time")

            remaining = {t for t in self.active_tasks if not t.done()}
            if remaining:
                self.logger.warning("Cancelling remaining tasks")
                for task in remaining:
                    task.cancel()

                await asyncio.gather(*remaining, return_exceptions=True)

        await self.query_db_pool.close()
        await self.redis.aclose()

        self.logger.info("Shutdown complete")

    async def run(self):
        self.logger.info("Starting query processor..")
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_shutdown)

        try:
            while not self.shutdown_event.is_set():
                self.active_tasks = {
                    task for task in self.active_tasks if not task.done()
                }
                n_active_tasks = len(self.active_tasks)
                limit = self.max_concurrent_tasks - n_active_tasks
                self.logger.info(
                    f"{n_active_tasks} active tasks, {self.max_concurrent_tasks} avail concurrent tasks"
                )
                if n_active_tasks < self.max_concurrent_tasks:
                    self.logger.info(f"Fetching {limit} queries from DB")
                    entries = await self.fetch_entries(limit)
                    if not entries:
                        self.logger.info("Nothing to process")
                        await asyncio.sleep(5)
                        continue

                    for entry in entries:
                        if self.shutdown_event.is_set():
                            break

                        # Set query status as "processing"
                        entry.status = QueryStatus.processing
                        async with self.query_db_pool.connection() as conn:
                            await update_query_status_async(conn, entry)

                        task = asyncio.create_task(self.process_entry(entry))
                        self.active_tasks.add(task)
                else:
                    self.logger.info("Not enough workers available")

                await asyncio.sleep(self.interval_seconds)

        except Exception as e:
            self.logger.error(f"Unexpected error in main loop: {str(e)}")
            raise

        finally:
            await self.shutdown()


async def start():
    logging.getLogger("asyncio").setLevel(logging.INFO)
    postgres_query_url = os.environ["POSTGRES_QUERY_URL"]
    postgres_jobs_url = os.environ["POSTGRES_JOBS_URL"]
    redis_url = os.environ["REDIS_URL"]
    query_db_pool = AsyncConnectionPool(
        postgres_query_url, min_size=1, max_size=5, open=False
    )
    await query_db_pool.open()
    jobs_db_pool = AsyncConnectionPool(
        postgres_jobs_url, min_size=1, max_size=5, open=False
    )
    await jobs_db_pool.open()

    qp = QueryProcessor(query_db_pool, jobs_db_pool, redis_url, max_concurrent_tasks=3)
    print("calling run")
    await qp.run()
    await query_db_pool.close()
    await jobs_db_pool.close()


def main():
    asyncio.run(start())
