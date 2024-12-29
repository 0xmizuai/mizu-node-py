import asyncio
import logging
import os
import signal

import redis.asyncio as redis
from psycopg_pool import AsyncConnectionPool

from mizu_node.db.query import get_unpublished_queries


class QueryProcessor:

    def __init__(
        self,
        db_pool: AsyncConnectionPool,
        redis_url: str,
        max_concurrent_tasks: int = 10,
    ):
        self.interval_seconds = 10
        self.max_concurrent_tasks = max_concurrent_tasks
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self.db_pool = db_pool
        self.redis_url = redis_url
        self.active_tasks = set()

        self.redis = redis.from_url(redis_url)

        self.shutdown_event = asyncio.Event()

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    async def fetch_entries(self):
        if self.shutdown_event.is_set():
            return []

        async with self.db_pool.connection() as conn:
            return await get_unpublished_queries(conn)

    async def process_entry(self, entry):
        try:
            async with self.semaphore:
                try:
                    if self.shutdown_event.is_set():
                        self.logger.info(f"Skipping {entry.id} dues to shutdown event")
                        return

                    self.logger.warning(f"Entry {entry.id}")
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

        await self.db_pool.close()
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
                if len(self.active_tasks) < self.max_concurrent_tasks:
                    entries = await self.fetch_entries()
                    if not entries:
                        self.logger.info("Nothing to process")
                        await asyncio.sleep(5)
                        continue

                    for entry in entries:
                        if self.shutdown_event.is_set():
                            break
                        task = asyncio.create_task(self.process_entry(entry))
                        self.active_tasks.add(task)

                await asyncio.sleep(self.interval_seconds)

        except Exception as e:
            self.logger.error(f"Unexpected error in main loop: {str(e)}")
            raise

        finally:
            await self.shutdown()


async def start():
    logging.getLogger("asyncio").setLevel(logging.INFO)
    postgres_url = os.environ["POSTGRES_URL"]
    redis_url = os.environ["REDIS_URL"]
    db_pool = AsyncConnectionPool(postgres_url, min_size=1, max_size=5, open=False)
    await db_pool.open()

    qp = QueryProcessor(db_pool, redis_url)
    print("calling run")
    await qp.run()
    await db_pool.close()


def main():
    asyncio.run(start())
