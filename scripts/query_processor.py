import asyncio
import logging
import os
import signal

import redis.asyncio as redis

from psycopg_pool import AsyncConnectionPool
from redis.asyncio.client import Redis

from mizu_node.db.job_queue import add_jobs_async
from mizu_node.db.query import (
    get_unpublished_queries,
    update_query_status_async,
    get_unpublished_data_per_query_async,
)
from mizu_node.types.data_job import DataJobContext, BatchClassifyContext, JobType
from mizu_node.types.query import QueryStatus, DataQuery


def redis_key(query_id: int):
    return f"qp:query:{query_id}:start_id"


class QueryProcessor:

    def __init__(
        self,
        query_db_pool: AsyncConnectionPool,
        jobs_db_pool: AsyncConnectionPool,
        redis_client: Redis,
        max_concurrent_tasks: int = 10,
    ):
        self.query_interval_s = 10  # seconds
        self.job_batch_interval_s = 30  # seconds
        self.max_concurrent_tasks = max_concurrent_tasks
        self.batched_jobs = 1000
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self.query_db_pool = query_db_pool
        self.jobs_db_pool = jobs_db_pool
        self.r_client = redis_client
        self.active_tasks = set()

        self.shutdown_event = asyncio.Event()

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    async def create_jobs(self, query: DataQuery):
        # retrieve start_id, if any
        start_id = await self.r_client.get(redis_key(query.id)) or 0

        while True:
            if self.shutdown_event.is_set():
                self.logger.info(f"create_jobs for query {query.id} received shutdown")
                return  # return because we do not want to write completion to db in this case

            self.logger.info(
                f"query {query.id}: publishing {self.batched_jobs} jobs after {start_id} id"
            )
            async with self.query_db_pool.connection() as conn:
                paginated_records = await get_unpublished_data_per_query_async(
                    conn, query, start_id, self.batched_jobs
                )
                self.logger.info(
                    f"Retrieved {len(paginated_records.records)} records for query {query.id}"
                )
                if paginated_records.last_id is None:
                    # we processed everything
                    self.logger.info(f"Processed all records for query {query.id}")
                    break
                else:
                    start_id = paginated_records.last_id
                    self.logger.info(
                        f"Query {query.id}: new start_id for next paginated record -> {start_id}"
                    )

                # prepare jobs
                contexts = [
                    DataJobContext(
                        batch_classify_ctx=BatchClassifyContext(
                            data_url=f"/{query.dataset.name}/{query.dataset.data_type}/{query.dataset.language}/{record.md5}.zz",
                            batch_size=0,
                            bytesize=record.byte_size,
                            decompressed_byte_size=record.decompressed_byte_size,
                            checksum_md5=record.md5,
                            classifier_id=0,
                        )
                    )
                    for record in paginated_records.records
                ]
                # Write jobs to jobs db
                async with self.jobs_db_pool.connection() as jobs_db:
                    _ = await add_jobs_async(
                        jobs_db, JobType.batch_classify, contexts, query.id
                    )
                    # Update redis with last record id stored into jobs table
                    await self.r_client.set(redis_key(query.id), start_id)
                self.logger.info(
                    f"last_id {paginated_records.last_id} for query {query.id} written to redis"
                )

            # Wait before next batch
            await asyncio.sleep(self.job_batch_interval_s)

        # Done with this query, mark it complete
        query.status = QueryStatus.processed
        async with self.query_db_pool.connection() as conn:
            await update_query_status_async(conn, query)

    async def fetch_entries(self, limit: int):
        if self.shutdown_event.is_set():
            return []

        async with self.query_db_pool.connection() as conn:
            return await get_unpublished_queries(conn, limit)

    async def process_query(self, entry):
        try:
            async with self.semaphore:
                try:
                    if self.shutdown_event.is_set():
                        self.logger.info(f"Skipping {entry.id} due to shutdown event")
                        return

                    self.logger.info(f"Processing query {entry.id}")
                    await self.create_jobs(entry)
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
        await self.jobs_db_pool.close()
        await self.r_client.aclose()

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

                        task = asyncio.create_task(self.process_query(entry))
                        self.active_tasks.add(task)
                else:
                    self.logger.info("Not enough workers available")

                await asyncio.sleep(self.query_interval_s)

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

    # Set up DB pools
    query_db_pool = AsyncConnectionPool(
        postgres_query_url, min_size=1, max_size=5, open=False
    )
    await query_db_pool.open()
    jobs_db_pool = AsyncConnectionPool(
        postgres_jobs_url, min_size=1, max_size=5, open=False
    )
    await jobs_db_pool.open()

    # Setup Redis
    r_client = await redis.from_url(redis_url)

    qp = QueryProcessor(query_db_pool, jobs_db_pool, r_client, max_concurrent_tasks=3)
    await qp.run()


def main():
    asyncio.run(start())
