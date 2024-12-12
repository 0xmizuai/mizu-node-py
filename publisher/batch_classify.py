from asyncio.log import logger
import os
import asyncio

from mizu_node.db.dataset import get_dataset, get_unpublished_data_per_query
from mizu_node.db.job_queue import add_jobs, get_queue_len
from mizu_node.db.orm.query import Query
from mizu_node.db.query import (
    get_unpublished_query,
    update_query_status,
)
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import BatchClassifyContext, DataJobContext, JobType


class BatchClassifier:
    def __init__(self):
        self.mizu_node_url = os.environ["MIZU_NODE_SERVICE_URL"]
        self.batch_size = 1000
        self.queue_threshold = 1000000
        self.conn = Connections()

    async def process_query_batch(self, query: Query) -> tuple[int, int]:
        """Process a single batch of records for a query
        Returns: (number of records processed, last data id processed)
        """
        async with self.conn.get_query_db_session() as session:
            dataset = await get_dataset(session, query.dataset_id)
            records = await get_unpublished_data_per_query(session, query)
            if not records:
                return 0, query.last_data_id_published

            contexts = [
                DataJobContext(
                    batch_classify_ctx=BatchClassifyContext(
                        data_url=f"/{dataset.name}/{dataset.data_type}/{dataset.language}/{record.md5}.zz",
                        batch_size=0,
                        bytesize=record.byte_size,
                        decompressed_byte_size=record.decompressed_byte_size,
                        checksum_md5=record.md5,
                        classifier_id=0,
                    )
                )
                for record in records
            ]
            async with self.conn.get_job_db_session() as session:
                await add_jobs(session, JobType.batch_classify, contexts, query.id)
            return len(records), records[-1].id

    async def manage_job_queue(self):
        """Main function to manage the job queue"""
        while True:
            try:
                async with self.conn.get_job_db_session() as session:
                    current_queue_len = await get_queue_len(
                        session, JobType.batch_classify
                    )
                if current_queue_len >= self.queue_threshold:
                    await asyncio.sleep(10)
                    continue

                async with self.conn.get_query_db_session() as session:
                    query = await get_unpublished_query(session)
                    if not query:
                        await asyncio.sleep(10)
                        continue

                # Calculate how many more jobs we can add
                available_slots = int(self.queue_threshold * 1.5) - current_queue_len
                jobs_to_process = (available_slots // self.batch_size) * self.batch_size
                if jobs_to_process <= 0:
                    await asyncio.sleep(10)
                    continue

                total_processed = 0
                async with self.conn.get_query_db_session() as session:
                    while total_processed < jobs_to_process:
                        records_processed, last_data_id = (
                            await self.process_query_batch(query)
                        )
                        if records_processed == 0:
                            await update_query_status(session, query.id, "published")
                            break

                        total_processed += records_processed
                        await update_query_status(
                            session, query.id, "publishing", last_data_id
                        )

            except Exception as e:
                logger.error(f"Error in job queue management: {str(e)}")
                await asyncio.sleep(10)

    async def start(self):
        """Start the queue manager as a background task"""
        while True:
            try:
                await self.manage_job_queue()
            except Exception as e:
                logger.error(f"Queue manager crashed: {str(e)}")
                await asyncio.sleep(10)


# Create singleton instance
batch_classifier = BatchClassifier()
