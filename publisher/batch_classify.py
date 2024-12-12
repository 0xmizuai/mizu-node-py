from asyncio.log import logger
import os
import asyncio
from typing import Optional

from sqlalchemy import select

from mizu_node.db.job_queue import add_jobs, queue_len
from mizu_node.db.orm.dataset import Dataset
from mizu_node.db.orm.query import Query
from mizu_node.db.queries import (
    get_async_db_session,
    get_unpublished_query,
    update_query_status,
)
from mizu_node.types.data_job import BatchClassifyContext, DataJobContext, JobType

MIZU_NODE_URL = os.environ["MIZU_NODE_SERVICE_URL"]
BATCH_SIZE = 1000
QUEUE_THRESHOLD = 1000000


async def process_query_batch(
    query: Query, start_after_data_id: Optional[int] = None
) -> tuple[int, int]:
    """Process a single batch of records for a query
    Returns: (number of records processed, last data id processed)
    """
    async with get_async_db_session() as session:
        # Get next batch of records using ORM
        stmt = (
            select(Dataset)
            .where(
                Dataset.name == query.dataset,
                Dataset.language == query.language,
                Dataset.id > (start_after_data_id or 0),
            )
            .order_by(Dataset.id)
            .limit(BATCH_SIZE)
        )
        result = await session.execute(stmt)
        records = result.scalars().all()

        if not records:
            return 0, start_after_data_id

        # Create job contexts using ORM objects
        contexts = [
            DataJobContext(
                batch_classify_ctx=BatchClassifyContext(
                    data_url=f"/{query.dataset}/text/{query.language}/{record.md5}.zz",
                    batch_size=0,
                    bytesize=record.byte_size,
                    decompressed_byte_size=record.decompressed_byte_size,
                    checksum_md5=record.md5,
                    classifier_id=0,  # Add appropriate classifier_id if needed
                )
            )
            for record in records
        ]

        await add_jobs(session, JobType.batch_classify, query.user, contexts)
        return len(records), records[-1].id


async def manage_job_queue():
    """Main function to manage the job queue"""
    while True:
        try:
            if await queue_len(JobType.batch_classify) >= QUEUE_THRESHOLD:
                await asyncio.sleep(10)
                continue

            query = await get_unpublished_query()
            if not query:
                await asyncio.sleep(10)
                continue

            last_data_id = query.last_data_id_published
            while (await queue_len(JobType.batch_classify)) < (QUEUE_THRESHOLD * 1.5):
                records_processed, last_data_id = await process_query_batch(
                    query, last_data_id
                )

                if records_processed == 0:
                    await update_query_status(query.id, "published")
                    break

                await update_query_status(query.id, "publishing", last_data_id)

        except Exception as e:
            logger.error(f"Error in job queue management: {str(e)}")
            await asyncio.sleep(10)


async def start_queue_manager():
    """Start the queue manager as a background task"""
    while True:
        try:
            await manage_job_queue()
        except Exception as e:
            logger.error(f"Queue manager crashed: {str(e)}")
            await asyncio.sleep(10)
