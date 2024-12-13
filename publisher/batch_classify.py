from asyncio.log import logger
import os
import time

from mizu_node.db.job_queue import add_jobs, get_queue_len
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import BatchClassifyContext, DataJobContext, JobType


class BatchClassifier:
    def __init__(self):
        self.batch_size = 1000
        self.queue_threshold = 1000000
        self.conn = Connections()

    def process_query_batch(self, query: Query) -> tuple[int, int]:
        """Process a single batch of records for a query
        Returns: (number of records processed, last data id processed)
        """
        with self.conn.get_pg_connection() as db:
            dataset = get_dataset(db, query.dataset_id)
            records = get_unpublished_data_per_query(db, query)
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
            with self.conn.get_pg_connection() as db:
                add_jobs(db, JobType.batch_classify, contexts, query.id)
            return len(records), records[-1].id

    def manage_job_queue(self):
        """Main function to manage the job queue"""
        while True:
            try:
                with self.conn.get_pg_connection() as db:
                    current_queue_len = get_queue_len(db, JobType.batch_classify)
                if current_queue_len >= self.queue_threshold:
                    time.sleep(10)
                    continue

                with self.conn.get_pg_connection() as db:
                    query = get_unpublished_query(db)
                    if not query:
                        time.sleep(10)
                        continue

                # Calculate how many more jobs we can add
                available_slots = int(self.queue_threshold * 1.5) - current_queue_len
                jobs_to_process = (available_slots // self.batch_size) * self.batch_size
                if jobs_to_process <= 0:
                    time.sleep(10)
                    continue

                total_processed = 0
                with self.conn.get_pg_connection() as db:
                    while total_processed < jobs_to_process:
                        records_processed, last_data_id = self.process_query_batch(
                            query
                        )
                        if records_processed == 0:
                            update_query_status(db, query.id, "published")
                            break

                        total_processed += records_processed
                        update_query_status(db, query.id, "publishing", last_data_id)

            except Exception as e:
                logger.error(f"Error in job queue management: {str(e)}")
                time.sleep(10)

    def run(self):
        """Start the queue manager as a background task"""
        while True:
            try:
                self.manage_job_queue()
            except Exception as e:
                logger.error(f"Queue manager crashed: {str(e)}")
                time.sleep(10)


def start():
    BatchClassifier().run()
