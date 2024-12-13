from asyncio.log import logger
import os
import time
from psycopg2.extensions import connection

from mizu_node.db.job_queue import add_jobs, get_queue_len
from mizu_node.db.query import (
    get_unpublished_data_per_query,
    get_unpublished_query,
    update_query_status,
)
from mizu_node.types.connections import Connections
from mizu_node.types.data_job import BatchClassifyContext, DataJobContext, JobType
from mizu_node.types.query import DataQuery, QueryStatus


class BatchClassifier:
    def __init__(self):
        self.batch_size = 1000
        self.queue_threshold = 1000000
        self.conn = Connections()
        self.job_db_url = os.getenv("JOB_DB_URL")
        self.query_db_url = os.getenv("QUERY_DB_URL")

    def process_query_batch(
        self, query_db: connection, query: DataQuery
    ) -> tuple[int, int]:
        """Process a single batch of records for a query
        Returns: (number of records processed, last data id processed)
        """
        paginated_records = get_unpublished_data_per_query(query_db, query)
        if not paginated_records.records:
            return 0, query.last_record_published

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
        with self.conn.get_pg_connection(self.job_db_url) as job_db:
            add_jobs(job_db, JobType.batch_classify, contexts, query.id)
        return len(paginated_records.records), paginated_records.last_id

    def manage_job_queue(self):
        """Main function to manage the job queue"""
        while True:
            try:
                with self.conn.get_pg_connection() as job_db:
                    current_queue_len = get_queue_len(job_db, JobType.batch_classify)
                if current_queue_len >= self.queue_threshold:
                    time.sleep(10)
                    continue

                with self.conn.get_pg_connection(self.query_db_url) as query_db:
                    query = get_unpublished_query(query_db)
                    if not query:
                        time.sleep(10)
                        continue

                    # Calculate how many more jobs we can add
                    available_slots = (
                        int(self.queue_threshold * 1.5) - current_queue_len
                    )
                    jobs_to_process = (
                        available_slots // self.batch_size
                    ) * self.batch_size
                    if jobs_to_process <= 0:
                        time.sleep(10)
                        continue

                    total_processed = 0
                    while total_processed < jobs_to_process:
                        records_processed, last_record_id = self.process_query_batch(
                            query_db, query
                        )
                        if records_processed == 0:
                            query.status = QueryStatus.processing
                            update_query_status(query_db, query)
                            break

                        total_processed += records_processed
                        query.status = QueryStatus.publishing
                        query.last_record_published = last_record_id
                        update_query_status(query_db, query)

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
