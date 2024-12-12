import logging
import os
import asyncio
import aioboto3
from botocore.config import Config
from typing import AsyncGenerator

from mizu_node.db.dataset import (
    save_data_records,
)
from mizu_node.db.orm.data_record import DataRecord
from mizu_node.db.orm.dataset import Dataset
from mizu_node.types.connections import Connections
from mizu_node.types.languages import LANGUAGES

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")

DATASET_BUCKET = "mizu-cmc"

conn = Connections()

# Set up logging at the top of the file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


async def list_r2_objects(
    dataset_id: int, prefix: str, start_after: str = ""
) -> AsyncGenerator[list[dict], None]:
    """Lists objects from R2 bucket and gets their metadata in batches"""
    logger.info(f"Starting to list objects with prefix: {prefix}, from: {start_after}")
    processed = 0
    errors = 0

    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(retries=dict(max_attempts=3)),
    ) as s3_client:
        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=DATASET_BUCKET,
                Prefix=prefix,
                StartAfter=start_after,
            ):
                if "Contents" not in page:
                    logger.warning(f"No contents found for prefix: {prefix}")
                    continue

                # Process the entire page directly
                records = [
                    DataRecord(
                        dataset_id=dataset_id,
                        md5=obj["Key"].split("/")[3].replace(".zz", ""),
                        num_of_records=0,
                        decompressed_byte_size=0,
                        byte_size=int(obj.get("Size", 0)),
                        source="",
                    )
                    for obj in page["Contents"]
                ]
                processed += len(records)
                errors += len(records) - len(records)

                last_md5 = records[-1]["md5"] if records else ""
                logger.info(
                    f"Processed batch of {len(records)} objects for {prefix}. Total: {processed}, Errors: {errors}, Last md5: {last_md5}"
                )
                yield records

            logger.info(
                f"Completed listing objects for {prefix}. Total processed: {processed}, Errors: {errors}"
            )

        except Exception as e:
            logger.error(f"Error listing objects for {prefix} from R2: {str(e)}")
            return


def get_last_processed_key(language: str) -> str:
    """Get the r2_key of the last processed item from the database"""
    try:
        with conn.get_query_db_session() as session:
            last_dataset = (
                session.query(DataRecord)
                .filter(DataRecord.language == language)
                .order_by(DataRecord.id.desc())
                .first()
            )

            if last_dataset:
                last_key = f"{last_dataset.name}/{last_dataset.data_type}/{last_dataset.language}/{last_dataset.md5}.zz"
                logger.info(f"Resuming from last processed key: {last_key}")
                return last_key

            logger.info("No previous progress found, starting from beginning")
            return ""
    except Exception as e:
        logger.error(f"Error getting last processed key: {str(e)}")
        return ""


async def load_dataset_for_language(name: str, data_type: int, language: str):
    """Process a single language prefix"""
    try:
        total_processed = 0
        prefix = f"{name}/{data_type}/{language}"
        async with conn.get_query_db_session() as session:
            dataset_id = (
                session.query(Dataset)
                .filter(
                    Dataset.name == name,
                    Dataset.data_type == data_type,
                    Dataset.language == language,
                )
                .first()
                .id
            )
        logger.info(f"Starting dataset load for {prefix}")

        #        start_after = get_last_processed_key(language)
        start_after = ""
        async for batch_metadata in list_r2_objects(dataset_id, prefix, start_after):
            if batch_metadata:
                async with conn.get_query_db_session() as session:
                    await save_data_records(session, batch_metadata)
                    total_processed += len(batch_metadata)
                    logger.info(f"Total processed for {language}: {total_processed}")

        logger.info(
            f"Completed loading dataset {prefix}. Total processed: {total_processed}"
        )

    except Exception as e:
        logger.error(f"Error processing language {language}: {str(e)}")


async def load_dataset(dataset: str, data_type: str):
    logger.info(f"Loading dataset {dataset} with data type {data_type}")
    try:
        tasks = [
            load_dataset_for_language(dataset, data_type, lang)
            for lang in LANGUAGES
            if not lang.is_finished()
        ]

        # Run all tasks concurrently
        await asyncio.gather(*tasks)

        logger.info(f"Completed loading all languages for {dataset}/{data_type}")

    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Exiting...")
        raise


def start():
    import asyncio

    asyncio.run(load_dataset("CC-MAIN-2024-46", "text"))
