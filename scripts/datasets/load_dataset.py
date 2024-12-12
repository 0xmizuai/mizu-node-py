import logging
import os
import asyncio
import aioboto3
from botocore.config import Config
from typing import AsyncGenerator
from sqlalchemy.sql import func

from mizu_node.db.dataset import save_data_records
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


async def get_object_metadata(s3_client, obj: dict) -> dict:
    """Get metadata for a single object"""
    try:
        # Parse key components
        key_parts = obj["Key"].split("/")
        if len(key_parts) >= 4:
            dataset = key_parts[0]
            data_type = key_parts[1]
            language = key_parts[2]
            md5 = key_parts[3].replace(".zz", "")
        else:
            raise Exception(f"Invalid key format: {obj['Key']}")

        return DataRecord(
            name=dataset,
            language=language,
            data_type=data_type,
            md5=md5,
            num_of_records=0,
            decompressed_byte_size=0,
            byte_size=int(obj.get("Size", 0)),
            source="",
        )
    except Exception as e:
        logger.error(f"Error getting metadata for {obj['Key']}: {str(e)}")
        return None


async def list_r2_objects(
    prefix: str = "", start_after: str = ""
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
                tasks = [
                    get_object_metadata(s3_client, obj) for obj in page["Contents"]
                ]
                results = await asyncio.gather(*tasks)

                # Filter out None results (failed requests)
                valid_results = [r for r in results if r is not None]
                processed += len(valid_results)
                errors += len(results) - len(valid_results)

                last_md5 = valid_results[-1]["md5"] if valid_results else ""
                logger.info(
                    f"Processed batch of {len(valid_results)} objects for {prefix}. Total: {processed}, Errors: {errors}, Last md5: {last_md5}"
                )
                yield valid_results

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


async def load_dataset_for_language(dataset: str, data_type: str, language: str):
    """Process a single language prefix"""
    try:
        prefix = f"{dataset}/{data_type}/{language}"
        total_processed = 0

        logger.info(f"Starting dataset load for {prefix}")

        #        start_after = get_last_processed_key(language)
        start_after = ""
        async for batch_metadata in list_r2_objects(prefix, start_after):
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


def update_dataset_stats():
    """Calculate and store dataset statistics in the dataset_stats table"""
    logger.info("Starting dataset statistics calculation")
    try:
        with conn.get_query_db_session() as session:
            # Create a subquery with the stats
            stats_subquery = (
                session.query(
                    DataRecord.dataset_id,
                    func.count().label("total_objects"),
                    func.sum(DataRecord.byte_size).label("total_bytes"),
                )
                .group_by(DataRecord.dataset_id)
                .subquery()
            )

            # Update using the ORM
            update_stmt = (
                session.query(Dataset)
                .filter(Dataset.id == stats_subquery.c.dataset_id)
                .update(
                    {
                        Dataset.total_objects: stats_subquery.c.total_objects,
                        Dataset.total_bytes: stats_subquery.c.total_bytes,
                    },
                    synchronize_session=False,
                )
            )
        logger.info(f"Successfully updated statistics for {update_stmt} datasets")
    except Exception as e:
        logger.error(f"Error updating dataset statistics: {str(e)}")


def start():
    import asyncio
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stats", action="store_true", help="Update dataset statistics"
    )
    args = parser.parse_args()

    if args.stats:
        update_dataset_stats()
        return

    asyncio.run(load_dataset("CC-MAIN-2024-46", "text"))
