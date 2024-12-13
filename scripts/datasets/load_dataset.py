import argparse
import logging
import os
import asyncio
import aioboto3
from botocore.config import Config
from typing import AsyncGenerator
from sqlalchemy.future import select
from sqlalchemy import text

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
    force=True,
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


async def list_r2_objects(
    dataset_id: int, prefix: str, start_after: str = ""
) -> AsyncGenerator[list[dict], None]:
    """Lists objects from R2 bucket and gets their metadata in batches"""
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
                        byte_size=int(obj.get("Size", 0)),
                    )
                    for obj in page["Contents"]
                ]
                processed += len(records)
                errors += len(records) - len(records)

                last_md5 = records[-1].md5 if records else ""
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


async def load_dataset_for_language(
    name: str, data_type: int, language: str, last_processed_md5: str
):
    """Process a single language prefix"""
    try:
        total_processed = 0
        prefix = f"{name}/{data_type}/{language}"
        async with conn.get_query_db_session() as session:
            stmt = select(Dataset).filter(
                Dataset.name == name,
                Dataset.data_type == data_type,
                Dataset.language == language,
            )
            result = await session.execute(stmt)
            dataset = result.scalar_one()
            dataset_id = dataset.id

        start_after = f"{prefix}/{last_processed_md5}.zz"
        logger.info(
            f"Starting dataset load for {prefix} with dataset id {dataset_id} and start after {start_after}"
        )
        async for batch_metadata in list_r2_objects(dataset_id, prefix, start_after):
            async with conn.get_query_db_session() as session:
                sql = """
                    INSERT INTO data_records (dataset_id, md5, byte_size)
                    VALUES (:dataset_id, :md5, :byte_size)
                    ON CONFLICT (md5) DO UPDATE 
                    SET byte_size = EXCLUDED.byte_size
                """
                for record in batch_metadata:
                    await session.execute(
                        text(sql),
                        {
                            "dataset_id": record.dataset_id,
                            "md5": record.md5,
                            "byte_size": record.byte_size,
                        },
                    )
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
            load_dataset_for_language(
                dataset, data_type, lang, LANGUAGES[lang].last_processed
            )
            for lang in LANGUAGES
            if LANGUAGES[lang].last_processed is not None
        ]

        # Run all tasks concurrently
        await asyncio.gather(*tasks)

        logger.info(f"Completed loading all languages for {dataset}/{data_type}")

    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Exiting...")
        raise


parser = argparse.ArgumentParser(description="Add a new dataset to the database")
parser.add_argument("--name", type=str, action="store", help="Name of the dataset")
parser.add_argument(
    "--data-type",
    action="store",
    type=str,
    default="text",
    help="Type of the dataset",
)

args = parser.parse_args()


def start():
    asyncio.run(load_dataset(args.name, args.data_type))
