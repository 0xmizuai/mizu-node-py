import logging
import os
import asyncio
import aioboto3
from botocore.config import Config
from typing import AsyncGenerator
from sqlalchemy.sql import text
from datetime import date

from mizu_node.db.common import get_async_db_session, get_db_session
from scripts.languages import LANGUAGES

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")

DATASET_BUCKET = "mizu-cmc"

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

        return {
            "name": dataset,
            "language": language,
            "data_type": data_type,
            "md5": md5,
            "num_of_records": 0,
            "decompressed_byte_size": 0,
            "byte_size": int(obj.get("Size", 0)),
            "source": "",
        }
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


async def insert_batch_to_db(objects: list[dict]):
    """
    Insert a batch of objects into the dataset table asynchronously
    """
    try:
        logger.info(f"Inserting batch of {len(objects)} records to database")
        async with get_async_db_session() as session:  # You'll need to create this function
            await session.execute(
                text(
                    """
                INSERT INTO datasets (
                    name, language, data_type, md5,
                    num_of_records, decompressed_byte_size, byte_size, source
                ) VALUES (
                    :name, :language, :data_type, :md4,
                    :num_of_records, :decompressed_byte_size, :byte_size, :source
                ) ON CONFLICT (md5) DO UPDATE SET
                    byte_size = EXCLUDED.byte_size
                """
                ),
                objects,
            )
        logger.info("Successfully inserted batch to database")
    except Exception as e:
        logger.error(f"Error inserting batch into database: {str(e)}")


def get_last_processed_key(language: str) -> str:
    """Get the r2_key of the last processed item from the database"""
    try:
        with get_db_session() as session:
            result = session.execute(
                text(
                    """
                    SELECT name, data_type, language, md5
                    FROM datasets
                    WHERE language = :language
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).bindparams(language=language)
            ).fetchone()

            if result:
                last_key = f"{result[0]}/{result[1]}/{result[2]}/{result[3]}.zz"
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
                await insert_batch_to_db(batch_metadata)
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
            load_dataset_for_language(dataset, data_type, lang) for lang in LANGUAGES
        ]

        # Run all tasks concurrently
        await asyncio.gather(*tasks)

        logger.info(f"Completed loading all languages for {dataset}/{data_type}")

    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Exiting...")
        raise


def update_dataset_stats():
    """
    Calculate and store dataset statistics in the dataset_stats table
    """
    logger.info("Starting dataset statistics calculation")
    try:
        with get_db_session() as session:
            # Get statistics grouped by language and r2_prefix
            stats = session.execute(
                text(
                    """
                    WITH prefix_extract AS (
                        SELECT 
                            language,
                            data_type,
                            name,
                            md5,
                        FROM datasets
                    )
                    SELECT 
                        language,
                        data_type,
                        name,
                        COUNT(*) as total_objects,
                    FROM prefix_extract
                    GROUP BY name, language, data_type
                """
                )
            ).fetchall()

            # Insert or update statistics
            current_date = date.today()
            for stat in stats:
                session.execute(
                    text(
                        """
                        INSERT INTO dataset_stats 
                            (language, data_type, name, total_objects)
                        VALUES 
                            (:language, :data_type, :name, :total_objects)
                        ON CONFLICT (language, data_type, name) 
                        DO UPDATE SET
                            total_objects = EXCLUDED.total_objects,
                            created_at = CURRENT_TIMESTAMP
                    """
                    ),
                    {
                        "language": stat.language,
                        "data_type": stat.data_type,
                        "name": stat.name,
                        "total_objects": stat.total_objects,
                    },
                )

            session.commit()
            logger.info(
                f"Successfully updated dataset statistics for {len(stats)} combinations"
            )

    except Exception as e:
        logger.error(f"Error updating dataset statistics: {str(e)}")


async def process_combination(session, combo, sample_size: int, batch_size: int = 1000):
    """Process a single dataset/language combination"""
    try:
        # Sample records for the combination
        result = await session.execute(
            text(
                """
                SELECT 
                    name, language, data_type, md5,
                    num_of_records, decompressed_byte_size, byte_size, source
                FROM datasets 
                WHERE name = :name 
                    AND language = :language 
                    AND data_type = :data_type
                ORDER BY RANDOM() 
                LIMIT :sample_size
                """
            ),
            {
                "name": combo.name,
                "language": combo.language,
                "data_type": combo.data_type,
                "sample_size": sample_size,
            },
        )
        samples = result.fetchall()

        # Convert to list of dictionaries
        records = [dict(row) for row in samples]

        if records:
            batches = [
                records[i : i + batch_size] for i in range(0, len(records), batch_size)
            ]

            # Process batches concurrently
            await asyncio.gather(*[insert_batch_to_db(batch) for batch in batches])

            logger.info(
                f"Processed {len(records)} records for {combo.name}/{combo.language}/{combo.data_type}"
            )
            return len(records)

        return 0

    except Exception as e:
        logger.error(
            f"Error processing {combo.name}/{combo.language}/{combo.data_type}: {str(e)}"
        )
        return 0


async def sample_datasets(source_db_url: str, sample_size: int = 100000):
    """
    Sample records from source database's datasets table and insert them into the current database.

    Args:
        source_db_url (str): The source database connection URL
        sample_size (int): Number of records to sample per dataset/language combination
    """
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker

    logger.info(
        f"Starting dataset sampling. Sample size per combination: {sample_size}"
    )

    try:
        # Create async source database connection
        source_engine = create_async_engine(
            source_db_url.replace("postgresql://", "postgresql+asyncpg://"), echo=False
        )
        AsyncSourceSession = sessionmaker(
            source_engine, class_=AsyncSession, expire_on_commit=False
        )

        async with AsyncSourceSession() as source_session:
            # Get distinct dataset/language combinations
            result = await source_session.execute(
                text(
                    """
                    SELECT DISTINCT name, language, data_type 
                    FROM datasets
                    """
                )
            )
            combinations = result.fetchall()

            # Process all combinations concurrently
            results = await asyncio.gather(
                *[
                    process_combination(source_session, combo, sample_size)
                    for combo in combinations
                ],
                return_exceptions=True,  # This prevents one failure from stopping all tasks
            )

            # Calculate total processed
            total_sampled = sum(r for r in results if isinstance(r, int))
            total_errors = sum(1 for r in results if isinstance(r, Exception))

            logger.info(
                f"Completed sampling. Total records sampled: {total_sampled}, "
                f"Failed combinations: {total_errors}"
            )

    except Exception as e:
        logger.error(f"Error during sampling process: {str(e)}")
    finally:
        await source_engine.dispose()


def start():
    import asyncio
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stats", action="store_true", help="Update dataset statistics"
    )
    parser.add_argument(
        "--sample", action="store_true", help="Sample data from source database"
    )
    parser.add_argument(
        "--source-db", type=str, help="Source database URL for sampling"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100000,
        help="Number of records to sample per combination",
    )
    args = parser.parse_args()

    if args.stats:
        update_dataset_stats()
        return

    if args.sample:
        if not args.source_db:
            logger.error("Source database URL is required for sampling")
            return
        sample_datasets(args.source_db, args.sample_size)
        return

    asyncio.run(load_dataset("CC-MAIN-2024-46", "text"))
