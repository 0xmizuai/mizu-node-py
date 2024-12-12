import logging
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from mizu_node.db.dataset import save_data_records
from mizu_node.db.orm.data_record import DataRecord

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


async def process_combination(session, combo, sample_size: int, batch_size: int = 1000):
    """Process a single dataset/language combination"""
    try:
        # Sample records for the combination
        samples = (
            await session.query(DataRecord)
            .filter(
                DataRecord.dataset_id == combo.name,
                DataRecord.language == combo.language,
                DataRecord.data_type == combo.data_type,
            )
            .order_by(func.random())
            .limit(sample_size)
            .all()
        )

        if samples:
            # Convert to dictionaries
            records = [
                {
                    "name": s.name,
                    "language": s.language,
                    "data_type": s.data_type,
                    "md5": s.md5,
                    "num_of_records": s.num_of_records,
                    "decompressed_byte_size": s.decompressed_byte_size,
                    "byte_size": s.byte_size,
                    "source": s.source,
                }
                for s in samples
            ]

            # Process in batches
            batches = [
                records[i : i + batch_size] for i in range(0, len(records), batch_size)
            ]
            await asyncio.gather(
                *[save_data_records(session, batch) for batch in batches]
            )

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
            combinations = (
                await source_session.query(
                    Dataset.name, Dataset.language, Dataset.data_type
                )
                .distinct()
                .all()
            )

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


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-db", type=str, required=True, help="Source database URL for sampling"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100000,
        help="Number of records to sample per dataset/language combination",
    )
    args = parser.parse_args()

    asyncio.run(sample_datasets(args.source_db, args.sample_size))


if __name__ == "__main__":
    main()
