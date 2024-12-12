from mizu_node.db.orm.data_record import DataRecord
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from mizu_node.db.orm.dataset import Dataset
from mizu_node.db.orm.query import Query

import logging
from typing import List

from mizu_node.types.connections import Connections

logger = logging.getLogger(__name__)


async def get_unpublished_data_per_query(
    session: AsyncSession, query: Query
) -> list[DataRecord]:
    stmt = select(DataRecord).where(
        DataRecord.name == query.dataset,
        DataRecord.language == query.language,
        DataRecord.id > (query.last_data_id_published or 0),
    )
    return (await session.execute(stmt)).scalars().all()


async def get_dataset(session: AsyncSession, dataset_id: int) -> Dataset:
    stmt = select(Dataset).where(Dataset.id == dataset_id)
    return (await session.execute(stmt)).scalar_one_or_none()


async def save_data_records(session: AsyncSession, objects: List[dict]) -> None:
    """Insert a batch of objects into the dataset table asynchronously

    Args:
        objects: List of dictionaries containing dataset metadata
    """
    try:
        logger.info(f"Inserting batch of {len(objects)} records to database")
        for obj in objects:
            dataset = DataRecord(
                dataset_id=obj["name"],
                md5=obj["md5"],
                num_of_records=obj["num_of_records"],
                decompressed_byte_size=obj["decompressed_byte_size"],
                byte_size=obj["byte_size"],
                source=obj["source"],
            )
            session.merge(dataset)
        logger.info("Successfully inserted batch to database")
    except Exception as e:
        logger.error(f"Error inserting batch into database: {str(e)}")
