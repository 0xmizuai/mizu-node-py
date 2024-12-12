from mizu_node.db.orm.data_record import DataRecord
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from mizu_node.db.orm.dataset import Dataset
from mizu_node.db.orm.query import Query

import logging

from mizu_node.types.languages import LANGUAGES

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


async def add_datasets(session: AsyncSession, dataset: str, data_type: str) -> None:
    for lang in LANGUAGES.keys():
        dataset_record = Dataset(
            name=dataset,
            data_type=data_type,
            language=lang,
        )
        await session.merge(dataset_record)


async def get_dataset(session: AsyncSession, dataset_id: int) -> Dataset:
    stmt = select(Dataset).where(Dataset.id == dataset_id)
    return (await session.execute(stmt)).scalar_one_or_none()
