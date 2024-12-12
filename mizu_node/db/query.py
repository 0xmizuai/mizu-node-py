from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from mizu_node.db.orm.dataset import Dataset
from mizu_node.db.orm.job_queue import JobQueue
from mizu_node.db.orm.query import Query
from mizu_node.types.data_job import JobStatus


async def save_new_query(
    session: AsyncSession,
    dataset_id: int,
    query_text: str,
    model: str,
    user: str,
    status: str = "pending",
) -> int:
    query_obj = Query(
        dataset_id=dataset_id,
        query_text=query_text,
        model=model,
        user=user,
        status=status,
    )
    session.add(query_obj)
    await session.flush()  # To get the ID
    return query_obj.id


async def get_query_status(session: AsyncSession, query_id: int) -> dict:
    stmt = select(Query).where(Query.id == query_id)
    query = (await session.execute(stmt)).scalar_one_or_none()
    if not query:
        return None

    # Get dataset size
    stmt = select(Dataset.total_objects).where(
        Dataset.name == query.dataset, Dataset.language == query.language
    )
    dataset_size = (await session.execute(stmt)).scalar_one()

    # Get query results count
    stmt = select(QueryResult).where(QueryResult.query_id == query_id)
    query_results_count = len((await session.execute(stmt)).scalars().all())

    return {
        "query_id": query.id,
        "dataset_id": query.dataset_id,
        "processed_records": query.progress,
        "created_at": query.created_at,
        "dataset_size": dataset_size,
        "query_results_count": query_results_count,
    }


async def get_query_detail(session: AsyncSession, query_id: int) -> Query:
    stmt = select(Query).where(Query.id == query_id)
    return (await session.execute(stmt)).scalar_one_or_none()


async def get_owned_queries(session: AsyncSession, user: str) -> list[Query]:
    stmt = select(Query).where(Query.user == user)
    return (await session.execute(stmt)).scalars().all()


async def get_unpublished_query(session: AsyncSession) -> Query:
    """Get the earliest unpublished query"""
    stmt = (
        select(Query)
        .where(Query.status == "pending")
        .order_by(Query.created_at)
        .limit(1)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def update_query_status(
    session: AsyncSession, query_id: int, status: str, last_data_id: int = None
):
    stmt = (
        update(Query)
        .where(Query.id == query_id)
        .values(status=status, last_data_id_published=last_data_id)
    )
    await session.execute(stmt)
    await session.commit()


async def get_query_results(
    session: AsyncSession, query_id: int, page: int = 1, page_size: int = 1000
) -> tuple[list, int]:
    # Get total count
    stmt = select(JobQueue).where(JobQueue.reference_id == query_id)
    total = len((await session.execute(stmt)).scalars().all())

    # Get paginated results
    stmt = (
        select(JobQueue)
        .where(
            JobQueue.reference_id == query_id,
            JobQueue.status == JobStatus.COMPLETED,
            JobQueue.result.isnot(None),
        )
        .order_by(JobQueue.id)
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    results = (await session.execute(stmt)).scalars().all()

    return results, total
