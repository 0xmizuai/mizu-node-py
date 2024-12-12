from datetime import datetime, timezone
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from mizu_node.db.orm.dataset import Dataset
from mizu_node.db.orm.query import Query
from mizu_node.db.orm.query_result import QueryResult
from mizu_node.types.query_service import QueryJobResult


async def save_new_query(
    session: AsyncSession,
    dataset: str,
    language: str,
    query_text: str,
    model: str,
    user: str,
    status: str = "pending",
) -> int:
    query_obj = Query(
        dataset=dataset,
        language=language,
        query_text=query_text,
        model=model,
        user=user,
        status=status,
    )
    session.add(query_obj)
    await session.flush()  # To get the ID
    return query_obj.id


async def add_query_result(
    session: AsyncSession,
    query_id: int,
    data_id: int,
    job_id: str,
) -> int:
    query_result = QueryResult(
        query_id=query_id,
        data_id=data_id,
        job_id=job_id,
        status="pending",
    )
    session.add(query_result)
    await session.flush()
    return query_result.id


async def save_query_result(
    session: AsyncSession,
    result: QueryJobResult,
) -> int:
    stmt = select(QueryResult).where(QueryResult.job_id == result.job_id)
    query_result = (await session.execute(stmt)).scalar_one_or_none()

    if query_result:
        # Update existing record
        query_result.result = result.batch_classify_result or result.error_result
        query_result.finished_at = datetime.now(timezone.utc)
        query_result.status = "error" if result.error_result else "processed"

        # Update the query's total_processed
        stmt = select(Query).where(Query.id == query_result.query_id)
        query = (await session.execute(stmt)).scalar_one_or_none()
        if query:
            query.total_processed = query.total_processed + 1
    else:
        raise HTTPException(status_code=404, detail="QueryResult not found")

    await session.flush()
    return query_result.id


async def get_query_results(
    session: AsyncSession, query_id: int, page: int = 1, page_size: int = 1000
) -> tuple[list, int]:
    # Get total count
    stmt = select(QueryResult).where(QueryResult.query_id == query_id)
    total = len((await session.execute(stmt)).scalars().all())

    # Get paginated results
    stmt = (
        select(QueryResult)
        .where(
            QueryResult.query_id == query_id,
            QueryResult.status == "processed",
            QueryResult.result.isnot(None),
        )
        .order_by(QueryResult.id)
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    results = (await session.execute(stmt)).scalars().all()

    return results, total


async def get_query_status(session: AsyncSession, query_id: int) -> dict:
    stmt = select(Query).where(Query.id == query_id)
    query = (await session.execute(stmt)).scalar_one_or_none()
    if not query:
        return None

    # Get dataset size
    stmt = select(Dataset).where(
        Dataset.name == query.dataset, Dataset.language == query.language
    )
    dataset_size = len((await session.execute(stmt)).scalars().all())

    # Get query results count
    stmt = select(QueryResult).where(QueryResult.query_id == query_id)
    query_results_count = len((await session.execute(stmt)).scalars().all())

    return {
        "query_id": query.id,
        "dataset_id": query.dataset_id,
        "processed_records": query.progress,
        "created_at": query.created_at,
        "language": query.language,
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
    """Update query status and last published data_id"""
    stmt = (
        update(Query)
        .where(Query.id == query_id)
        .values(status=status, last_data_id_published=last_data_id)
    )
    await session.execute(stmt)
    await session.commit()
