from typing import List, Optional
from psycopg2.extensions import connection
from psycopg_pool import AsyncConnectionPool
from dataclasses import dataclass

from mizu_node.db.common import with_transaction
from mizu_node.types.query import DataQuery, DataRecord, Dataset

QUERY_WITH_DATASET = """
    SELECT 
        q.id, q.dataset_id, q.query_text, q.model, q.user_id, q.last_record_published, q.status, q.created_at,
        d.name, d.language, d.data_type, d.total_objects, d.total_bytes, d.created_at, d.crawled_at, d.source, d.source_link
    FROM queries q
    JOIN datasets d ON q.dataset_id = d.id
"""


@dataclass
class PaginatedDataRecords:
    records: List[DataRecord]
    last_id: Optional[int]


@with_transaction
def get_dataset(db: connection, dataset_id: int) -> Dataset:
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT id, name, language, data_type, total_objects, total_bytes, created_at, crawled_at, source, source_link
        FROM datasets
        WHERE id = %s
        """,
        (dataset_id,),
    )
    result = cursor.fetchone()
    if result is None:
        raise ValueError(f"Dataset with id {dataset_id} not found")

    return Dataset(
        id=result[0],
        name=result[1],
        language=result[2],
        data_type=result[3],
        total_objects=result[4],
        total_bytes=result[5],
        created_at=result[6],
        crawled_at=result[7],
        source=result[8],
        source_link=result[9],
    )


@with_transaction
def get_unpublished_data_per_query(
    db: connection,
    query: DataQuery,
    start_after_id: Optional[int] = None,
    limit: int = 1000,
) -> PaginatedDataRecords:
    """
    Fetch unpublished data records after a specific ID.

    Args:
        db: Database connection
        query: The data query object
        start_after_id: ID to start fetching after (exclusive). If None, starts from query.last_record_published
        limit: Maximum number of records to return

    Returns:
        PaginatedDataRecords containing the records and the last ID for next pagination
    """
    cursor = db.cursor()
    effective_start_id = (
        start_after_id if start_after_id is not None else query.last_record_published
    )
    cursor.execute(
        """
        SELECT id, dataset_id, md5, num_of_records, decompressed_byte_size, byte_size, source, created_at
        FROM data_records
        WHERE dataset_id = %s AND id > %s
        ORDER BY id
        LIMIT %s
        """,
        (query.dataset.id, effective_start_id, limit),
    )
    results = cursor.fetchall()

    if not results:
        return PaginatedDataRecords(records=[], last_id=None)

    records = [
        DataRecord(
            id=row[0],
            dataset_id=row[1],
            md5=row[2],
            num_of_records=row[3],
            decompressed_byte_size=row[4],
            byte_size=row[5],
            source=row[6],
            created_at=row[7],
        )
        for row in results
    ]

    last_id = records[-1].id if records else None

    return PaginatedDataRecords(records=records, last_id=last_id)


async def get_unpublished_data_per_query_async(
    db: AsyncConnectionPool.connection,
    query: DataQuery,
    start_after_id: Optional[int] = None,
    limit: int = 1000,
) -> PaginatedDataRecords:
    """
    Fetch unpublished data records after a specific ID.

    Args:
        db: Database connection
        query: The data query object
        start_after_id: ID to start fetching after (exclusive). If None, starts from query.last_record_published
        limit: Maximum number of records to return

    Returns:
        PaginatedDataRecords containing the records and the last ID for next pagination
    """
    async with db.cursor() as cursor:
        effective_start_id = (
            start_after_id
            if start_after_id is not None
            else query.last_record_published
        )
        await cursor.execute(
            """
            SELECT id, dataset_id, md5, num_of_records, decompressed_byte_size, byte_size, source, created_at
            FROM data_records
            WHERE dataset_id = %s AND id > %s
            ORDER BY id
            LIMIT %s
            """,
            (query.dataset.id, effective_start_id, limit),
        )
        results = await cursor.fetchall()

        if not results:
            return PaginatedDataRecords(records=[], last_id=None)

        records = [
            DataRecord(
                id=row[0],
                dataset_id=row[1],
                md5=row[2],
                num_of_records=row[3],
                decompressed_byte_size=row[4],
                byte_size=row[5],
                source=row[6],
                created_at=row[7],
            )
            for row in results
        ]

        last_id = records[-1].id if records else None

        return PaginatedDataRecords(records=records, last_id=last_id)


@with_transaction
def update_query_status(db: connection, query: DataQuery) -> None:
    cursor = db.cursor()
    cursor.execute(
        """
        UPDATE queries
        SET status = %s, last_record_published = %s
        WHERE id = %s
        """,
        (query.status.value, query.last_record_published, query.id),
    )


async def update_query_status_async(
    db: AsyncConnectionPool.connection, query: DataQuery
) -> None:
    async with db.cursor() as cur:
        await cur.execute(
            """
        UPDATE queries
        SET status = %s, last_record_published = %s
        WHERE id = %s
        """,
            (query.status.value, query.last_record_published, query.id),
        )


@with_transaction
def add_query(db: connection, query: DataQuery) -> None:
    cursor = db.cursor()
    cursor.execute(
        """
        INSERT INTO queries (dataset_id, query_text, model, "user", last_record_published, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            query.dataset.id,
            query.query_text,
            query.model,
            query.user,
            query.last_record_published,
            query.status,
        ),
    )


@with_transaction
def get_query(db: connection, query_id: int) -> DataQuery:
    cursor = db.cursor()
    cursor.execute(f"{QUERY_WITH_DATASET} WHERE q.id = %s", (query_id,))
    result = cursor.fetchone()
    if result is None:
        raise ValueError(f"Query with id {query_id} not found")

    return _create_query_from_result(result)


@with_transaction
def get_unpublished_query(db: connection) -> DataQuery:
    cursor = db.cursor()
    cursor.execute(f"{QUERY_WITH_DATASET} WHERE q.status = 0 LIMIT 1")
    result = cursor.fetchone()
    if result is None:
        raise ValueError("No unpublished query found")

    return _create_query_from_result(result)


async def get_unpublished_queries(
    db: AsyncConnectionPool.connection, limit: int
) -> List[DataQuery]:
    async with db.cursor() as cur:
        await cur.execute(
            f"{QUERY_WITH_DATASET} WHERE q.status = 0 ORDER BY q.id LIMIT {limit}"
        )
        results = await cur.fetchall()
        return [_create_query_from_result(row) for row in results]


def _create_query_from_result(result) -> DataQuery:
    return DataQuery(
        id=result[0],
        dataset=Dataset(
            id=result[1],
            name=result[8],
            language=result[9],
            data_type=result[10],
            total_objects=result[11],
            total_bytes=result[12],
            created_at=result[13],
            crawled_at=result[14],
            source=result[15],
            source_link=result[16],
        ),
        query_text=result[2],
        model=result[3],
        user=result[4],
        last_record_published=result[5],
        status=result[6],
        created_at=result[7],
    )
