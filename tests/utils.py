from pathlib import Path
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def load_sql_file(filename: str) -> str:
    """Load SQL file from db/sql directory"""
    current_dir = Path(__file__).parent
    sql_path = current_dir / ".." / "mizu_node" / "db" / "sqls" / filename
    with open(sql_path, "r") as f:
        return f.read()


async def initiate_job_db(session: AsyncSession):
    async with session.begin():
        raw_conn = await (await session.connection()).get_raw_connection()
        pg_conn = raw_conn.driver_connection

        await session.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto;"))
        # Check if tables exist before running SQL files
        result = await session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'job_queue'
                );
                """
            )
        )
        if not result.scalar_one():
            job_queue_sql = load_sql_file("job_queue.sql")
            await pg_conn.execute(job_queue_sql)
            await session.commit()


async def initiate_query_db(session: AsyncSession):
    async with session.begin():
        raw_conn = await (await session.connection()).get_raw_connection()
        pg_conn = raw_conn.driver_connection
        result = await session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'queries'
                );
                """
            )
        )
        if not result.scalar_one():
            query_sql = load_sql_file("query.sql")
            await pg_conn.execute(query_sql)

        result = await session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'query_results'
                );
                """
            )
        )
        if not result.scalar_one():
            query_result_sql = load_sql_file("query_result.sql")
            await pg_conn.execute(query_result_sql)

        await session.commit()
