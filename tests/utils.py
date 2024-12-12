from contextlib import closing
from pathlib import Path

import psycopg2


def load_sql_file(filename: str) -> str:
    """Load SQL file from db/sql directory"""
    current_dir = Path(__file__).parent
    sql_path = current_dir / ".." / "mizu_node" / "db" / "sqls" / filename
    with open(sql_path, "r") as f:
        return f.read()


def initiate_pg_db(conn: psycopg2.extensions.connection):
    with closing(conn.cursor()) as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        # Check if tables exist before running SQL files
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'job_queue'
            );
        """
        )
        if not cur.fetchone()[0]:
            job_queue_sql = load_sql_file("job_queue.sql")
            cur.execute(job_queue_sql)

        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'queries'
            );
        """
        )
        if not cur.fetchone()[0]:
            query_sql = load_sql_file("query.sql")
            cur.execute(query_sql)

        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'query_results'
            );
        """
        )
        if not cur.fetchone()[0]:
            query_result_sql = load_sql_file("query_result.sql")
            cur.execute(query_result_sql)

        conn.commit()
