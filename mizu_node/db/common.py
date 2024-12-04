from contextlib import closing
import logging

import functools
from pathlib import Path
from typing import Callable, TypeVar, ParamSpec

import psycopg2


logging.basicConfig(level=logging.INFO)  # Set the desired logging level

T = TypeVar("T")
P = ParamSpec("P")


def with_transaction(func: Callable[P, T]) -> Callable[P, T]:
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        db = args[0]  # Get the actual db connection
        try:
            result = func(*args, **kwargs)
            db.commit()
            return result
        except Exception as e:
            db.rollback()
            logging.error(f"Transaction failed in {func.__name__}: {e}")
            raise

    return wrapper


def load_sql_file(filename: str) -> str:
    """Load SQL file from db/sql directory"""
    current_dir = Path(__file__).parent
    sql_path = current_dir / "sqls" / filename
    with open(sql_path, "r") as f:
        return f.read()


def initiate_db(conn: psycopg2.extensions.connection):
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
                AND table_name = 'api_keys'
            );
        """
        )
        if not cur.fetchone()[0]:
            api_key_sql = load_sql_file("api_key.sql")
            cur.execute(api_key_sql)

        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'classifiers'
            );
        """
        )
        if not cur.fetchone()[0]:
            classifier_sql = load_sql_file("classifier.sql")
            cur.execute(classifier_sql)

        conn.commit()
