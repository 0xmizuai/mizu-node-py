from contextlib import contextmanager
import logging
import os
import psycopg2
import redis


class Connections:
    def __init__(self):
        self.postgres_url = os.environ["POSTGRES_URL"]
        logging.info(f"Connecting to postgres at {self.postgres_url}")

        REDIS_URL = os.environ["REDIS_URL"]
        logging.info(f"Connecting to redis at {REDIS_URL}")
        self.redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        logging.info(f"Connected to redis at {REDIS_URL}")

    @contextmanager
    def get_pg_connection(self):
        conn = None
        try:
            conn = psycopg2.connect(self.postgres_url)
            yield conn
        finally:
            if conn:
                conn.close()
                logging.debug("Connection closed")
