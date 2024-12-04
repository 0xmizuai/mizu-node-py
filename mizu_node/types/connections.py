from contextlib import contextmanager
import logging
import os
import redis
from pymongo import MongoClient
from psycopg2 import pool


class Connections:
    def __init__(self):
        self.postgres_url = os.environ["POSTGRES_URL"]
        logging.info(f"Connecting to postgres at {self.postgres_url}")

        # Create a connection pool with smaller max connections
        self.pg_pool = pool.SimpleConnectionPool(
            minconn=1, maxconn=5, dsn=self.postgres_url
        )
        # Track active connections
        self._active_connections = 0
        logging.info("Postgres connection pool created")

        REDIS_URL = os.environ["REDIS_URL"]
        logging.info(f"Connecting to redis at {REDIS_URL}")
        self.redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        logging.info(f"Connected to redis at {REDIS_URL}")

        MIZU_NODE_MONGO_URL = os.environ["MIZU_NODE_MONGO_URL"]
        logging.info(f"Connecting to mongo at {MIZU_NODE_MONGO_URL}")
        MIZU_NODE_MONGO_DB_NAME = os.environ.get("MIZU_NODE_MONGO_DB_NAME", "mizu_node")
        self.mdb = MongoClient(MIZU_NODE_MONGO_URL)[MIZU_NODE_MONGO_DB_NAME]
        logging.info(f"Connected to mongo at {os.environ['MIZU_NODE_MONGO_URL']}")

    @contextmanager
    def get_pg_connection(self):
        """Get a connection from the connection pool with tracking."""
        try:
            self._active_connections += 1
            logging.info(
                f"Getting connection. Active connections: {self._active_connections}"
            )

            conn = self.pg_pool.getconn()
            try:
                yield conn
            finally:
                self.pg_pool.putconn(conn)
                self._active_connections -= 1
                logging.info(
                    f"Released connection. Active connections: {self._active_connections}"
                )
        except Exception as e:
            self._active_connections -= 1
            logging.error(
                f"Connection error: {e}. Active connections: {self._active_connections}"
            )
            raise
