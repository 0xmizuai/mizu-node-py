from contextlib import contextmanager
import logging
import os
import psycopg2
import redis
from pymongo import MongoClient
from asyncio import Semaphore
import time


class Connections:
    def __init__(self):
        self.postgres_url = os.environ["POSTGRES_URL"] + "?connect_timeout=5"
        # Limit concurrent connections
        self.connection_semaphore = Semaphore(
            20
        )  # Adjust number based on your PostgreSQL max_connections
        self.active_connections = 0
        logging.info(
            f"Initialized postgres connection config with max 20 concurrent connections"
        )

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
    async def get_pg_connection(self):
        """Get a connection with concurrency control."""
        start_time = time.time()

        async with self.connection_semaphore:  # Wait if too many connections
            self.active_connections += 1
            logging.info(f"Creating connection ({self.active_connections} active)")

            conn = None
            try:
                conn = psycopg2.connect(self.postgres_url)
                yield conn
            finally:
                if conn:
                    conn.close()
                self.active_connections -= 1
                logging.debug(
                    f"Connection closed. Active: {self.active_connections}. "
                    f"Duration: {time.time() - start_time:.2f}s"
                )
