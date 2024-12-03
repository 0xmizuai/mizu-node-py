import logging
import os
from mongomock import MongoClient
import psycopg2
import redis


class Connections:
    def __init__(
        self,
        postgres: psycopg2.extensions.connection | None = None,
        redis: redis.Redis | None = None,
        mdb: MongoClient | None = None,
    ):
        if postgres is None:
            POSTGRES_URL = os.environ["POSTGRES_URL"]
            logging.info(f"Connecting to postgres at {POSTGRES_URL}")
            self.postgres = psycopg2.connect(POSTGRES_URL)
            logging.info(f"Connected to postgres at {POSTGRES_URL}")
        else:
            self.postgres = postgres

        if redis is None:
            REDIS_URL = os.environ["REDIS_URL"]
            logging.info(f"Connecting to redis at {REDIS_URL}")
            self.redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)
            logging.info(f"Connected to redis at {REDIS_URL}")
        else:
            self.redis = redis

        if mdb is None:
            MIZU_NODE_MONGO_URL = os.environ["MIZU_NODE_MONGO_URL"]
            logging.info(f"Connecting to mongo at {MIZU_NODE_MONGO_URL}")
            MIZU_NODE_MONGO_DB_NAME = os.environ.get(
                "MIZU_NODE_MONGO_DB_NAME", "mizu_node"
            )
            self.mdb = MongoClient(MIZU_NODE_MONGO_URL)[MIZU_NODE_MONGO_DB_NAME]
            logging.info(f"Connected to mongo at {os.environ['MIZU_NODE_MONGO_URL']}")
        else:
            self.mdb = mdb
