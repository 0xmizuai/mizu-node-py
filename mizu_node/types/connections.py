from contextlib import asynccontextmanager
import logging
import os
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker


class Connections:
    def __init__(self):
        # Convert PostgreSQL URL to async format
        self.postgres_url = os.environ["POSTGRES_URL"].replace(
            "postgresql://", "postgresql+asyncpg://"
        )
        logging.info(f"Connecting to postgres at {self.postgres_url}")

        self.engine = create_async_engine(self.postgres_url, echo=False)
        self.async_session = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

        REDIS_URL = os.environ["REDIS_URL"]
        logging.info(f"Connecting to redis at {REDIS_URL}")
        self.redis = Redis.from_url(REDIS_URL, decode_responses=True)
        logging.info(f"Connected to redis at {REDIS_URL}")

    @asynccontextmanager
    async def get_db_session(self):
        async with self.async_session() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
