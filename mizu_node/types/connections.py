from contextlib import asynccontextmanager
import logging
import os
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker


class Connections:
    def __init__(
        self,
        job_db_url: str | None = None,
        query_db_url: str | None = None,
        redis: AsyncRedis | None = None,
    ):
        self.job_db_url = job_db_url or os.environ["JOB_DB_URL"].replace(
            "postgresql://", "postgresql+asyncpg://"
        )
        logging.info(f"Connecting to postgres at {self.job_db_url}")

        self.job_db_engine = create_async_engine(self.job_db_url, echo=False)
        self.job_db_session = sessionmaker(
            self.job_db_engine, class_=AsyncSession, expire_on_commit=False
        )

        self.query_db_url = query_db_url or os.environ["QUERY_DB_URL"].replace(
            "postgresql://", "postgresql+asyncpg://"
        )
        logging.info(f"Connecting to postgres at {self.query_db_url}")
        self.query_db_engine = create_async_engine(self.query_db_url, echo=False)
        self.query_db_session = sessionmaker(
            self.query_db_engine, class_=AsyncSession, expire_on_commit=False
        )

        self.redis = redis or AsyncRedis.from_url(
            os.environ["REDIS_URL"], decode_responses=True
        )
        logging.info(f"Connected to redis at {os.environ['REDIS_URL']}")

    @asynccontextmanager
    async def get_job_db_session(self):
        async with self.job_db_session() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    @asynccontextmanager
    async def get_query_db_session(self):
        async with self.query_db_session() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def close(self):
        await self.job_db_engine.dispose()
        await self.query_db_engine.dispose()
        await self.redis.aclose()
