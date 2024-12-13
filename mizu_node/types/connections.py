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
        if redis is not None:
            self.redis = redis
        else:
            redis_url = os.environ.get("REDIS_URL", None)
            if redis_url:
                self.redis = AsyncRedis.from_url(redis_url, decode_responses=True)
                logging.info(f"Connected to redis at {redis_url}")
            else:
                self.redis = None

        job_db_url = (
            job_db_url
            or os.environ.get("JOB_DB_URL", None)
            or os.environ.get("POSTGRES_URL", None)
        )
        if job_db_url:
            self.job_db_url = job_db_url.replace(
                "postgresql://", "postgresql+asyncpg://"
            )
            logging.info(f"Connecting to postgres at {self.job_db_url}")

            self.job_db_engine = create_async_engine(
                self.job_db_url,
                echo=False,
                pool_size=20,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600,
            )
            self.job_db_session = sessionmaker(
                self.job_db_engine, class_=AsyncSession, expire_on_commit=False
            )
        else:
            self.job_db_engine = None
            self.job_db_session = None

        query_db_url = query_db_url or os.environ.get("QUERY_DB_URL", None)
        if query_db_url:
            self.query_db_url = query_db_url.replace(
                "postgresql://", "postgresql+asyncpg://"
            )
            logging.info(f"Connecting to postgres at {self.query_db_url}")
            self.query_db_engine = create_async_engine(
                self.query_db_url,
                echo=False,
                pool_size=20,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600,
            )
            self.query_db_session = sessionmaker(
                self.query_db_engine, class_=AsyncSession, expire_on_commit=False
            )
        else:
            self.query_db_engine = None
            self.query_db_session = None

    @asynccontextmanager
    async def get_job_db_session(self, *, autocommit=False):
        if not self.job_db_session:
            raise Exception("Job database is not initialized")

        async with self.job_db_session() as session:
            if autocommit:
                # For read-only operations
                session.sync_session.autocommit = True
            try:
                yield session
                if not autocommit:
                    await session.commit()
            except Exception:
                if not autocommit:
                    await session.rollback()
                raise

    @asynccontextmanager
    async def get_query_db_session(self, *, autocommit=False):
        if not self.query_db_session:
            raise Exception("Query database is not initialized")

        async with self.query_db_session() as session:
            if autocommit:
                # For read-only operations
                session.sync_session.autocommit = True
            try:
                yield session
                if not autocommit:
                    await session.commit()
            except Exception:
                if not autocommit:
                    await session.rollback()
                raise

    async def close(self):
        if self.job_db_engine:
            await self.job_db_engine.dispose()
        if self.query_db_engine:
            await self.query_db_engine.dispose()
        if self.redis:
            await self.redis.aclose()
