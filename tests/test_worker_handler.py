from fastapi import HTTPException
import pytest
import testing.postgresql

from mizu_node.security import validate_worker
from mizu_node.types.data_job import JobType
from tests.redis_mock import AsyncRedisMock
from tests.utils import initiate_job_db
from tests.utils import (
    block_worker,
    clear_cooldown,
    set_one_unclaimed_reward,
    set_reward_stats,
    set_unclaimed_reward,
)

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool


@pytest.fixture()
def setenvvar(monkeypatch):
    envvars = {
        "ACTIVE_USER_PAST_7D_THRESHOLD": "50",
        "MIN_REWARD_GAP": "60",
        "ENABLE_ACTIVE_USER_CHECK": "true",
    }
    for k, v in envvars.items():
        monkeypatch.setenv(k, v)
    yield


@pytest.fixture(scope="session")
def pq_db_url():
    """Create a PostgreSQL instance for testing."""
    with testing.postgresql.Postgresql() as postgresql:
        dsn = postgresql.dsn()
        url = f"postgresql+asyncpg://{dsn['user']}@{dsn['host']}:{dsn['port']}/{dsn['database']}"
        yield url


@pytest.fixture
async def job_db_session(pq_db_url):
    """Get a fresh connection for each test."""
    db_engine = create_async_engine(pq_db_url, echo=False, poolclass=NullPool)
    db_session = sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with db_session() as session:
        await initiate_job_db(session)
    return db_engine, db_session


@pytest.mark.asyncio
async def test_validate_worker_blocked(setenvvar, job_db_session):
    db_engine, db_session = await job_db_session
    r_client = AsyncRedisMock()

    # given user1 is blocked
    user1 = "some_user"
    await block_worker(r_client, user1)

    # should throw
    with pytest.raises(HTTPException) as e:
        async with db_session() as session:
            await validate_worker(r_client, session, user1, JobType.pow)
    assert e.value.status_code == 403
    assert e.value.detail == "worker is blocked"
    await db_engine.dispose()


@pytest.mark.asyncio
async def test_validate_worker_cooldown(setenvvar, job_db_session):
    db_engine, db_session = await job_db_session
    r_client = AsyncRedisMock()

    async with db_session() as session:
        # given user2 has cooldown
        user2 = "some_user2"
        await set_reward_stats(r_client, user2)
        await validate_worker(r_client, session, user2, JobType.reward)

        # should throw
        with pytest.raises(HTTPException) as e:
            await validate_worker(r_client, session, user2, JobType.reward)
        assert e.value.status_code == 429
        assert e.value.detail.startswith("please retry after")

        # skip 10 requests
        for _ in range(10):
            await validate_worker(r_client, session, user2, JobType.pow)
        with pytest.raises(HTTPException) as e:
            await validate_worker(r_client, session, user2, JobType.pow)
        assert e.value.status_code == 429
        assert e.value.detail.startswith("please retry after")

        # give user cooldown is cleared
        await clear_cooldown(r_client, user2, JobType.pow)

        # should not throw
        await validate_worker(r_client, session, user2, JobType.pow)
    await db_engine.dispose()


@pytest.mark.asyncio
async def test_validate_worker_active_user(setenvvar, job_db_session):
    db_engine, db_session = await job_db_session
    r_client = AsyncRedisMock()

    # given user3 is not active
    user3 = "some_user3"

    # should throw
    async with db_session() as session:
        with pytest.raises(HTTPException) as e:
            await validate_worker(r_client, session, user3, JobType.reward)
        assert e.value.status_code == 403
        assert e.value.detail == "not active user"

        # give user4 is active
        user4 = "some_user4"
        await set_reward_stats(r_client, user4)

        # should not throw
        await validate_worker(r_client, session, user4, JobType.reward)
    await db_engine.dispose()


@pytest.mark.asyncio
async def test_validate_worker_already_rewarded(setenvvar, job_db_session):
    db_engine, db_session = await job_db_session
    r_client = AsyncRedisMock()

    async with db_session() as session:
        # given user5 is active and has been rewarded
        user5 = "some_user5"
        await set_reward_stats(r_client, user5)

        # Insert a reward job
        await set_one_unclaimed_reward(session, user5)

        # should throw
        with pytest.raises(HTTPException) as e:
            await validate_worker(r_client, session, user5, JobType.reward)
        assert e.value.status_code == 429
        assert e.value.detail.startswith("please retry after")
    await db_engine.dispose()


@pytest.mark.asyncio
async def test_validate_worker_full_pocket(setenvvar, job_db_session):
    db_engine, db_session = await job_db_session
    r_client = AsyncRedisMock()

    async with db_session() as session:
        # give user 6 is active and
        user6 = "some_user6"
        await set_reward_stats(r_client, user6)
        await set_unclaimed_reward(session, user6)

        # should throw
        with pytest.raises(HTTPException) as e:
            await validate_worker(r_client, session, user6, JobType.reward)
        assert e.value.status_code == 403
        assert e.value.detail == "unclaimed reward limit reached"
    await db_engine.dispose()
