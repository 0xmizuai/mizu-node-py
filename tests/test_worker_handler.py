from fastapi import HTTPException
import pytest
import testing.postgresql
import psycopg2

from mizu_node.security import validate_worker
from mizu_node.types.data_job import JobType
from tests.redis_mock import AsyncRedisMock
from tests.worker_utils import (
    block_worker,
    clear_cooldown,
    set_one_unclaimed_reward,
    set_reward_stats,
    set_unclaimed_reward,
)


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
def postgresql():
    """Create a PostgreSQL instance for testing."""
    with testing.postgresql.Postgresql() as postgresql:
        yield postgresql


@pytest.fixture
def pg_conn(postgresql):
    """Get a fresh connection for each test."""
    conn = psycopg2.connect(**postgresql.dsn())
    yield conn
    conn.close()


async def test_validate_worker_blocked(setenvvar, pg_conn):
    r_client = AsyncRedisMock()

    # given user1 is blocked
    user1 = "some_user"
    block_worker(r_client, user1)

    # should throw
    with pytest.raises(HTTPException) as e:
        await validate_worker(r_client, pg_conn, user1, JobType.pow)
    assert e.value.status_code == 403
    assert e.value.detail == "worker is blocked"


async def test_validate_worker_cooldown(setenvvar, pg_conn):
    r_client = AsyncRedisMock()

    # given user2 has cooldown
    user2 = "some_user2"
    await set_reward_stats(r_client, user2)
    await validate_worker(r_client, pg_conn, user2, JobType.reward)

    # should throw
    with pytest.raises(HTTPException) as e:
        await validate_worker(r_client, pg_conn, user2, JobType.reward)
    assert e.value.status_code == 429
    assert e.value.detail.startswith("please retry after")

    # skip 10 requests
    for _ in range(10):
        await validate_worker(r_client, pg_conn, user2, JobType.pow)
    with pytest.raises(HTTPException) as e:
        await validate_worker(r_client, pg_conn, user2, JobType.pow)
    assert e.value.status_code == 429
    assert e.value.detail.startswith("please retry after")

    # give user cooldown is cleared
    clear_cooldown(r_client, user2, JobType.pow)

    # should not throw
    await validate_worker(r_client, pg_conn, user2, JobType.pow)


async def test_validate_worker_active_user(setenvvar, pg_conn):
    r_client = AsyncRedisMock()

    # given user3 is not active
    user3 = "some_user3"

    # should throw
    with pytest.raises(HTTPException) as e:
        await validate_worker(r_client, pg_conn, user3, JobType.reward)
    assert e.value.status_code == 403
    assert e.value.detail == "not active user"

    # give user4 is active
    user4 = "some_user4"
    await set_reward_stats(r_client, user4)

    # should not throw
    await validate_worker(r_client, pg_conn, user4, JobType.reward)


async def test_validate_worker_already_rewarded(setenvvar, pg_conn):
    r_client = AsyncRedisMock()

    # given user5 is active and has been rewarded
    user5 = "some_user5"
    await set_reward_stats(r_client, user5)

    # Insert a reward job
    await set_one_unclaimed_reward(pg_conn, user5)

    # should throw
    with pytest.raises(HTTPException) as e:
        await validate_worker(r_client, pg_conn, user5, JobType.reward)
    assert e.value.status_code == 429
    assert e.value.detail.startswith("please retry after")


async def test_validate_worker_full_pocket(setenvvar, pg_conn):
    r_client = AsyncRedisMock()

    # give user 6 is active and
    user6 = "some_user6"
    await set_reward_stats(r_client, user6)
    await set_unclaimed_reward(pg_conn, user6)

    # should throw
    with pytest.raises(HTTPException) as e:
        await validate_worker(r_client, pg_conn, user6, JobType.reward)
    assert e.value.status_code == 403
    assert e.value.detail == "unclaimed reward limit reached"
