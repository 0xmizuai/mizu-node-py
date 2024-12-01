import os
from unittest import mock
from fastapi import HTTPException
import pytest

from mizu_node.security import validate_worker
from mizu_node.stats import record_claim_event, record_reward_event
from mizu_node.types.data_job import JobType, RewardContext, WorkerJob
from tests.redis_mock import RedisMock
from tests.worker_utils import (
    block_worker,
    clear_cooldown,
    set_reward_stats,
    set_unclaimed_reward,
)


@pytest.fixture()
def setenvvar(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "ACTIVE_USER_PAST_7D_THRESHOLD": "50",
            "MIN_REWARD_GAP": "60",
            "ENABLE_ACTIVE_USER_CHECK": "true",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield


def test_validate_worker_blocked(setenvvar):
    r_client = RedisMock()

    # given user1 is blocked
    user1 = "some_user"
    block_worker(r_client, user1)

    # should throw
    with pytest.raises(HTTPException) as e:
        validate_worker(r_client, user1, JobType.pow)
    assert e.value.status_code == 403
    assert e.value.detail == "worker is blocked"


def test_validate_worker_cooldown(setenvvar):
    r_client = RedisMock()

    # given user2 has cooldown
    user2 = "some_user2"
    set_reward_stats(r_client, user2)
    validate_worker(r_client, user2, JobType.reward)

    # should throw
    with pytest.raises(HTTPException) as e:
        validate_worker(r_client, user2, JobType.reward)
    assert e.value.status_code == 429
    assert e.value.detail.startswith("please retry after")

    # skip 10 requests
    for _ in range(10):
        validate_worker(r_client, user2, JobType.pow)
    with pytest.raises(HTTPException) as e:
        validate_worker(r_client, user2, JobType.pow)
    assert e.value.status_code == 429
    assert e.value.detail.startswith("please retry after")

    # give user cooldown is cleared
    clear_cooldown(r_client, user2, JobType.pow)

    # should not throw
    validate_worker(r_client, user2, JobType.pow)


def test_validate_worker_active_user(setenvvar):
    r_client = RedisMock()

    # given user3 is not active
    user3 = "some_user3"

    # should throw
    with pytest.raises(HTTPException) as e:
        validate_worker(r_client, user3, JobType.reward)
    assert e.value.status_code == 403
    assert e.value.detail == "not active user"

    # give user4 is active
    user4 = "some_user4"
    set_reward_stats(r_client, user4)

    # should not throw
    validate_worker(r_client, user4, JobType.reward)


def test_validate_worker_already_rewarded(setenvvar):
    r_client = RedisMock()

    # given user5 is active and has been rewarded
    user5 = "some_user5"
    set_reward_stats(r_client, user5)
    record_reward_event(
        r_client,
        user5,
        WorkerJob(
            job_id="some_job",
            job_type=JobType.reward,
            reward_ctx=RewardContext(amount=100),
        ),
    )

    # should throw
    with pytest.raises(HTTPException) as e:
        validate_worker(r_client, user5, JobType.reward)
    assert e.value.status_code == 429
    assert e.value.detail.startswith("please retry after")


def test_validate_worker_full_pocket(setenvvar):
    r_client = RedisMock()

    # give user 6 is active and
    user6 = "some_user6"
    set_reward_stats(r_client, user6)
    set_unclaimed_reward(r_client, user6)

    # should throw
    with pytest.raises(HTTPException) as e:
        validate_worker(r_client, user6, JobType.reward)
    assert e.value.status_code == 403
    assert e.value.detail == "unclaimed reward limit reached"
