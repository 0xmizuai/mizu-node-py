import os
from unittest import mock
from fastapi import HTTPException
import pytest

from mizu_node.security import validate_worker
from mizu_node.types.data_job import JobType
from tests.redis_mock import RedisMock
from tests.worker_utils import clear_cooldown


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


def test_validate_worker_cooldown(setenvvar):
    r_client = RedisMock()

    # given user2 has cooldown
    user2 = "some_user2"
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
