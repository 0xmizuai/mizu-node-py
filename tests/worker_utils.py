import json
import time
from mizu_node.constants import MAX_UNCLAIMED_REWARD
from mizu_node.security import (
    blocked_worker_key,
    mined_points_per_day_key,
    mined_points_per_hour_key,
    unclaimed_reward_key,
    worker_cooldown_key,
)
from mizu_node.types.data_job import JobType
from tests.redis_mock import RedisMock


def block_worker(rclient: RedisMock, worker: str):
    rclient.set(
        blocked_worker_key(worker),
        json.dumps({"blocked": True, "updated_at": int(time.time())}),
    )


def set_reward_stats_strict(rclient: RedisMock, worker: str):
    epoch = int(time.time()) // 3600
    # set past 24 hours stats
    keys = [mined_points_per_hour_key(worker, epoch - i) for i in range(0, 24)]
    rclient.mset({k: "50" for k in keys})

    day = int(time.time()) // 86400
    keys = [mined_points_per_day_key(worker, day - i) for i in range(1, 8)]
    rclient.mset({k: "200" for k in keys})


def set_reward_stats(rclient: RedisMock, worker: str):
    day = int(time.time()) // 86400
    rclient.set(mined_points_per_day_key(worker, day - 1), 200)


def set_unclaimed_reward(rclient: RedisMock, worker: str):
    rclient.set(unclaimed_reward_key(worker), MAX_UNCLAIMED_REWARD)


def set_cooldown(rclient: RedisMock, worker: str, job_type: JobType):
    cooldown_key = worker_cooldown_key(worker, job_type)
    rclient.set(cooldown_key, int(time.time()), ex=60)


def clear_cooldown(rclient: RedisMock, worker: str, job_type: JobType):
    cooldown_key = worker_cooldown_key(worker, job_type)
    rclient.delete(cooldown_key)
