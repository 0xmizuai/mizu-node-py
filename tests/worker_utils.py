import json
import time
from mizu_node.security import (
    BLOCKED_FIELD,
    REWARD_FIELD,
    event_name,
    last_requested_field,
    mined_per_day_field,
    mined_per_hour_field,
)
from mizu_node.types.data_job import JobType, RewardJobRecord, RewardJobRecords
from tests.redis_mock import RedisMock


def block_worker(rclient: RedisMock, worker: str):
    rclient.hset(
        event_name(worker),
        BLOCKED_FIELD,
        json.dumps({"blocked": True, "updated_at": int(time.time())}),
    )


def set_reward_stats_strict(rclient: RedisMock, worker: str):
    epoch = int(time.time()) // 3600
    # set past 24 hours stats
    keys = [mined_per_hour_field(epoch - i) for i in range(0, 24)]
    rclient.hmset(event_name(worker), {k: "50" for k in keys})

    day = int(time.time()) // 86400
    keys = [mined_per_day_field(day - i) for i in range(0, 7)]
    rclient.hmset(event_name(worker), {k: "200" for k in keys})


def set_reward_stats(rclient: RedisMock, worker: str):
    day = int(time.time()) // 86400
    keys = [mined_per_day_field(day - i) for i in range(0, 7)]
    rclient.hmset(event_name(worker), {k: "200" for k in keys})


def set_unclaimed_reward(r_client, worker: str):
    data = [
        RewardJobRecord(job_id="0x123", issued_at=int(time.time())) for _ in range(5)
    ]
    r_client.hset(
        event_name(worker), REWARD_FIELD, RewardJobRecords(data=data).model_dump_json()
    )


def set_cooldown(rclient: RedisMock, worker: str, job_type: JobType):
    rclient.hset(event_name(worker), last_requested_field(job_type), int(time.time()))


def clear_cooldown(rclient: RedisMock, worker: str, job_type: JobType):
    rclient.hset(event_name(worker), last_requested_field(job_type), 0)
