from redis import Redis

from mizu_node.common import epoch
from mizu_node.constants import (
    REWARD_TTL,
)
from mizu_node.types.data_job import (
    JobType,
    RewardContext,
    RewardJobRecord,
    RewardJobRecords,
    WorkerJob,
)

REWARD_FIELD = "reward"
LAST_REWARDED_AT_FIELD = "last_rewarded_at"


def event_name(worker: str):
    return f"event:{worker}"


def rate_limit_field(job_type: JobType) -> str:
    return f"rate_limit:{str(job_type)}"


def mined_per_hour_field(hour: int):
    return f"mined_per_hour:{hour}"


def mined_per_day_field(day: int):
    return f"mined_per_day:{day}"


def total_mined_points_per_hour_key(hour: int):
    return f"mined_points:per_hour:{hour}"


def total_mined_points_per_day_key(day: int):
    return f"mined_points:per_day:{day}"


def total_rewarded_per_hour_key(token: str, hour: int):
    return f"rewarded_{token}:per_hour:{hour}"


def total_rewarded_per_day_key(token: str, day: int):
    return f"rewarded_{token}:per_day:{day}"


def record_mined_points(rclient: Redis, worker: str, points: float):
    if points > 0.0:
        now = epoch()
        hour = now // 3600
        day = now // 86400
        pipeline = rclient.pipeline()
        pipeline.hincrbyfloat(event_name(worker), mined_per_hour_field(hour), points)
        pipeline.hincrbyfloat(event_name(worker), mined_per_day_field(day), points)
        pipeline.incrbyfloat(total_mined_points_per_hour_key(hour), points)
        pipeline.incrbyfloat(total_mined_points_per_day_key(day), points)
        pipeline.execute()


def record_reward_event(rclient: Redis, worker: str, job: WorkerJob):
    rewards = get_valid_rewards(rclient, worker)
    rewards.jobs.append(
        RewardJobRecord(
            job_id=job.job_id, reward_ctx=job.reward_ctx, assigned_at=epoch()
        )
    )
    rclient.hmset(
        event_name(worker),
        {
            REWARD_FIELD: rewards.model_dump_json(),
            LAST_REWARDED_AT_FIELD: str(epoch()),
        },
    )


def total_mined_points_in_past_n_hour_per_worker(
    rclient: Redis, worker: str, n: int
) -> float:
    hour = epoch() // 3600
    fields = [mined_per_hour_field(hour - i) for i in range(0, n)]
    values = rclient.hmget(event_name(worker), fields)
    return sum([float(v or 0) for v in values])


def total_mined_points_in_past_n_days_per_worker(
    rclient: Redis, worker: str, n: int
) -> float:
    day = epoch() // 86400
    fields = [mined_per_day_field(day - i) for i in range(0, n)]
    values = rclient.hmget(event_name(worker), fields)
    return sum([float(v or 0) for v in values])


def total_mined_points_in_past_n_hour(rclient: Redis, token: str, n: int):
    hour = epoch() // 3600
    keys = [total_mined_points_per_hour_key(token, hour - i) for i in range(0, n)]
    values = rclient.mget(keys)
    return sum([float(v or 0) for v in values])


def total_mined_points_in_past_n_days(rclient: Redis, token: str, n: int):
    day = epoch() // 86400
    keys = [total_mined_points_per_day_key(token, day - i) for i in range(0, n)]
    values = rclient.mget(keys)
    return sum([float(v or 0) for v in values])


def total_rewarded_in_past_n_hour(rclient: Redis, token: str, n: int):
    hour = epoch() // 3600
    keys = [total_rewarded_per_hour_key(token, hour - i) for i in range(0, n)]
    values = rclient.mget(keys)
    return sum([float(v or 0) for v in values])


def total_rewarded_in_past_n_days(rclient: Redis, token: str, n: int):
    day = epoch() // 86400
    keys = [total_rewarded_per_day_key(token, day - i) for i in range(0, n)]
    values = rclient.mget(keys)
    return sum([float(v or 0) for v in values])


def get_valid_rewards(rclient: Redis, worker: str) -> RewardJobRecords:
    name = event_name(worker)
    value = rclient.hget(name, REWARD_FIELD)
    rewards = (
        RewardJobRecords.model_validate_json(value) if value else RewardJobRecords()
    )
    rewards.jobs = [r for r in rewards.jobs if r.assigned_at + REWARD_TTL > epoch()]
    return rewards


def get_token_name(ctx: RewardContext) -> str:
    return "point" if ctx.token is None else "usdt"


def record_claim_event(rclient: Redis, worker: str, job_id: str, ctx: RewardContext):
    rewards = get_valid_rewards(rclient, worker)
    rewards.jobs = [r for r in rewards.jobs if r.job_id != job_id]
    now = epoch()
    hour = now // 3600
    day = now // 86400
    token_name = get_token_name(ctx)
    pipeline = rclient.pipeline()
    pipeline.incrbyfloat(
        total_rewarded_per_hour_key(token_name, hour), float(ctx.amount)
    )
    pipeline.incrbyfloat(total_rewarded_per_day_key(token_name, day), float(ctx.amount))
    pipeline.hset(event_name(worker), REWARD_FIELD, rewards.model_dump_json())
    pipeline.execute()


def try_remove_reward_record(rclient: Redis, worker: str, job_id: str):
    rewards = get_valid_rewards(rclient, worker)
    filtered = [r for r in rewards.jobs if r.job_id != job_id]
    if len(filtered) < len(rewards.jobs):
        rewards.jobs = filtered
        rclient.hset(event_name(worker), REWARD_FIELD, rewards.model_dump_json())
