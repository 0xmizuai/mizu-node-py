import json
from mizu_node.common import epoch
from mizu_node.db.orm.job_queue import JobQueue
from mizu_node.security import (
    BLOCKED_FIELD,
)
from mizu_node.stats import (
    event_name,
    mined_per_day_field,
    mined_per_hour_field,
    rate_limit_field,
)
from mizu_node.types.data_job import (
    DataJobContext,
    JobStatus,
    JobType,
    RewardContext,
)
from tests.redis_mock import AsyncRedisMock
from sqlalchemy.ext.asyncio import AsyncSession


async def block_worker(rclient: AsyncRedisMock, worker: str):
    await rclient.hset(
        event_name(worker),
        BLOCKED_FIELD,
        json.dumps({"blocked": True, "updated_at": epoch()}),
    )


async def set_reward_stats_strict(rclient: AsyncRedisMock, worker: str):
    epoch = epoch() // 3600
    # set past 24 hours stats
    keys = [mined_per_hour_field(epoch - i) for i in range(0, 24)]
    await rclient.hmset(event_name(worker), {k: "50" for k in keys})

    day = epoch() // 86400
    keys = [mined_per_day_field(day - i) for i in range(0, 7)]
    await rclient.hmset(event_name(worker), {k: "200" for k in keys})


async def set_reward_stats(rclient: AsyncRedisMock, worker: str):
    day = epoch() // 86400
    keys = [mined_per_day_field(day - i) for i in range(0, 7)]
    await rclient.hmset(event_name(worker), {k: "200" for k in keys})


async def set_unclaimed_reward(session: AsyncSession, worker: str, total: int = 5):
    """Insert reward jobs into job_queue table using ORM."""
    current_time = epoch()
    jobs = [
        JobQueue(
            job_type=JobType.reward,
            status=JobStatus.processing,
            worker=worker,
            assigned_at=current_time,
            lease_expired_at=current_time + 3600,
            published_at=current_time,
            retry=0,
            ctx=DataJobContext(
                reward_ctx=RewardContext(amount=100)
            ).model_dump(),  # Note: model_dump instead of model_dump_json for JSON column
        )
        for _ in range(total)
    ]
    session.add_all(jobs)
    await session.flush()


async def set_one_unclaimed_reward(session: AsyncSession, worker: str):
    await set_unclaimed_reward(session, worker, 1)


async def set_cooldown(rclient: AsyncRedisMock, worker: str, job_type: JobType):
    await rclient.hset(event_name(worker), rate_limit_field(job_type), str(epoch()))


async def clear_cooldown(rclient: AsyncRedisMock, worker: str, job_type: JobType):
    await rclient.hset(event_name(worker), rate_limit_field(job_type), "0")
