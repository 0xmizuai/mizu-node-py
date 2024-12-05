import json
from mizu_node.common import epoch
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
from tests.redis_mock import RedisMock
from psycopg2.extensions import connection


def block_worker(rclient: RedisMock, worker: str):
    rclient.hset(
        event_name(worker),
        BLOCKED_FIELD,
        json.dumps({"blocked": True, "updated_at": epoch()}),
    )


def set_reward_stats_strict(rclient: RedisMock, worker: str):
    epoch = epoch() // 3600
    # set past 24 hours stats
    keys = [mined_per_hour_field(epoch - i) for i in range(0, 24)]
    rclient.hmset(event_name(worker), {k: "50" for k in keys})

    day = epoch() // 86400
    keys = [mined_per_day_field(day - i) for i in range(0, 7)]
    rclient.hmset(event_name(worker), {k: "200" for k in keys})


def set_reward_stats(rclient: RedisMock, worker: str):
    day = epoch() // 86400
    keys = [mined_per_day_field(day - i) for i in range(0, 7)]
    rclient.hmset(event_name(worker), {k: "200" for k in keys})


def set_unclaimed_reward(pg_conn: connection, worker: str, total: int = 5):
    """Insert reward jobs into job_queue table."""
    with pg_conn.cursor() as cur:
        # Prepare batch insert values
        current_time = epoch()
        values = [
            (
                JobType.reward,
                JobStatus.processing,  # or 'pending' based on your needs
                worker,
                current_time,  # assigned_at
                current_time + 3600,  # lease_expired_at (1 hour TTL)
                current_time,  # published_at
                0,  # retry count
                DataJobContext(
                    reward_ctx=RewardContext(amount=100)
                ).model_dump_json(),  # ctx as JSON
            )
            for _ in range(total)
        ]

        # Batch insert
        cur.executemany(
            """
            INSERT INTO job_queue (
                job_type, status, worker,
                assigned_at, lease_expired_at, published_at,
                retry, ctx
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            values,
        )
        pg_conn.commit()


def set_one_unclaimed_reward(pg_conn: connection, worker: str):
    set_unclaimed_reward(pg_conn, worker, 1)


def set_cooldown(rclient: RedisMock, worker: str, job_type: JobType):
    rclient.hset(event_name(worker), rate_limit_field(job_type), str(epoch()))


def clear_cooldown(rclient: RedisMock, worker: str, job_type: JobType):
    rclient.hset(event_name(worker), rate_limit_field(job_type), "0")
