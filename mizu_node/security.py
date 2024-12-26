from fastapi import HTTPException, status
from redis import Redis

from mizu_node.common import epoch
from mizu_node.config import get_cooldown_config

from mizu_node.stats import (
    event_name,
    rate_limit_field,
)
from mizu_node.types.data_job import JobType


def validate_worker(redis: Redis, worker: str, job_type: JobType) -> bool:
    config = get_cooldown_config(job_type)
    rate_limit_key = rate_limit_field(job_type)
    req_ts_values = redis.hget(event_name(worker), rate_limit_key)
    request_ts = (req_ts_values or "0").split(",")
    now = epoch()
    request_ts = [ts for ts in request_ts if int(ts) > now - config.interval]
    if len(request_ts) >= config.limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"please retry after {config.interval} seconds",
        )
    request_ts.append(str(now))
    redis.hset(event_name(worker), rate_limit_key, ",".join(request_ts))
