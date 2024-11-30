from fastapi import HTTPException
from pydantic import BaseModel
from redis import Redis

from fastapi import status

from mizu_node.common import epoch


class RateLimitConfig(BaseModel):
    interval: int
    limit: int


default_config = RateLimitConfig(interval=60, limit=10)


def per_key_rate_limit(
    rclient: Redis, key: str, config: RateLimitConfig = default_config
) -> RateLimitConfig:
    rate_limit_key = f"rate_limit:{key}"
    request_timestamps = rclient.get(rate_limit_key)
    if request_timestamps is None:
        request_timestamps = []
    else:
        request_timestamps = request_timestamps.split(",")
    now = epoch()
    request_timestamps = [
        ts for ts in request_timestamps if int(ts) > now - config.interval
    ]
    if len(request_timestamps) >= config.limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"please retry after {config.interval} seconds",
        )
    request_timestamps.append(str(now))
    rclient.set(rate_limit_key, ",".join(request_timestamps))
