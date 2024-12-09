import os

from fastapi import HTTPException, status
from redis import Redis

from mizu_node.common import epoch
from mizu_node.constants import (
    ACTIVE_USER_PAST_7D_THRESHOLD,
    COOLDOWN_WORKER_EXPIRE_TTL_SECONDS,
    MAX_UNCLAIMED_REWARD,
    MIN_REWARD_GAP,
    MIZU_ADMIN_USER,
    REWARD_TTL,
)
import jwt

from mizu_node.db.api_key import get_user_id
from mizu_node.db.job_queue import get_reward_jobs_stats
from mizu_node.stats import (
    event_name,
    mined_per_day_field,
    rate_limit_field,
)
from mizu_node.types.data_job import (
    JobType,
)
from mizu_node.types.service import CooldownConfig
from psycopg2.extensions import connection

ALGORITHM = "EdDSA"
BLOCKED_FIELD = "blocked_worker"


def verify_jwt(token: str, public_key: str) -> str:
    """verify and return user is from token, raise otherwise"""
    try:
        # Decode and validate: expiration is automatically taken care of
        payload = jwt.decode(jwt=token, key=public_key, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Token is invalid"
            )
        return str(user_id)
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired"
        )
    except jwt.InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token verification failed"
        )


def verify_api_key(pg_conn: connection, token: str) -> str:
    if token == os.environ["API_SECRET_KEY"]:
        return MIZU_ADMIN_USER

    user_id = get_user_id(pg_conn, token)
    if user_id is None or user_id == MIZU_ADMIN_USER:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="API key is invalid"
        )
    return user_id


def validate_worker(
    redis: Redis, pg_conn: connection, worker: str, job_type: JobType
) -> bool:
    rate_limit_key = rate_limit_field(job_type)
    fields = [BLOCKED_FIELD, rate_limit_key]
    day = epoch() // 86400
    if job_type == JobType.reward:
        fields.extend([mined_per_day_field(day - i) for i in range(0, 7)])
    values = redis.hmget(event_name(worker), fields)

    # check if worker is blocked
    if values[0] is not None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="worker is blocked",
        )

    # check if worker is rate limited
    config = get_cooldown_config(job_type)
    request_ts = (values[1] or "0").split(",")
    now = epoch()
    request_ts = [ts for ts in request_ts if int(ts) > now - config.interval]
    if len(request_ts) >= config.limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"please retry after {config.interval} seconds",
        )
    request_ts.append(str(now))
    redis.hset(event_name(worker), rate_limit_key, ",".join(request_ts))

    if job_type == JobType.reward:
        # check total unclaimed rewards
        count, last_assigned = get_reward_jobs_stats(pg_conn, worker)
        if count >= MAX_UNCLAIMED_REWARD:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="unclaimed reward limit reached",
            )

        # check last rewarded time
        if last_assigned and last_assigned + MIN_REWARD_GAP > epoch():
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"please retry after {MIN_REWARD_GAP} seconds",
            )

        # check if user is active in past 7 days
        enable_active_user_check = os.environ.get(
            "ENABLE_ACTIVE_USER_CHECK", "false"
        ).lower()
        if enable_active_user_check == "true" and all(
            [float(v or 0) < ACTIVE_USER_PAST_7D_THRESHOLD for v in values[2:]]
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="not active user",
            )


def get_lease_ttl(job_type: JobType) -> int:
    if job_type == JobType.reward:
        return REWARD_TTL
    elif job_type == JobType.batch_classify:
        return 3600
    else:
        return 600


def get_cooldown_config(job_type: JobType) -> CooldownConfig:
    if job_type == JobType.reward:
        return CooldownConfig(60, 1)
    return CooldownConfig(COOLDOWN_WORKER_EXPIRE_TTL_SECONDS, 10)


def get_allowed_origins() -> list[str]:
    return os.environ.get("ALLOWED_ORIGINS", "*").split(",")
