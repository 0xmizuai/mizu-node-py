import os
import time

from fastapi import HTTPException, status
from pymongo.database import Collection
from redis import Redis

from mizu_node.constants import (
    ACTIVE_USER_PAST_24H_THRESHOLD,
    ACTIVE_USER_PAST_7D_THRESHOLD,
    BLOCKED_WORKER_PREFIX,
    COOLDOWN_WORKER_EXPIRE_TTL_SECONDS,
    COOLDOWN_WORKER_PREFIX,
    MIN_REWARD_GAP,
    MIZU_ADMIN_USER,
)
import jwt

from mizu_node.types.data_job import JobType

ALGORITHM = "EdDSA"
MIZU_ADMIN_USER_API_KEY = os.environ.get("MIZU_ADMIN_USER_API_KEY")

# ToDo: we should define a schema for the decoded payload


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
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token verification failed"
        )


def verify_api_key(mdb: Collection, token: str) -> str:
    if token == MIZU_ADMIN_USER_API_KEY:
        return MIZU_ADMIN_USER

    doc = mdb.find_one({"api_key": token})
    if doc is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="API key is invalid"
        )
    if doc["user"] == MIZU_ADMIN_USER:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="API key is invalid"
        )
    return doc["user"]


def blocked_worker_key(worker: str) -> str:
    return f"{BLOCKED_WORKER_PREFIX}:{worker}"


def worker_cooldown_key(worker: str, job_type: JobType) -> str:
    return f"{COOLDOWN_WORKER_PREFIX}:{job_type}:{worker}"


def get_cooldown_ttl(job_type: JobType) -> bool:
    if job_type == JobType.reward:
        return 60
    return COOLDOWN_WORKER_EXPIRE_TTL_SECONDS


def mined_points_per_hour_key(worker: str, epoch: int):
    return f"points:{worker}:hour:{epoch}"


def mined_points_per_day_key(worker: str, epoch: int):
    return f"points:{worker}:day:{epoch}"


def record_mined_points(rclient: Redis, worker: str, points: int):
    now = int(time.time())
    epoch = now // 3600
    day = now // 86400
    rclient.pipeline().incrby(mined_points_per_hour_key(worker, epoch), points).incrby(
        mined_points_per_day_key(worker, day), points
    ).execute()


def is_active_in_past_24h(rclient: Redis, worker: str) -> int:
    epoch = int(time.time()) // 3600
    keys = [mined_points_per_hour_key(worker, epoch - i) for i in range(0, 24)]
    values = rclient.mget(keys)
    return sum([int(v or 0) for v in values]) > ACTIVE_USER_PAST_24H_THRESHOLD


def is_active_in_past_7days(rclient: Redis, worker: str) -> int:
    day = int(time.time()) // 86400
    # exclude today because:
    #  1. it could be the start of the day where no one has mined any points
    #  2. it's already validated by past 24h check
    keys = [mined_points_per_day_key(worker, day - i) for i in range(1, 8)]
    values = rclient.mget(keys)
    return all([int(v or 0) > ACTIVE_USER_PAST_7D_THRESHOLD for v in values])


def last_rewarded_key(worker: str) -> str:
    return f"{JobType.reward}:last_rewarded:{worker}"


def record_reward_event(rclient: Redis, worker: str):
    rclient.set(last_rewarded_key(worker), int(time.time()))


def validate_reward_job_request(rclient: Redis, worker: str) -> bool:
    last_rewarded = rclient.get(last_rewarded_key(worker))
    if int(last_rewarded or 0) + MIN_REWARD_GAP > int(time.time()):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="please retry after {MIN_REWARD_GAP} seconds",
        )

    if not is_active_in_past_24h(rclient, worker) or not is_active_in_past_7days(
        rclient, worker
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="not active user",
        )


def validate_worker(r_client: Redis, worker: str, job_type: JobType) -> bool:
    # check if worker is blocked
    if r_client.exists(blocked_worker_key(worker)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="worker is blocked",
        )

    # check if worker is in cool down
    ttl = get_cooldown_ttl(job_type)
    cooldown_key = worker_cooldown_key(worker, job_type)
    if r_client.exists(cooldown_key):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="please retry after ${ttl} seconds",
        )
    r_client.setex(cooldown_key, ttl, int(time.time()))

    if job_type == JobType.reward:
        validate_reward_job_request(r_client, worker)
