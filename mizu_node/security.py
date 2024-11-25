import os
import time

from fastapi import HTTPException, status
from pymongo.database import Collection
from redis import Redis

from mizu_node.constants import (
    ACTIVE_USER_PAST_7D_THRESHOLD,
    BLOCKED_WORKER_PREFIX,
    COOLDOWN_WORKER_EXPIRE_TTL_SECONDS,
    COOLDOWN_WORKER_PREFIX,
    MAX_UNCLAIMED_REWARD,
    MIN_REWARD_GAP,
    MIZU_ADMIN_USER,
)
import jwt

from mizu_node.types.data_job import JobType

ALGORITHM = "EdDSA"


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
    if token == os.environ["MIZU_ADMIN_USER_API_KEY"]:
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


def record_mined_points(rclient: Redis, worker: str, points: float):
    if points > 0.0:
        now = int(time.time())
        epoch = now // 3600
        day = now // 86400
        pipeline = rclient.pipeline()
        pipeline.incrbyfloat(mined_points_per_hour_key(worker, epoch), points)
        pipeline.incrbyfloat(mined_points_per_day_key(worker, day), points)
        pipeline.execute()


def total_mined_points_in_past_24h(rclient: Redis, worker: str) -> float:
    epoch = int(time.time()) // 3600
    keys = [mined_points_per_hour_key(worker, epoch - i) for i in range(0, 24)]
    values = rclient.mget(keys)
    return sum([float(v or 0) for v in values])


def last_rewarded_key(worker: str) -> str:
    return f"last_rewarded:{JobType.reward}:{worker}"


def unclaimed_reward_key(worker: str) -> str:
    return f"unclaimed_reward:{JobType.reward}:{worker}"


def record_reward_event(rclient: Redis, worker: str):
    pipeline = rclient.pipeline()
    pipeline.set(last_rewarded_key(worker), int(time.time()))
    pipeline.incrby(unclaimed_reward_key(worker), 1)
    pipeline.execute()


def record_reward_claim(rclient: Redis, worker: str):
    rclient.decrby(unclaimed_reward_key(worker), 1)


def validate_worker(r_client: Redis, worker: str, job_type: JobType) -> bool:
    cooldown_key = worker_cooldown_key(worker, job_type)
    keys = [
        blocked_worker_key(worker),
        cooldown_key,
    ]
    if job_type == JobType.reward:
        keys.extend([last_rewarded_key(worker), unclaimed_reward_key(worker)])
        day = int(time.time()) // 86400
        keys.extend([mined_points_per_day_key(worker, day - i) for i in range(0, 7)])

    values = r_client.mget(keys)

    # check if worker is blocked
    if values[0] is not None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="worker is blocked",
        )

    # check if worker is in cool down
    ttl = get_cooldown_ttl(job_type)
    if values[1] is not None:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"please retry after {ttl} seconds",
        )
    r_client.setex(cooldown_key, ttl, int(time.time()))

    if job_type == JobType.reward:
        # check last rewarded time
        if int(values[2] or 0) + MIN_REWARD_GAP > int(time.time()):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="please retry after {MIN_REWARD_GAP} seconds",
            )

        # check total unclaimed rewards
        if int(values[3] or 0) >= MAX_UNCLAIMED_REWARD:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="unclaimed reward limit reached",
            )

        # check if user is active in past 7 days
        if all([float(v or 0) < ACTIVE_USER_PAST_7D_THRESHOLD for v in values[4:]]):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="not active user",
            )
