import os

from fastapi import HTTPException, status
from pymongo.database import Collection
from redis import Redis

from mizu_node.common import epoch
from mizu_node.constants import (
    ACTIVE_USER_PAST_7D_THRESHOLD,
    COOLDOWN_WORKER_EXPIRE_TTL_SECONDS,
    MAX_UNCLAIMED_REWARD,
    MIN_REWARD_GAP,
    MIZU_ADMIN_USER,
)
import jwt

from mizu_node.types.data_job import JobType, RewardJobRecord, RewardJobRecords

ALGORITHM = "EdDSA"
BLOCKED_FIELD = "blocked_worker"
REWARD_FIELD = "reward"
REWARD_TTL = 43200  # 12 hours


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


def event_name(worker: str):
    return f"event:{worker}"


def last_requested_field(job_type: JobType) -> str:
    return f"last_requested_at:{str(job_type)}"


def mined_per_hour_field(hour: int):
    return f"mined_per_hour:{hour}"


def mined_per_day_field(day: int):
    return f"mined_per_day:{day}"


def record_mined_points(rclient: Redis, worker: str, points: float):
    if points > 0.0:
        now = epoch()
        hour = now // 3600
        day = now // 86400
        pipeline = rclient.pipeline()
        pipeline.hincrbyfloat(event_name(worker), mined_per_hour_field(hour), points)
        pipeline.hincrbyfloat(event_name(worker), mined_per_day_field(day), points)
        pipeline.execute()


def total_mined_points_in_past_24h(rclient: Redis, worker: str) -> float:
    hour = epoch() // 3600
    fields = [mined_per_hour_field(hour - i) for i in range(0, 24)]
    values = rclient.hmget(event_name(worker), fields)
    return sum([float(v or 0) for v in values])


def get_valid_rewards(rclient: Redis, worker: str) -> RewardJobRecords:
    name = event_name(worker)
    value = rclient.hget(name, REWARD_FIELD)
    rewards = (
        RewardJobRecords.model_validate_json(value) if value else RewardJobRecords()
    )
    rewards.data = [r for r in rewards.data if r.issued_at + REWARD_TTL > epoch()]
    return rewards


def record_reward_event(rclient: Redis, worker: str, job_id: str):
    rewards = get_valid_rewards(rclient, worker)
    rewards.data.append(RewardJobRecord(job_id=job_id, issued_at=epoch()))
    rclient.hset(event_name(worker), REWARD_FIELD, rewards.model_dump_json())


def record_reward_claim(rclient: Redis, worker: str, job_id):
    rewards = get_valid_rewards(rclient, worker)
    rewards.data = [r for r in rewards.data if r.job_id != job_id]
    rclient.hset(event_name(worker), REWARD_FIELD, rewards.model_dump_json())


def validate_worker(r_client: Redis, worker: str, job_type: JobType) -> bool:
    last_requested_at_field = last_requested_field(job_type)
    fields = [BLOCKED_FIELD, last_requested_at_field]
    day = epoch() // 86400
    if job_type == JobType.reward:
        fields.append(REWARD_FIELD)
        fields.extend([mined_per_day_field(day - i) for i in range(0, 7)])
    values = r_client.hmget(event_name(worker), fields)

    # check if worker is blocked
    if values[0] is not None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="worker is blocked",
        )

    # check if worker is in cool down
    cooldown_ttl = get_cooldown_ttl(job_type)
    if int(values[1] or 0) + cooldown_ttl > epoch():
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"please retry after {cooldown_ttl} seconds",
        )
    r_client.hset(event_name(worker), last_requested_at_field, epoch())

    if job_type == JobType.reward:
        parsed_rewards = (
            RewardJobRecords.model_validate_json(values[2])
            if values[2]
            else RewardJobRecords()
        )
        valid_rewards = [
            r for r in parsed_rewards.data if r.issued_at + REWARD_TTL > epoch()
        ]
        if len(valid_rewards) != len(parsed_rewards.data):
            parsed_rewards.data = valid_rewards
            r_client.hset(
                event_name(worker), REWARD_FIELD, parsed_rewards.model_dump_json()
            )

        # check total unclaimed rewards
        if len(valid_rewards) >= MAX_UNCLAIMED_REWARD:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="unclaimed reward limit reached",
            )

        # check last rewarded time
        last_reward_ts = valid_rewards[-1].issued_at if valid_rewards else 0
        if last_reward_ts + MIN_REWARD_GAP > epoch():
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"please retry after {MIN_REWARD_GAP} seconds",
            )

        # check if user is active in past 7 days
        if all([float(v or 0) < ACTIVE_USER_PAST_7D_THRESHOLD for v in values[3:]]):
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


def get_cooldown_ttl(job_type: JobType) -> bool:
    if job_type == JobType.reward:
        return 60
    return COOLDOWN_WORKER_EXPIRE_TTL_SECONDS
