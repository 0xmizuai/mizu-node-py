import argparse
import logging
import os
from datetime import datetime
from typing import Callable, Dict
import zlib
import aioboto3
import asyncio
from pydantic import BaseModel, Field
import pymongo
import redis

from mizu_node.common import epoch
from mizu_node.stats import event_name, mined_per_day_field, mined_per_hour_field

logging.basicConfig(level=logging.INFO)


R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
R2_BACKCUP_BUCKET_NAME = "mongo-backup"

REDIS_URL = os.environ["REDIS_URL"]
logging.info(f"Connecting to redis at {REDIS_URL}")
rclient = redis.Redis.from_url(REDIS_URL, decode_responses=True)

USER_MONGO_URL = os.environ["USER_MONGO_URL"]
USER_DB_NAME = "user"
USER_COLLECTION_NAME = "user_reward_points"
logging.info(f"Connecting to mongourl at {USER_MONGO_URL}")
mclient = pymongo.MongoClient(USER_MONGO_URL)
mdb = mclient.get_database(USER_DB_NAME)
user_coll = mdb.get_collection(USER_COLLECTION_NAME)


class UserRecord(BaseModel):
    timestamp: int = Field(default=epoch())
    user_id: str
    username: str
    claimed_point: float
    mined_points_past_24h: Dict[int, float] = Field(default={})  # past 24h
    mined_points_past_7d: Dict[int, float] = Field(default={})  # past 7days


def inspect_by_points_users(env: str, point: float):
    filter: Callable[[UserRecord], bool] = lambda user: user.claimed_point > point
    load_backups(filter, env=env, verbose=False)


def inspect_by_user(env: str, u: str):
    filter: Callable[[UserRecord], bool] = lambda user: user.username == u
    load_backups(filter, env=env, verbose=False)


# Replace synchronous boto3 initialization with async session
session = aioboto3.Session()


async def load_backups(filter: Callable, env: str, verbose: bool = False):
    async with session.resource(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
    ) as r2:
        keys = await r2.meta.client.list_objects_v2(
            Bucket=R2_BACKCUP_BUCKET_NAME, Prefix=f"{env}/mizu_users/"
        )
        for obj in keys.get("Contents", []):
            epoch = int(obj["Key"].split("/")[-1].split(".")[0])
            logging.info(
                f"Loading backup file {epoch}.zz: {datetime.fromtimestamp(epoch)}"
            )
            file = f"{env}/mizu_users/{epoch}.zz"
            await load_backup(r2, file, filter, verbose)


async def load_backup(r2, file: str, filter: Callable, verbose: bool = False):
    obj = await r2.meta.client.get_object(
        Bucket=R2_BACKCUP_BUCKET_NAME,
        Key=file,
    )
    compressed = await obj["Body"].read()
    json_str = zlib.decompress(compressed).decode("utf-8")
    users = [UserRecord.model_validate_json(line) for line in json_str.split("\n")]
    for user in users:
        if not filter(user):
            continue

        logging.info(
            f"User {user.user_id}: username={user.username}, points = {user.claimed_point}"
        )
        if verbose:
            logging.info(f"mined_points_past_24h: {user.mined_points_past_24h}")
            logging.info(f"mined_points_past_7d: {user.mined_points_past_7d}")
    return users


def restore(epoch: int):
    users = load_backup(epoch)
    logging.info(f"loaded {len(users)} users from backup")
    for user in users:
        name = event_name(user.username)
        hourly_field = [
            mined_per_hour_field(hour) for hour in user.mined_points_past_24h
        ]
        daily_field = [mined_per_day_field(day) for day in user.mined_points_past_7d]
        values = [
            str(user.mined_points_past_24h.get(hour, 0)) for hour in hourly_field
        ] + [str(user.mined_points_past_7d.get(day, 0)) for day in daily_field]
        rclient.hmset(name, dict(zip(hourly_field + daily_field, values)))
    user_coll.bulk_write(
        [
            pymongo.UpdateOne(
                {"_id": user.user_id},
                {"$set": {"claimed_point": user.claimed_point}},
                upsert=True,
            )
            for user in users
        ]
    )
    logging.info("Backup file restored")


async def backup(env: str):
    now = int(epoch())
    logging.info("Backing up user data to R2")
    hour = now // 3600
    day = now // 86400
    logging.info("Loading user total points snapshot")
    docs = list(user_coll.find({}, {"_id": 1, "user_key": 1, "claimed_point": 1}))
    users = [
        UserRecord(
            user_id=str(d["_id"]),
            username=d["user_key"],
            claimed_point=d.get("claimed_point", 0),
        )
        for d in docs
    ]
    if not users:
        logging.info("No user data found")
        return

    logging.info(f"{len(users)} users loaded")
    logging.info("Loading user mined points stats")
    for user in users:
        name = event_name(user.username)
        hourly_field = [mined_per_hour_field(hour - i) for i in range(0, 24)]
        daily_field = [mined_per_day_field(day - i) for i in range(0, 7)]
        values = rclient.hmget(name, hourly_field + daily_field)
        user.mined_points_past_24h = {
            hour - i: float(v or 0) for i, v in enumerate(values[:24])
        }
        user.mined_points_past_7d = {
            day - i: float(v or 0) for i, v in enumerate(values[24:])
        }
    logging.info(f"mined points stats users loaded")
    logging.info("Compressing user data")
    json_str = "\n".join([user.model_dump_json() for user in users])
    compressed = zlib.compress(json_str.encode("utf-8"))
    logging.info("Uploading user data")
    async with session.resource(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
    ) as r2:
        await r2.meta.client.put_object(
            Bucket=R2_BACKCUP_BUCKET_NAME,
            Key=f"{env}/mizu_users/{epoch()}.zz",
            Body=compressed,
            ContentLength=len(compressed),
        )
    logging.info("Backup done")


parser = argparse.ArgumentParser()
parser.add_argument("--env", type=str, action="store", default="local")
args = parser.parse_args()


def main():
    env = os.environ.get("RAILWAY_ENVIRONMENT_NAME", "local")
    asyncio.run(backup(args.env or env))
