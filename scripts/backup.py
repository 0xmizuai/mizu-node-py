import logging
import os
from typing import Dict
import zlib
import boto3
from mongomock import MongoClient
from pydantic import BaseModel, Field
import redis

from mizu_node.common import epoch
from mizu_node.security import mined_per_day_field, mined_per_hour_field

logging.basicConfig(level=logging.INFO)  # Set the desired logging level


R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
R2_BACKCUP_BUCKET_NAME = "mongo-backup"
r2 = boto3.resource(
    "s3",
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
)

REDIS_URL = os.environ["REDIS_URL"]
logging.info(f"Connecting to redis at {REDIS_URL}")
rclient = redis.Redis.from_url(REDIS_URL, decode_responses=True)

USER_MONGO_URL = os.environ["USER_MONGO_URL"]
USER_DB_NAME = "user"
USER_COLLECTION_NAME = "user_reward_points"
logging.info(f"Connecting to mongourl at {USER_MONGO_URL}")
mclient = MongoClient(USER_MONGO_URL)
mdb = mclient.get_database(USER_DB_NAME)
user_coll = mdb.get_collection(USER_COLLECTION_NAME)


class UserRecord(BaseModel):
    timestamp: int = Field(default=epoch())
    user_id: str
    claimed_point: float
    mined_points_past_24h: Dict[int, float] = Field(default={})  # past 24h
    mined_points_past_7d: Dict[int, float] = Field(default={})  # past 7days


def main():
    now = int(epoch())
    logging.info("Backing up user data to R2")
    hour = now // 3600
    day = now // 86400
    logging.info("Loading user total points snapshot")
    docs = list(user_coll.find({}, {"_id": 1, "claimed_point": 1}))
    users = [
        UserRecord(user_id=str(d["_id"]), claimed_point=d["claimed_point"])
        for d in docs
    ]
    if not users:
        logging.info("No user data found")
        return

    logging.info(f"{len(users)} users loaded")
    logging.info("Loading user mined points stats")
    for user in users:
        name = f"event:{user.user_id}"
        hourly_field = [mined_per_hour_field(hour - i) for i in range(0, 24)]
        daily_field = [mined_per_day_field(day - i) for i in range(0, 7)]
        values = rclient.hmget(name, hourly_field + daily_field)
        user.mined_points_past_24h = {
            hour - i: float(v) for i, v in enumerate(values[:24])
        }
        user.mined_points_past_7d = {
            day - i: float(v) for i, v in enumerate(values[24:])
        }
    logging.info(f"mined points stats users loaded")
    logging.info("Compressing user data")
    json_str = "\n".join([user.model_dump_json() for user in users])
    compressed = zlib.compress(json_str.encode("utf-8"))
    logging.info("Uploading user data")
    r2.meta.client.put_object(
        Bucket=R2_BACKCUP_BUCKET_NAME,
        Key=f"staging/mizu_users/{epoch()}.zz",
        Body=compressed,
        ContentLength=len(compressed),
    )
    logging.info("Backup done")
